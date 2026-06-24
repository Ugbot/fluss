/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for OffsetCommit / OffsetFetch with FindCoordinator routing (Phase 2D). */
class KafkaConsumerOffsetsITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

    private static Admin admin;

    @BeforeAll
    static void createAdmin() {
        admin = KafkaAdminClient.create(adminProps());
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close();
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void commitThenFetchRoundTrips() throws Exception {
        String topic = "off_" + System.nanoTime();
        String group = "g_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, (short) 1)))
                .all()
                .get();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp0 = new TopicPartition(topic, 0);
            TopicPartition tp1 = new TopicPartition(topic, 1);
            consumer.assign(java.util.Arrays.asList(tp0, tp1));

            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            commits.put(tp0, new OffsetAndMetadata(42L, "partition-zero"));
            commits.put(tp1, new OffsetAndMetadata(123L));
            consumer.commitSync(commits);

            Map<TopicPartition, OffsetAndMetadata> fetched =
                    consumer.committed(new java.util.HashSet<>(java.util.Arrays.asList(tp0, tp1)));

            assertThat(fetched).hasSize(2);
            assertThat(fetched.get(tp0).offset()).isEqualTo(42L);
            assertThat(fetched.get(tp0).metadata()).isEqualTo("partition-zero");
            assertThat(fetched.get(tp1).offset()).isEqualTo(123L);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void resumeFromCommittedOffset() throws Exception {
        String topic = "resume_" + System.nanoTime();
        String group = "g_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int totalRecords = 8;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < totalRecords; i++) {
                byte[] v = ("msg-" + i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(topic, 0, null, v)).get();
            }
        }

        long commitPoint;
        try (KafkaConsumer<byte[], byte[]> first = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            first.assign(Collections.singletonList(tp));
            first.seekToBeginning(Collections.singletonList(tp));

            int readHalf = totalRecords / 2;
            int received = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (received < readHalf && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = first.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    received++;
                    if (received >= readHalf) {
                        commitPoint = r.offset() + 1;
                        first.commitSync(
                                Collections.singletonMap(tp, new OffsetAndMetadata(commitPoint)));
                        break;
                    }
                }
            }
            assertThat(received).isEqualTo(readHalf);
        }

        try (KafkaConsumer<byte[], byte[]> second = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            second.assign(Collections.singletonList(tp));
            Map<TopicPartition, OffsetAndMetadata> committed =
                    second.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            long resumeFrom = committed.get(tp).offset();
            second.seek(tp, resumeFrom);

            int remaining = totalRecords - (int) resumeFrom;
            int got = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (got < remaining && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = second.poll(Duration.ofSeconds(2));
                got += batch.count();
            }
            assertThat(got).isEqualTo(remaining);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void listAndDescribeGroupsReflectCommits() throws Exception {
        String topic = "lg_" + System.nanoTime();
        String group = "grp_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.commitSync(
                    Collections.singletonMap(tp, new OffsetAndMetadata(7L, "seen-seven")));

            // ListGroups should surface the committed group.
            java.util.Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> listed =
                    admin.listConsumerGroups().all().get();
            assertThat(listed)
                    .extracting(org.apache.kafka.clients.admin.ConsumerGroupListing::groupId)
                    .contains(group);

            // DescribeGroups returns Empty state + consumer protocol for a committed-only group.
            Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> described =
                    admin.describeConsumerGroups(Collections.singletonList(group)).all().get();
            assertThat(described).containsKey(group);
            assertThat(described.get(group).groupId()).isEqualTo(group);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void committedOffsetsSurviveTabletServerRestart() throws Exception {
        String topic = "durable_" + System.nanoTime();
        String group = "g_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Commit from a consumer, then close and rebuild the AdminClient to guarantee a fresh
        // metadata/coordinator lookup after restart.
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.commitSync(
                    Collections.singletonMap(tp, new OffsetAndMetadata(99L, "pre-restart")));
        }

        // Bounce one tablet server. Offsets were committed against the "lowest-id" coordinator;
        // we don't bother restarting every server because the ZK-backed store is cluster-wide.
        int victimId = CLUSTER.getTabletServerInfos().get(0).id();
        CLUSTER.restartTabletServer(victimId, new Configuration());

        // Rebuild the admin client against the fresh bootstrap (old sockets may be closed).
        admin.close();
        admin = KafkaAdminClient.create(adminProps());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer.committed(Collections.singleton(tp));
            assertThat(committed.get(tp)).isNotNull();
            assertThat(committed.get(tp).offset()).isEqualTo(99L);
            assertThat(committed.get(tp).metadata()).isEqualTo("pre-restart");
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void unknownGroupReturnsNoOffsets() {
        String group = "ghost_" + System.nanoTime();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition("nonexistent-topic", 0);
            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer.committed(Collections.singleton(tp));
            // Kafka protocol: absent offsets are represented by null in the returned map.
            assertThat(committed.get(tp)).isNull();
        }
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties consumerProps(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static String bootstrap() {
        StringBuilder b = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }
        return b.toString();
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
