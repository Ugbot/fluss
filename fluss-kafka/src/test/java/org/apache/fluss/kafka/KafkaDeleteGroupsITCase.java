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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for Kafka {@code DELETE_GROUPS} and {@code OFFSET_DELETE} driven through
 * kafka-clients' {@link Admin}. Covers per-partition offset deletion, empty-group deletion, and the
 * refusal path for a group that still has live members.
 */
class KafkaDeleteGroupsITCase {

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
    void offsetDeletePerPartitionLeavesSiblingIntact() throws Exception {
        String topic = "offdel_" + uniqueSuffix();
        String group = "g_" + uniqueSuffix();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 3, (short) 1)))
                .all()
                .get();

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);
        long off0 = randomOffset();
        long off1 = randomOffset();
        long off2 = randomOffset();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            consumer.assign(Arrays.asList(tp0, tp1, tp2));
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            commits.put(tp0, new OffsetAndMetadata(off0));
            commits.put(tp1, new OffsetAndMetadata(off1));
            commits.put(tp2, new OffsetAndMetadata(off2));
            consumer.commitSync(commits);
        }

        // Delete the offset only for partition 1.
        admin.deleteConsumerGroupOffsets(group, Collections.singleton(tp1))
                .partitionResult(tp1)
                .get();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            consumer.assign(Arrays.asList(tp0, tp1, tp2));
            Map<TopicPartition, OffsetAndMetadata> fetched =
                    consumer.committed(new HashSet<>(Arrays.asList(tp0, tp1, tp2)));

            assertThat(fetched.get(tp0)).isNotNull();
            assertThat(fetched.get(tp0).offset()).isEqualTo(off0);
            assertThat(fetched.get(tp1)).isNull();
            assertThat(fetched.get(tp2)).isNotNull();
            assertThat(fetched.get(tp2).offset()).isEqualTo(off2);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void deleteConsumerGroupOfEmptyGroupSucceedsAndClearsOffsets() throws Exception {
        String topic = "dg_" + uniqueSuffix();
        String group = "g_" + uniqueSuffix();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, (short) 1)))
                .all()
                .get();

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        long off0 = randomOffset();
        long off1 = randomOffset();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            consumer.assign(Arrays.asList(tp0, tp1));
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            commits.put(tp0, new OffsetAndMetadata(off0));
            commits.put(tp1, new OffsetAndMetadata(off1));
            consumer.commitSync(commits);
        }

        // Sanity: ListGroups sees the group before deletion.
        Collection<String> listedBefore = extractGroupIds(admin.listConsumerGroups().all().get());
        assertThat(listedBefore).contains(group);

        admin.deleteConsumerGroups(Collections.singleton(group)).all().get();

        // After deletion, OffsetFetch via a fresh consumer must yield no committed offsets.
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            consumer.assign(Arrays.asList(tp0, tp1));
            Map<TopicPartition, OffsetAndMetadata> fetched =
                    consumer.committed(new HashSet<>(Arrays.asList(tp0, tp1)));
            assertThat(fetched.get(tp0)).isNull();
            assertThat(fetched.get(tp1)).isNull();
        }

        // ListGroups must no longer surface the group.
        Collection<String> listedAfter = extractGroupIds(admin.listConsumerGroups().all().get());
        assertThat(listedAfter).doesNotContain(group);

        admin.deleteTopics(Collections.singletonList(topic)).all().get();
    }

    @Test
    void deleteUnknownGroupFails() {
        String ghost = "ghost_" + uniqueSuffix();
        assertThatThrownBy(
                        () -> admin.deleteConsumerGroups(Collections.singleton(ghost)).all().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(GroupIdNotFoundException.class);
    }

    @Test
    void deleteNonEmptyGroupIsRefused() throws Exception {
        String topic = "ne_" + uniqueSuffix();
        String group = "g_" + uniqueSuffix();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Subscribe-mode consumer: joining the group installs a live member in the registry.
        Properties props = consumerProps(group);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            // Drive the coordinator handshake until the member owns the partition.
            long deadline = System.currentTimeMillis() + 30_000;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofSeconds(1));
                // No records expected on an empty topic; the poll drives rebalance.
                assertThat(polled.count()).isZero();
            }
            assertThat(consumer.assignment())
                    .as("consumer should have joined the group and been assigned the partition")
                    .isNotEmpty();

            // With a live member, DELETE_GROUPS must refuse.
            assertThatThrownBy(
                            () ->
                                    admin.deleteConsumerGroups(Collections.singleton(group))
                                            .all()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(GroupNotEmptyException.class);

            // OFFSET_DELETE must likewise refuse for a non-empty group.
            TopicPartition tp = new TopicPartition(topic, 0);
            assertThatThrownBy(
                            () ->
                                    admin.deleteConsumerGroupOffsets(
                                                    group, Collections.singleton(tp))
                                            .all()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(GroupNotEmptyException.class);
        }

        admin.deleteTopics(Collections.singletonList(topic)).all().get();
    }

    private static Collection<String> extractGroupIds(
            Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> listings) {
        java.util.List<String> ids = new java.util.ArrayList<>(listings.size());
        for (org.apache.kafka.clients.admin.ConsumerGroupListing l : listings) {
            ids.add(l.groupId());
        }
        return ids;
    }

    private static long randomOffset() {
        return 1L + (ThreadLocalRandom.current().nextLong() & 0x7fffL);
    }

    private static String uniqueSuffix() {
        return System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
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
