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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end exercise of consumer {@code subscribe()} mode, which drives the full JoinGroup /
 * SyncGroup / Heartbeat path. Phase 2D supports single-consumer groups — that is what's tested.
 */
class KafkaSubscribeModeITCase {

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
    void singleConsumerSubscribeConsumesAllProducedRecords() throws Exception {
        String topic = "sub_" + System.nanoTime();
        String group = "g_" + System.nanoTime();
        int partitions = 3;
        int recordsPerPartition = 4;
        int expected = partitions * recordsPerPartition;

        admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                .all()
                .get();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int p = 0; p < partitions; p++) {
                for (int i = 0; i < recordsPerPartition; i++) {
                    producer.send(
                                    new ProducerRecord<>(
                                            topic,
                                            p,
                                            null,
                                            ("p" + p + "-" + i).getBytes(StandardCharsets.UTF_8)))
                            .get();
                }
            }
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(group))) {
            consumer.subscribe(Collections.singletonList(topic));

            List<String> received = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (received.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    received.add(new String(r.value(), StandardCharsets.UTF_8));
                }
            }
            assertThat(received).hasSize(expected);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void subscribeAndCommitAutoCommitDisabled() throws Exception {
        String topic = "subc_" + System.nanoTime();
        String group = "g_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < 5; i++) {
                producer.send(
                                new ProducerRecord<>(
                                        topic, 0, null, ("v" + i).getBytes(StandardCharsets.UTF_8)))
                        .get();
            }
        }

        try (KafkaConsumer<byte[], byte[]> first = new KafkaConsumer<>(consumerProps(group))) {
            first.subscribe(Collections.singletonList(topic));
            int seen = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (seen < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = first.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    seen++;
                    if (seen >= 3) {
                        break;
                    }
                }
            }
            assertThat(seen).isEqualTo(3);
            // Commit exactly offset=3 so the second consumer picks up records 3 and 4. Using a
            // bare commitSync() would commit the full in-memory position, which may have advanced
            // past record 3 if poll() returned the whole batch in one call.
            org.apache.kafka.common.TopicPartition tp =
                    new org.apache.kafka.common.TopicPartition(topic, 0);
            first.commitSync(
                    Collections.singletonMap(
                            tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(3L)));
        }

        // Second subscriber with same group should resume after the commit point.
        try (KafkaConsumer<byte[], byte[]> second = new KafkaConsumer<>(consumerProps(group))) {
            second.subscribe(Collections.singletonList(topic));
            int seen = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (seen < 2 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = second.poll(Duration.ofSeconds(2));
                seen += batch.count();
            }
            assertThat(seen).isEqualTo(2);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
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
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 40_000);
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
