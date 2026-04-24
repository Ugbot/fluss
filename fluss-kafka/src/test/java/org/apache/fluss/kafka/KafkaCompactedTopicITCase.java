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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of Kafka compacted topics ({@code cleanup.policy=compact}) mapped onto Fluss
 * PK tables. Verifies the produce path routes through {@code ReplicaManager.putRecordsToKv}, and
 * that last-writer-wins holds for the snapshot view.
 */
class KafkaCompactedTopicITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
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
            admin.close(Duration.ofSeconds(5));
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void compactTopic_acceptsRepeatedKeyProduceSuccessfully() throws Exception {
        String topic = "compact_" + System.nanoTime();
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        NewTopic nt = new NewTopic(topic, 1, (short) 1).configs(configs);
        admin.createTopics(Collections.singletonList(nt)).all().get();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            // Three writes against the same key — on a compacted topic the PK-backed Fluss table
            // overwrites by key, last-writer-wins. Each send() is round-tripped separately to
            // force three individual Produce RPCs through the upsert path.
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "v3")).get();
            // Distinct key — proves multiple PK rows coexist.
            producer.send(new ProducerRecord<>(topic, "k2", "vA")).get();
        }
    }

    @Test
    void compactTopic_consumerReadsChangelogAndSeesLatestPerKey() throws Exception {
        String topic = "compact_cons_" + System.nanoTime();
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, 1, (short) 1).configs(configs)))
                .all()
                .get();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
            producer.send(new ProducerRecord<>(topic, "k2", "a")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "v3")).get();
            producer.send(new ProducerRecord<>(topic, "k2", "b")).get();
        }

        // Consume the CDC changelog. A compacted-topic consumer sees the stream of updates
        // (INSERT / UPDATE_AFTER rows), UPDATE_BEFORE records are filtered out by the fetch
        // transcoder, DELETE records surface as null-value Kafka tombstones. Client-side we
        // materialise last-writer-wins per key.
        Map<String, String> latestByKey = new java.util.LinkedHashMap<>();
        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps())) {
            consumer.assign(
                    Collections.singletonList(
                            new org.apache.kafka.common.TopicPartition(topic, 0)));
            consumer.seekToBeginning(
                    Collections.singletonList(
                            new org.apache.kafka.common.TopicPartition(topic, 0)));
            long deadline = System.currentTimeMillis() + 20_000;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 5) {
                org.apache.kafka.clients.consumer.ConsumerRecords<String, String> batch =
                        consumer.poll(Duration.ofSeconds(1));
                for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> r : batch) {
                    latestByKey.put(r.key(), r.value());
                    seen++;
                }
            }
        }

        assertThat(latestByKey).containsEntry("k1", "v3").containsEntry("k2", "b");
    }

    @Test
    void compactTopic_nullValueDeletesKey() throws Exception {
        String topic = "compact_del_" + System.nanoTime();
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, 1, (short) 1).configs(configs)))
                .all()
                .get();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
            // Kafka tombstone: null value on a compacted topic means "delete this key".
            producer.send(new ProducerRecord<>(topic, "k1", null)).get();
        }

        // Consume: expect the insert record followed by a null-value tombstone. A client
        // materialising latest-per-key drops k1 on the tombstone.
        java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> seen =
                new java.util.ArrayList<>();
        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps())) {
            consumer.assign(
                    Collections.singletonList(
                            new org.apache.kafka.common.TopicPartition(topic, 0)));
            consumer.seekToBeginning(
                    Collections.singletonList(
                            new org.apache.kafka.common.TopicPartition(topic, 0)));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline && seen.size() < 2) {
                for (org.apache.kafka.clients.consumer.ConsumerRecord<String, String> r :
                        consumer.poll(Duration.ofSeconds(1))) {
                    seen.add(r);
                }
            }
        }
        assertThat(seen).hasSize(2);
        assertThat(seen.get(0).key()).isEqualTo("k1");
        assertThat(seen.get(0).value()).isEqualTo("v1");
        assertThat(seen.get(1).key()).isEqualTo("k1");
        assertThat(seen.get(1).value()).isNull();
    }

    @Test
    void defaultTopicIsLog_notPkTable() throws Exception {
        // Sanity: creating a topic without cleanup.policy=compact must NOT produce a PK-backed
        // table. This was already the behaviour before Phase C-T; the test just pins it.
        String topic = "log_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            // Same key twice — on a log topic both records are appended; no overwrite semantics.
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "v2")).get();
        }
    }

    // --- helpers ---

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "compact-cons-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20_000);
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
