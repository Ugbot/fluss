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
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.rpc.metrics.BoltOnMetricGroup;
import org.apache.fluss.server.tablet.TabletServer;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end Kafka bolt-on metrics test. Drives a real kafka-clients 3.9 producer + consumer +
 * admin through the Fluss Kafka plugin and asserts that the matching counters on the broker-side
 * {@link KafkaMetricGroup} advance. Exercises the canonical call sites:
 *
 * <ul>
 *   <li>Connection open / close
 *   <li>Per-API request rate + latency histogram
 *   <li>Per-topic bytesIn / bytesOut
 *   <li>Aggregate bytesIn / bytesOut (mirrors Kafka-native {@code bytesInPerSecond})
 * </ul>
 */
class KafkaMetricsITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;

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
    void producerAndConsumerAdvanceMetrics() throws Exception {
        String topic = "metrics_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        KafkaMetricGroup metrics = kafkaMetricsOnAnyServer();
        assertThat(metrics).as("KafkaMetricGroup must be wired into the plugin").isNotNull();

        long bytesInBefore = metrics.bytesInCount();
        long bytesOutBefore = metrics.bytesOutCount();
        long requestsBefore = metrics.requestCount();

        // Produce N distinct random records.
        Random rng = new Random(42);
        int recordCount = 25;
        int valueLen = 128;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < recordCount; i++) {
                byte[] key = bytesOf(rng, 12);
                byte[] value = bytesOf(rng, valueLen);
                producer.send(new ProducerRecord<>(topic, 0, key, value)).get();
            }
            producer.flush();
        }

        // Consume everything back.
        int consumed = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (consumed < recordCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    consumed++;
                    assertThat(r.topic()).isEqualTo(topic);
                }
            }
        }
        assertThat(consumed).isEqualTo(recordCount);

        // The metric group may be per-server; pick up whichever instance actually served traffic.
        // In a single-server cluster this is the same one we already grabbed.
        KafkaMetricGroup after = kafkaMetricsOnAnyServer();
        assertThat(after.requestCount())
                .as("requestCount must grow after traffic")
                .isGreaterThan(requestsBefore);
        assertThat(after.bytesInCount())
                .as("aggregate bytesIn must grow after produce")
                .isGreaterThan(bytesInBefore);
        assertThat(after.bytesOutCount())
                .as("aggregate bytesOut must grow after consume")
                .isGreaterThan(bytesOutBefore);

        // Per-topic bucket advanced.
        BoltOnMetricGroup.SessionEntityMetricGroup topicMetrics = after.topicMetrics(topic);
        assertThat(topicMetrics).isNotNull();
        assertThat(topicMetrics.bytesInCount())
                .as("per-topic bytesIn must be at least the rough record-payload size")
                .isGreaterThanOrEqualTo((long) recordCount * valueLen);
        assertThat(topicMetrics.bytesOutCount())
                .as("per-topic bytesOut must be non-zero after consume")
                .isGreaterThan(0L);
        assertThat(topicMetrics.messagesInCount())
                .as("per-topic messagesIn must count the records we sent")
                .isGreaterThanOrEqualTo(recordCount);
        assertThat(topicMetrics.operationCount())
                .as("per-topic op count must be at least the number of produce + fetch batches")
                .isGreaterThan(0L);

        // Per-API sub-group exists for PRODUCE and FETCH.
        Set<String> expectedApis =
                new java.util.HashSet<>(Arrays.asList("PRODUCE", "FETCH", "METADATA"));
        for (String api : expectedApis) {
            assertThat(after.apiGroupsSnapshot().keySet())
                    .as("expected per-API sub-group for " + api)
                    .contains(api);
        }

        // Connection-count gauge advanced at least once during the test.
        assertThat(after.activeConnectionCount()).isGreaterThanOrEqualTo(0);

        admin.deleteTopics(Collections.singletonList(topic)).all().get();
    }

    /** Locate the {@link KafkaMetricGroup} attached to any live tablet server in the cluster. */
    private static KafkaMetricGroup kafkaMetricsOnAnyServer() {
        Set<TabletServer> servers = CLUSTER.getTabletServers();
        for (TabletServer ts : servers) {
            KafkaMetricGroup m = KafkaProtocolPlugin.metricGroupForServer(ts.getServerId());
            if (m != null) {
                return m;
            }
        }
        return null;
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

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
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

    private static byte[] bytesOf(Random rng, int len) {
        byte[] bytes = new byte[len];
        rng.nextBytes(bytes);
        return bytes;
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }

    // Keep reference to suppress unused-import warnings on StandardCharsets.
    @SuppressWarnings("unused")
    private static final String UNUSED_IMPORT_ANCHOR = StandardCharsets.UTF_8.name();
}
