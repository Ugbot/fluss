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
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end DeleteRecords test: real kafka-clients {@link Admin#deleteRecords} against the Fluss
 * Kafka bolt-on. Produces 10 records into a single-partition topic, trims to offset 5, then
 * verifies the Kafka {@code LIST_OFFSETS} EARLIEST response reports the new low-watermark and a
 * subsequent {@code deleteRecords} with {@link RecordsToDelete#beforeOffset(long) beforeOffset(0)}
 * is a no-op.
 */
class KafkaDeleteRecordsITCase {

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
    void deleteRecordsAdvancesLogStartOffset() throws Exception {
        String topic = "deleterecords_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try {
            TopicPartition tp = new TopicPartition(topic, 0);
            int numRecords = 10;
            long lastOffset;
            Random rng = new Random();
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
                RecordMetadata tail = null;
                for (int i = 0; i < numRecords; i++) {
                    byte[] key = bytesOf(rng, 8);
                    byte[] value = bytesOf(rng, 64);
                    tail = producer.send(new ProducerRecord<>(topic, 0, key, value)).get();
                }
                producer.flush();
                assertThat(tail).isNotNull();
                lastOffset = tail.offset();
            }
            // Sanity: 10 records landed contiguously starting at offset 0.
            assertThat(lastOffset).isEqualTo(numRecords - 1);
            assertThat(earliestOffset(tp)).isZero();

            long trimTo = 5L;
            Map<TopicPartition, RecordsToDelete> req = new HashMap<>();
            req.put(tp, RecordsToDelete.beforeOffset(trimTo));
            Map<TopicPartition, DeletedRecords> lowWatermarks =
                    admin.deleteRecords(req).lowWatermarks().entrySet().stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> {
                                                try {
                                                    return e.getValue().get();
                                                } catch (Exception ex) {
                                                    throw new RuntimeException(ex);
                                                }
                                            }));
            assertThat(lowWatermarks).containsKey(tp);
            assertThat(lowWatermarks.get(tp).lowWatermark()).isEqualTo(trimTo);

            // The broker-reported EARLIEST offset must now match the new low-watermark.
            assertThat(earliestOffset(tp)).isEqualTo(trimTo);

            // Idempotency: trimming to an offset <= current log-start is a no-op.
            Map<TopicPartition, RecordsToDelete> noop = new HashMap<>();
            noop.put(tp, RecordsToDelete.beforeOffset(0L));
            Map<TopicPartition, DeletedRecords> noopResult =
                    admin.deleteRecords(noop).lowWatermarks().entrySet().stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> {
                                                try {
                                                    return e.getValue().get();
                                                } catch (Exception ex) {
                                                    throw new RuntimeException(ex);
                                                }
                                            }));
            assertThat(noopResult.get(tp).lowWatermark()).isEqualTo(trimTo);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void deleteRecordsBeyondHighWatermarkReturnsOffsetOutOfRange() throws Exception {
        String topic = "deleterecords_oor_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try {
            TopicPartition tp = new TopicPartition(topic, 0);
            Random rng = new Random();
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
                producer.send(new ProducerRecord<>(topic, 0, bytesOf(rng, 8), bytesOf(rng, 32)))
                        .get();
                producer.flush();
            }

            Map<TopicPartition, RecordsToDelete> req = new HashMap<>();
            // Offset 100 is far beyond the 1-record high-watermark; Kafka contract:
            // OFFSET_OUT_OF_RANGE.
            req.put(tp, RecordsToDelete.beforeOffset(100L));
            assertThatThrownBy(() -> admin.deleteRecords(req).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(
                            org.apache.kafka.common.errors.OffsetOutOfRangeException.class);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void deleteRecordsOnUnknownTopicFailsFast() {
        // Kafka AdminClient first issues a METADATA request to resolve partition leaders. For an
        // unknown topic metadata returns no partition, so AdminClient retries internally until its
        // default api timeout elapses. Use a dedicated short-timeout client so we don't sit on the
        // classwide 30 s default; the key assertion is simply that we never succeed.
        Properties props = adminProps();
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2_000);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
        try (Admin shortAdmin = KafkaAdminClient.create(props)) {
            String ghost = "ghost_" + System.nanoTime();
            TopicPartition tp = new TopicPartition(ghost, 0);
            Map<TopicPartition, RecordsToDelete> req = new HashMap<>();
            req.put(tp, RecordsToDelete.beforeOffset(1L));
            assertThatThrownBy(() -> shortAdmin.deleteRecords(req).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .satisfiesAnyOf(
                            t ->
                                    assertThat(t)
                                            .isInstanceOf(
                                                    org.apache.kafka.common.errors
                                                            .UnknownTopicOrPartitionException
                                                            .class),
                            t ->
                                    assertThat(t)
                                            .isInstanceOf(
                                                    org.apache.kafka.common.errors.TimeoutException
                                                            .class));
        }
    }

    private static long earliestOffset(TopicPartition tp) throws Exception {
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> spec = new HashMap<>();
        spec.put(tp, org.apache.kafka.clients.admin.OffsetSpec.earliest());
        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>
                result = admin.listOffsets(spec).all().get();
        org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo info =
                result.get(tp);
        assertThat(info).isNotNull();
        // Guard against accidental sentinel leakage.
        assertThat(info.offset()).isNotEqualTo(ListOffsetsRequest.EARLIEST_TIMESTAMP);
        return info.offset();
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
}
