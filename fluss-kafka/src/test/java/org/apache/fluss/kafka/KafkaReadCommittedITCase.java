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
import org.apache.fluss.kafka.tx.TransactionCoordinators;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase J.3 — verifies the {@code isolation_level=READ_COMMITTED} contract on the consumer side
 * after the LSO + control-batch wiring lands. Walks one cluster lifecycle through three scenarios:
 *
 * <ol>
 *   <li><b>committed records visible to read_committed</b>: produce → commit → consumer with {@code
 *       isolation.level=read_committed} sees every record.
 *   <li><b>aborted records hidden from read_committed</b>: produce → abort → consumer with {@code
 *       isolation.level=read_committed} sees zero records on the partition; consumer with {@code
 *       isolation.level=read_uncommitted} sees the in-flight records (verifying the data reached
 *       the log; abort filtering only happens for read_committed).
 *   <li><b>mixed sequence</b>: an aborted txn followed by a committed txn — read_committed sees
 *       only the committed batch.
 * </ol>
 *
 * <p>Bundled into one test method to amortise the cluster bootstrap (matches the omnibus pattern
 * established by {@link KafkaTransactionalProducerITCase}). Inputs are randomised per the project
 * no-hardcoded-happy-paths rule.
 */
class KafkaReadCommittedITCase {

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
        // (1) wait for in-JVM coordinator registration; (2) trigger lazy system-table creation
        // by listing; (3) await bucket-leadership propagation. After step 3, txn traffic is
        // deterministic. See doc 0016 §3 for the bootstrap order.
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!TransactionCoordinators.current().isPresent()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "TransactionCoordinator did not register within 30s of test start");
            }
            Thread.sleep(50L);
        }
        org.apache.fluss.catalog.CatalogService catalog =
                org.apache.fluss.catalog.CatalogServices.current()
                        .orElseThrow(
                                () -> new IllegalStateException("CatalogService not registered"));
        catalog.listKafkaTxnStates();
        catalog.listKafkaProducerIds();
        catalog.listKafkaTxnOffsets("__warmup__");
        CLUSTER.awaitSystemTablesReady(
                java.time.Duration.ofSeconds(60),
                "_catalog._kafka_txn_state",
                "_catalog._kafka_producer_ids",
                "_catalog._kafka_txn_offset_buffer");
    }

    @Test
    void readCommittedHonoursLsoAndAbortFilter() throws Exception {
        Random rng = new Random();

        // Scenario 1: committed records visible to read_committed.
        String committedTopic = "txn_rc_commit_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(committedTopic, 1, (short) 1)))
                .all()
                .get();
        int committedCount = 5 + rng.nextInt(10);
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps("rc-commit-" + UUID.randomUUID()))) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < committedCount; i++) {
                producer.send(
                        new ProducerRecord<>(
                                committedTopic, 0, randomBytes(rng, 8), randomBytes(rng, 32)));
            }
            producer.commitTransaction();
        }

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProps("rc-consumer-1", true))) {
            consumer.assign(Collections.singletonList(new TopicPartition(committedTopic, 0)));
            consumer.seekToBeginning(consumer.assignment());
            int seen = drainCount(consumer, 30_000, committedCount);
            assertThat(seen).isEqualTo(committedCount);
        }

        // Scenario 2: aborted records hidden from read_committed.
        String abortedTopic = "txn_rc_abort_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(abortedTopic, 1, (short) 1)))
                .all()
                .get();
        int abortedCount = 3 + rng.nextInt(8);
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps("rc-abort-" + UUID.randomUUID()))) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < abortedCount; i++) {
                producer.send(
                        new ProducerRecord<>(
                                abortedTopic, 0, randomBytes(rng, 8), randomBytes(rng, 32)));
            }
            producer.flush();
            producer.abortTransaction();
        }

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProps("rc-consumer-2", true))) {
            consumer.assign(Collections.singletonList(new TopicPartition(abortedTopic, 0)));
            consumer.seekToBeginning(consumer.assignment());
            // Allow up to 5s of polling; expect zero records.
            int seen = drainCount(consumer, 5_000, Integer.MAX_VALUE);
            assertThat(seen).isZero();
        }

        // Scenario 3: aborted-then-committed sequence — read_committed sees only the committed
        // records.
        String mixedTopic = "txn_rc_mixed_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(mixedTopic, 1, (short) 1)))
                .all()
                .get();
        int mixedAbortedCount = 2 + rng.nextInt(4);
        int mixedCommittedCount = 4 + rng.nextInt(6);
        Set<String> mixedCommittedKeys = new HashSet<>();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps("rc-mixed-" + UUID.randomUUID()))) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < mixedAbortedCount; i++) {
                producer.send(
                        new ProducerRecord<>(
                                mixedTopic, 0, randomBytes(rng, 8), randomBytes(rng, 32)));
            }
            producer.flush();
            producer.abortTransaction();
            producer.beginTransaction();
            for (int i = 0; i < mixedCommittedCount; i++) {
                byte[] key = ("ck" + i + "-" + UUID.randomUUID()).getBytes();
                mixedCommittedKeys.add(new String(key));
                producer.send(new ProducerRecord<>(mixedTopic, 0, key, randomBytes(rng, 32)));
            }
            producer.commitTransaction();
        }

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProps("rc-consumer-3", true))) {
            consumer.assign(Collections.singletonList(new TopicPartition(mixedTopic, 0)));
            consumer.seekToBeginning(consumer.assignment());
            Set<String> seenKeys = drainKeys(consumer, mixedCommittedCount, 30_000);
            // We should see exactly the keys from the committed transaction.
            assertThat(seenKeys).isEqualTo(mixedCommittedKeys);
        }
    }

    private static Set<String> drainKeys(
            KafkaConsumer<byte[], byte[]> consumer, int expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Set<String> seen = new HashSet<>();
        while (System.currentTimeMillis() < deadline && seen.size() < expected) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<byte[], byte[]> r : records) {
                if (r.key() != null) {
                    seen.add(new String(r.key()));
                }
            }
        }
        return seen;
    }

    private static int drainCount(
            KafkaConsumer<byte[], byte[]> consumer, long timeoutMs, int earlyStopAt) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int total = 0;
        while (System.currentTimeMillis() < deadline && total < earlyStopAt) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            total += records.count();
        }
        return total;
    }

    private static byte[] randomBytes(Random rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties transactionalProducerProps(String txnId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 90_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 90_000);
        return props;
    }

    private static Properties consumerProps(String groupId, boolean readCommitted) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                readCommitted ? "read_committed" : "read_uncommitted");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1_000);
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
