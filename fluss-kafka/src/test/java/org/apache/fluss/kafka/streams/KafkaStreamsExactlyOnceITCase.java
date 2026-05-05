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

package org.apache.fluss.kafka.streams;

import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * C.4 — EOS word-count proof. Runs the same KTable word-count topology as {@link
 * KafkaStreamsWordCountITCase} but with {@code processing.guarantee=exactly_once_v2}, a forced
 * mid-stream crash, and a second app instance that recovers from the changelog topic.
 *
 * <p>Verifies that:
 *
 * <ul>
 *   <li>Kafka Streams can produce output under EOS against the Fluss Kafka listener.
 *   <li>After a crash (simulated by {@code streams.close(Duration.ZERO)}) the restarted app (same
 *       {@code application.id}) re-fences the crashed producer via an epoch bump and recovers state
 *       from the changelog topic on Fluss.
 *   <li>A {@code READ_COMMITTED} consumer on the output topic sees every word counted exactly once
 *       across batch A + batch B — no duplicates, no missing records.
 * </ul>
 *
 * <p>Word pools and sentence lengths are randomised per run — no hardcoded happy paths.
 *
 * <p>Requires the full transaction coordinator bootstrap ({@code ensureKafkaDatabase}) because
 * {@code exactly_once_v2} issues {@code INIT_PRODUCER_ID} and full transaction RPCs on every commit
 * cycle.
 */
class KafkaStreamsExactlyOnceITCase {

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
    private EmbeddedStreamsApp app1;
    private EmbeddedStreamsApp app2;

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
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!TransactionCoordinators.current().isPresent()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "TransactionCoordinator did not register within 30s of test start");
            }
            Thread.sleep(50L);
        }
        CatalogService catalog =
                CatalogServices.current()
                        .orElseThrow(
                                () -> new IllegalStateException("CatalogService not registered"));
        catalog.listKafkaTxnStates();
        catalog.listKafkaProducerIds();
        catalog.listKafkaTxnOffsets("__warmup__");
        catalog.allocateProducerId(null);
        CLUSTER.awaitSystemTablesReady(
                Duration.ofSeconds(60),
                "_catalog._kafka_txn_state",
                "_catalog._kafka_producer_ids",
                "_catalog._kafka_txn_offset_buffer");
    }

    @AfterEach
    void stopApps() {
        if (app1 != null) {
            app1.close();
            app1 = null;
        }
        if (app2 != null) {
            app2.close();
            app2 = null;
        }
    }

    @Test
    void exactlyOnceV2ZeroDuplicates() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        String inTopic = "eos_in_" + suffix;
        String outTopic = "eos_out_" + suffix;
        // Unique appId per run; no hyphens in interior to avoid Fluss topic-name collisions on
        // the internal changelog topics that Streams creates with the appId as prefix.
        String appId = "eostest" + UUID.randomUUID().toString().replace("-", "");

        NewTopic outTopicDef = new NewTopic(outTopic, 1, (short) 1);
        outTopicDef.configs(Collections.singletonMap("cleanup.policy", "compact"));
        admin.createTopics(Arrays.asList(new NewTopic(inTopic, 1, (short) 1), outTopicDef))
                .all()
                .get();

        // 6-word pool, randomised per run.
        Random rng = new Random();
        String[] wordPool = new String[6];
        for (int i = 0; i < wordPool.length; i++) {
            wordPool[i] = "w" + Long.toHexString(Math.abs(rng.nextLong())).substring(0, 6);
        }

        // Batch A: 500 records.
        Map<String, Long> expectedA = new HashMap<>();
        for (String w : wordPool) {
            expectedA.put(w, 0L);
        }
        List<byte[]> batchA = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            int numWords = 1 + rng.nextInt(4);
            StringBuilder sentence = new StringBuilder();
            for (int j = 0; j < numWords; j++) {
                if (j > 0) {
                    sentence.append(' ');
                }
                String word = wordPool[rng.nextInt(wordPool.length)];
                sentence.append(word);
                expectedA.put(word, expectedA.get(word) + 1);
            }
            batchA.add(sentence.toString().getBytes(StandardCharsets.UTF_8));
        }

        // Produce batch A before starting Streams (pre-seed avoids startup race).
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (byte[] record : batchA) {
                producer.send(new ProducerRecord<>(inTopic, record)).get();
            }
            producer.flush();
        }

        Topology topology = buildWordCountTopology(inTopic, outTopic);

        // Phase 1 — start app1 and let it run until at least one EOS commit has landed.
        app1 = new EmbeddedStreamsApp(topology, eosStreamsProps(appId));
        app1.start();
        app1.waitForRunning(Duration.ofSeconds(45));
        waitForFirstCommittedOutput(outTopic, 30_000L);

        // Phase 2 — crash app1 (no cooperative shutdown; simulates a process kill).
        app1.streams().close(Duration.ZERO);
        long stopDeadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < stopDeadline) {
            KafkaStreams.State s = app1.state();
            if (s == KafkaStreams.State.NOT_RUNNING || s == KafkaStreams.State.ERROR) {
                break;
            }
            Thread.sleep(100L);
        }

        // Phase 3 — batch B: 500 more records, produced after the crash.
        Map<String, Long> expectedB = new HashMap<>();
        for (String w : wordPool) {
            expectedB.put(w, 0L);
        }
        List<byte[]> batchB = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            int numWords = 1 + rng.nextInt(4);
            StringBuilder sentence = new StringBuilder();
            for (int j = 0; j < numWords; j++) {
                if (j > 0) {
                    sentence.append(' ');
                }
                String word = wordPool[rng.nextInt(wordPool.length)];
                sentence.append(word);
                expectedB.put(word, expectedB.get(word) + 1);
            }
            batchB.add(sentence.toString().getBytes(StandardCharsets.UTF_8));
        }
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (byte[] record : batchB) {
                producer.send(new ProducerRecord<>(inTopic, record)).get();
            }
            producer.flush();
        }

        // expectedFinal = per-word sum of batch A + batch B.
        Map<String, Long> expectedFinal = new HashMap<>();
        for (String w : wordPool) {
            expectedFinal.put(w, expectedA.get(w) + expectedB.get(w));
        }

        // App2 uses the same appId → same transactional-id namespace → epoch bump fences app1.
        // App2's state dir is fresh (new UUID); state is restored from the changelog on Fluss.
        app2 = new EmbeddedStreamsApp(topology, eosStreamsProps(appId));
        app2.start();
        app2.waitForRunning(Duration.ofSeconds(60));

        // Drain the output topic with read_committed until it matches expectedFinal or timeout.
        Map<String, Long> outputMap = new HashMap<>();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-drain-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);

        long drainDeadline = System.currentTimeMillis() + 120_000L;
        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(outTopic));
            while (!outputMap.equals(expectedFinal) && System.currentTimeMillis() < drainDeadline) {
                ConsumerRecords<String, Long> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Long> r : batch) {
                    if (r.key() != null && r.value() != null) {
                        outputMap.put(r.key(), r.value());
                    }
                }
            }
        }

        assertThat(app2.uncaughtException())
                .as("Streams app2 must not throw an uncaught exception")
                .isNull();

        for (String word : wordPool) {
            assertThat(outputMap.get(word))
                    .as(
                            "count for word '%s' (expectedA=%d, expectedB=%d)",
                            word, expectedA.get(word), expectedB.get(word))
                    .isEqualTo(expectedFinal.get(word));
        }
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private void waitForFirstCommittedOutput(String topic, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return;
                }
            }
        }
        throw new AssertionError(
                "No committed output record appeared in '"
                        + topic
                        + "' within "
                        + timeoutMs
                        + "ms — app1 may not have completed an EOS commit cycle");
    }

    private static Topology buildWordCountTopology(String inTopic, String outTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .flatMapValues(
                        v -> Arrays.asList(new String(v, StandardCharsets.UTF_8).split("\\s+")))
                .groupBy((k, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private Properties eosStreamsProps(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        // Short session timeout so the stale-member (app1 after crash) is evicted within ~10s
        // by the server's session reaper rather than the Kafka 3.x default of 45s. Without this
        // the crash-recovery rebalance would take ≥45s, exceeding the waitForRunning deadline.
        props.put(
                StreamsConfig.consumerPrefix(
                        org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                10_000);
        return props;
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return p;
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return p;
    }

    private static String bootstrap() {
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        StringBuilder b = new StringBuilder();
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
