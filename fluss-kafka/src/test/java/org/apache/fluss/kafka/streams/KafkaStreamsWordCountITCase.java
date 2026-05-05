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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
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
 * C.1 — Workstream C stateful-aggregation foundation (design 0018 §5). Runs a KTable word-count
 * topology ({@code stream → flatMapValues(split) → groupBy(word) → count → to}) against the
 * embedded Fluss KAFKA listener, using {@code AT_LEAST_ONCE} processing.
 *
 * <p>Verifies that:
 *
 * <ul>
 *   <li>Kafka Streams can create and use internal changelog + repartition topics on Fluss.
 *   <li>A stateful KTable aggregation produces correct counts when there are no crashes.
 *   <li>The output compacted topic reflects the final word-count state.
 * </ul>
 *
 * <p>This test is the prerequisite for C.4 ({@code KafkaStreamsExactlyOnceITCase}). Word pools and
 * sentence lengths are randomised per run — no hardcoded happy paths.
 */
class KafkaStreamsWordCountITCase {

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
    private EmbeddedStreamsApp app;

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

    @AfterEach
    void stopApp() {
        if (app != null) {
            app.close();
            app = null;
        }
    }

    @Test
    void wordCountStatefulAggregation() throws Exception {
        String suffix = String.valueOf(System.nanoTime());
        String inTopic = "wc_in_" + suffix;
        String outTopic = "wc_out_" + suffix;

        NewTopic outTopicDef = new NewTopic(outTopic, 1, (short) 1);
        outTopicDef.configs(Collections.singletonMap("cleanup.policy", "compact"));
        admin.createTopics(Arrays.asList(new NewTopic(inTopic, 1, (short) 1), outTopicDef))
                .all()
                .get();

        // Generate a random 4-word pool and 200 sentences. Track expected counts.
        Random rng = new Random();
        String[] wordPool = new String[4];
        for (int i = 0; i < wordPool.length; i++) {
            wordPool[i] = "w" + Long.toHexString(Math.abs(rng.nextLong())).substring(0, 6);
        }
        Map<String, Long> expected = new HashMap<>();
        for (String w : wordPool) {
            expected.put(w, 0L);
        }
        List<byte[]> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            int numWords = 1 + rng.nextInt(4);
            StringBuilder sentence = new StringBuilder();
            for (int j = 0; j < numWords; j++) {
                if (j > 0) {
                    sentence.append(' ');
                }
                String word = wordPool[rng.nextInt(wordPool.length)];
                sentence.append(word);
                expected.put(word, expected.get(word) + 1);
            }
            records.add(sentence.toString().getBytes(StandardCharsets.UTF_8));
        }

        // Produce all 200 records before starting Streams (pre-seed avoids startup race).
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (byte[] record : records) {
                producer.send(new ProducerRecord<>(inTopic, record)).get();
            }
            producer.flush();
        }

        // Build the word-count topology.
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .flatMapValues(
                        v -> Arrays.asList(new String(v, StandardCharsets.UTF_8).split("\\s+")))
                .groupBy((k, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count()
                .toStream()
                .to(outTopic, Produced.with(Serdes.String(), Serdes.Long()));
        Topology topology = builder.build();

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "wc-" + UUID.randomUUID());
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        streamsProps.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        streamsProps.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        streamsProps.put(
                StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "false");
        streamsProps.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "1");

        app = new EmbeddedStreamsApp(topology, streamsProps);
        app.start();
        app.waitForRunning(Duration.ofSeconds(30));

        // Drain output: fold into map (latest value per key) until it equals expected.
        Map<String, Long> outputMap = new HashMap<>();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "wc-drain-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);

        long drainDeadline = System.currentTimeMillis() + 60_000L;
        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(outTopic));
            while (!outputMap.equals(expected) && System.currentTimeMillis() < drainDeadline) {
                ConsumerRecords<String, Long> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Long> r : batch) {
                    if (r.key() != null && r.value() != null) {
                        outputMap.put(r.key(), r.value());
                    }
                }
            }
        }

        assertThat(app.uncaughtException())
                .as("Streams app must not throw an uncaught exception")
                .isNull();

        for (String word : wordPool) {
            assertThat(outputMap.get(word))
                    .as("count for word '%s'", word)
                    .isEqualTo(expected.get(word));
        }
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

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

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_TYPED_TABLES_ENABLED, false);
        return conf;
    }
}
