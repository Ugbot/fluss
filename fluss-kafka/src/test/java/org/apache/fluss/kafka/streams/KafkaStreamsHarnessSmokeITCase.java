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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke IT that proves {@link EmbeddedStreamsApp} can run a trivial topology ({@code stream("in") →
 * filter(v -> v != null) → to("out")}) against the embedded {@link FlussClusterExtension} KAFKA
 * listener.
 *
 * <p>Exercises the full Streams lifecycle: construction, {@link KafkaStreams.State#RUNNING} wait,
 * filter processing, consumer visibility on the output topic, clean {@link KafkaStreams#close()},
 * and per-test state-dir cleanup. Every downstream Streams IT depends on this one going green. See
 * design 0017 §F.
 */
class KafkaStreamsHarnessSmokeITCase {

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
    void trivialFilterTopologyForwardsTenRecordsEndToEnd() throws Exception {
        String inTopic = "streams_smoke_in_" + System.nanoTime();
        String outTopic = "streams_smoke_out_" + System.nanoTime();
        admin.createTopics(
                        Arrays.asList(
                                new NewTopic(inTopic, 1, (short) 1),
                                new NewTopic(outTopic, 1, (short) 1)))
                .all()
                .get();

        String applicationId = "streams-smoke-" + UUID.randomUUID();

        // 1. Build the topology: stream → filter(v != null) → to.
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .filter((k, v) -> v != null)
                .to(outTopic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        Topology topology = builder.build();

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        streamsProps.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        streamsProps.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        streamsProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        // Disable producer idempotence / EOS for this smoke test — phase J covers those paths.
        streamsProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        streamsProps.put(
                StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "false");
        streamsProps.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "1");

        // 2. Produce 10 randomised records to the input topic *before* the Streams app starts.
        //    Pre-seeding the topic and then relying on auto.offset.reset=earliest avoids any race
        //    where Streams' embedded admin/producer competes with our test producer on a busy
        //    single-broker setup. The trivial filter→to topology will pick everything up on its
        //    first poll.
        Random rng = new Random();
        List<byte[]> sent = new ArrayList<>(10);
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < 10; i++) {
                byte[] v = randomBytes(rng, 32);
                sent.add(v);
                p.send(new ProducerRecord<>(inTopic, 0, null, v)).get();
            }
            p.flush();
        }

        app = new EmbeddedStreamsApp(topology, streamsProps);
        Path stateDir = app.stateDir();
        app.start();
        app.waitForRunning(Duration.ofSeconds(30));
        assertThat(app.state()).isEqualTo(KafkaStreams.State.RUNNING);
        assertThat(stateDir).as("state.dir should exist once app is running").exists();

        // 3. Drain the output topic; assert we get exactly 10 and the bodies match.
        Set<String> expected = new HashSet<>();
        for (byte[] b : sent) {
            expected.add(hex(b));
        }
        List<byte[]> received = drainTopic(outTopic, 10, Duration.ofSeconds(30));
        assertThat(received).hasSize(10);
        Set<String> receivedHex = new HashSet<>();
        for (byte[] b : received) {
            receivedHex.add(hex(b));
        }
        assertThat(receivedHex).isEqualTo(expected);

        // 4. Uncaught-exception sentinel is empty — the app never dropped a record.
        assertThat(app.uncaughtException()).isNull();

        // 5. Close cleanly; state dir is removed.
        app.close();
        assertThat(app.state()).isEqualTo(KafkaStreams.State.NOT_RUNNING);
        assertThat(Files.exists(stateDir))
                .as("state dir should be deleted after close()")
                .isFalse();
        app = null; // prevent AfterEach double-close
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static byte[] randomBytes(Random rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(String.format("%02x", x & 0xff));
        }
        return sb.toString();
    }

    private List<byte[]> drainTopic(String topic, int expected, Duration deadline) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "drain-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);

        List<byte[]> out = new ArrayList<>();
        long stop = System.currentTimeMillis() + deadline.toMillis();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.subscribe(Collections.singletonList(topic));
            while (out.size() < expected && System.currentTimeMillis() < stop) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    out.add(r.value());
                }
            }
        }
        return out;
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

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // kafka-clients 3.x defaults enable.idempotence=true + acks=all; under a busy single-broker
        // Fluss cluster with the Streams app holding active sessions, the producer-side bookkeeping
        // for idempotent batching can stall. Disable both for the smoke test — Phase J (doc 0016)
        // covers the exactly-once / idempotent producer path with its own dedicated IT.
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
        return conf;
    }
}
