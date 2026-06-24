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

package org.apache.fluss.kafka.connect;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke IT that proves {@link EmbeddedConnectCluster} can stand up a real Kafka Connect worker in
 * standalone mode against the embedded {@link FlussClusterExtension} KAFKA listener, deploy a
 * {@code FileStreamSourceConnector} that reads 10 randomised lines from a {@link TempDir} file, and
 * that those 10 records land on our broker under the configured topic.
 *
 * <p>This is the harness's load-bearing smoke check — every downstream Connect IT depends on this
 * one going green. See design 0017 §F.
 */
class KafkaConnectHarnessSmokeITCase {

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

    private EmbeddedConnectCluster connect;

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
    void stopConnect() {
        if (connect != null) {
            connect.close();
            connect = null;
        }
    }

    @Test
    void fileStreamSourceDeliversTenLinesToBroker(@TempDir Path workDir) throws Exception {
        String topic = "connect_smoke_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Wait for the partition leader to be elected before starting the connector so
        // the Connect producer doesn't spin on LEADER_NOT_AVAILABLE retries.
        awaitTopicReady(topic);

        // 1. Write 10 randomised lines to a source file. FileStreamSourceConnector tails this.
        Path sourceFile = workDir.resolve("source.txt");
        List<String> lines = randomLines(10);
        try (BufferedWriter w = Files.newBufferedWriter(sourceFile, StandardCharsets.UTF_8)) {
            for (String line : lines) {
                w.write(line);
                w.newLine();
            }
        }

        // 2. Start an embedded Connect standalone worker against our KAFKA listener.
        // Disable idempotent producer: Kafka 3.9+ defaults to enable.idempotence=true (KIP-939),
        // which requires our broker to handle idempotent sequence tracking. Use acks=1 + no
        // idempotence until that feature is fully validated.
        Map<String, String> workerOverrides = new HashMap<>();
        workerOverrides.put("producer.enable.idempotence", "false");
        workerOverrides.put("producer.acks", "1");
        connect = new EmbeddedConnectCluster(bootstrap(), workerOverrides);
        connect.start();
        assertThat(connect.workerUri())
                .as("standalone worker should expose a REST listener once started")
                .isNotNull();
        assertThat(connect.stateDir()).exists();

        // 3. Deploy a FileStreamSource writing to `topic`.
        java.util.Map<String, String> cfg = new java.util.HashMap<>();
        cfg.put("connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector");
        cfg.put("tasks.max", "1");
        cfg.put("file", sourceFile.toAbsolutePath().toString());
        cfg.put("topic", topic);
        // FileStreamSource emits STRING records; the worker-level JsonConverter would require a
        // schema, so override key/value converters to STRING for this connector only.
        cfg.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        cfg.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        ConnectorInfo info = connect.startConnector("file-source", cfg);
        assertThat(info.name()).isEqualTo("file-source");

        // 4. Wait for the connector to transition to RUNNING.
        awaitRunning("file-source");

        // 5. Drain the topic via a kafka-clients consumer and assert we received all 10 lines.
        List<String> received = drainTopic(topic, lines.size(), Duration.ofSeconds(30));
        assertThat(received)
                .as(
                        "FileStreamSource should have delivered all %s lines to %s",
                        lines.size(), topic)
                .containsExactlyElementsOf(lines);

        // 6. Stop the connector cleanly; the herder's connectorInfo() forgets it asynchronously,
        // so poll for up to 5 s for connectorInfo() to either throw or return null.
        connect.stopConnector("file-source");
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        boolean gone = false;
        Throwable lastSeen = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo after =
                        connect.connectorInfo("file-source");
                if (after == null) {
                    gone = true;
                    break;
                }
            } catch (Exception expected) {
                lastSeen = expected;
                gone = true;
                break;
            }
            Thread.sleep(50);
        }
        assertThat(gone)
                .as(
                        "stopped connector should surface as not-found within 5 s (last error: %s)",
                        lastSeen)
                .isTrue();

        Path stateDir = connect.stateDir();
        connect.close();
        connect = null;
        assertThat(Files.exists(stateDir))
                .as("connect state dir should be removed after close()")
                .isFalse();
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    /**
     * Polls DescribeTopics until every partition of the topic has an elected leader. Without this
     * barrier the Connect producer can spin on LEADER_NOT_AVAILABLE retries for the first few
     * hundred milliseconds after CreateTopics returns, racing with the 30-second drain window.
     */
    private void awaitTopicReady(String topic) {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<String, TopicDescription> desc =
                        admin.describeTopics(Collections.singletonList(topic)).all().get();
                TopicDescription td = desc.get(topic);
                if (td != null) {
                    boolean allLeadersElected = true;
                    for (TopicPartitionInfo pi : td.partitions()) {
                        if (pi.leader() == null || pi.leader().id() < 0) {
                            allLeadersElected = false;
                            break;
                        }
                    }
                    if (allLeadersElected) {
                        return;
                    }
                }
            } catch (Exception ignored) {
                // DescribeTopics may transiently fail while metadata propagates.
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for topic leader", ie);
            }
        }
        throw new AssertionError("Topic " + topic + " did not get a leader within 30s");
    }

    private void awaitRunning(String connectorName) {
        long deadline = System.currentTimeMillis() + 30_000L;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConnectorStateInfo status = connect.connectorStatus(connectorName);
                assertThat(status.connector().state())
                        .as("connector state for %s", connectorName)
                        .isEqualTo("RUNNING");
                // Every task must be RUNNING too.
                for (ConnectorStateInfo.TaskState task : status.tasks()) {
                    assertThat(task.state())
                            .as("task %s state for %s", task.id(), connectorName)
                            .isEqualTo("RUNNING");
                }
                return;
            } catch (AssertionError ae) {
                last = ae;
            } catch (Exception e) {
                last = new AssertionError(e);
            }
            try {
                Thread.sleep(200L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
        }
        throw new AssertionError(
                "Connector " + connectorName + " did not reach RUNNING within 30s", last);
    }

    private List<String> drainTopic(String topic, int expected, Duration deadline) {
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

        List<String> collected = new ArrayList<>();
        long stop = System.currentTimeMillis() + deadline.toMillis();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.subscribe(Collections.singletonList(topic));
            while (collected.size() < expected && System.currentTimeMillis() < stop) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    // FileStreamSource with StringConverter writes JSON-quoted strings for the
                    // value; strip the leading/trailing quote emitted by StringConverter.
                    String raw = new String(r.value(), StandardCharsets.UTF_8);
                    collected.add(unwrapStringConverter(raw));
                }
            }
        }
        return collected;
    }

    private static String unwrapStringConverter(String raw) {
        // StringConverter emits the raw string bytes (UTF-8, no quoting). Be defensive in case
        // any transform appends a framing quote.
        if (raw.length() >= 2 && raw.charAt(0) == '"' && raw.charAt(raw.length() - 1) == '"') {
            return raw.substring(1, raw.length() - 1);
        }
        return raw;
    }

    private static List<String> randomLines(int count) {
        Random rng = new Random();
        List<String> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add("line-" + i + "-" + Long.toHexString(rng.nextLong()));
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
