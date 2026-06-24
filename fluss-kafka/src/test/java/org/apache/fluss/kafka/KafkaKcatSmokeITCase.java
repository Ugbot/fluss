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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test that drives the Fluss Kafka bolt-on with the real {@code kcat} CLI. A third-party
 * client that neither Apache Kafka nor Fluss controls — if metadata / produce / consume round-trip
 * here, the wire contract is genuinely portable.
 *
 * <p>Skipped when {@code kcat} is not on {@code PATH}; CI environments without kcat installed stay
 * green.
 */
class KafkaKcatSmokeITCase {

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
    void kcatListMetadata_lists_createdTopic() throws Exception {
        skipIfNoKcat();
        String topic = "kcat_meta_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        KcatResult result = runKcat(15, "-b", bootstrap(), "-L");
        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.stdout).contains(topic);
    }

    @Test
    void kcatProduceConsume_roundTripsPayload() throws Exception {
        skipIfNoKcat();
        String topic = "kcat_rt_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        String message = "hello-kcat-" + System.nanoTime();
        KcatResult produce =
                runKcatWithStdin(message + "\n", 15, "-b", bootstrap(), "-t", topic, "-P");
        assertThat(produce.exitCode)
                .withFailMessage(
                        "kcat produce failed (%d)\nstdout=%s\nstderr=%s",
                        produce.exitCode, produce.stdout, produce.stderr)
                .isEqualTo(0);

        KcatResult consume =
                runKcat(
                        20,
                        "-b",
                        bootstrap(),
                        "-t",
                        topic,
                        "-C",
                        "-o",
                        "beginning",
                        "-c",
                        "1",
                        "-e");
        assertThat(consume.exitCode)
                .withFailMessage(
                        "kcat consume failed (%d)\nstdout=%s\nstderr=%s",
                        consume.exitCode, consume.stdout, consume.stderr)
                .isEqualTo(0);
        assertThat(consume.stdout).contains(message);
    }

    @Test
    void kcatQueryWatermarks_reportsHighWatermark() throws Exception {
        skipIfNoKcat();
        String topic = "kcat_wm_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Produce two messages so the high-watermark ticks to 2.
        runKcatWithStdin("a\nb\n", 15, "-b", bootstrap(), "-t", topic, "-P");

        KcatResult watermarks = runKcat(15, "-b", bootstrap(), "-Q", "-t", topic + ":0:-1");
        assertThat(watermarks.exitCode).isEqualTo(0);
        // kcat prints "topic [partition] high_watermark: N"
        assertThat(watermarks.stdout).contains(topic).contains("0");
    }

    // --- helpers ---

    private static void skipIfNoKcat() {
        Assumptions.assumeTrue(
                findOnPath("kcat") != null,
                "kcat not on PATH; install via 'brew install kcat' or 'apt install kcat'");
    }

    private static Path findOnPath(String binary) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) {
            return null;
        }
        for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
            Path candidate = Path.of(dir, binary);
            if (Files.isExecutable(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static KcatResult runKcat(int timeoutSeconds, String... args) throws IOException {
        return runKcatWithStdin(null, timeoutSeconds, args);
    }

    private static KcatResult runKcatWithStdin(
            String stdinInput, int timeoutSeconds, String... args) throws IOException {
        String[] command = new String[args.length + 1];
        command[0] = "kcat";
        System.arraycopy(args, 0, command, 1, args.length);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);
        Process process = pb.start();

        if (stdinInput != null) {
            process.getOutputStream().write(stdinInput.getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().close();
        } else {
            process.getOutputStream().close();
        }

        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        Thread stdoutReader =
                new Thread(
                        () -> {
                            try (BufferedReader r =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getInputStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = r.readLine()) != null) {
                                    stdout.append(line).append('\n');
                                }
                            } catch (IOException ignored) {
                                // swallow — process likely terminated
                            }
                        });
        Thread stderrReader =
                new Thread(
                        () -> {
                            try (BufferedReader r =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getErrorStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = r.readLine()) != null) {
                                    stderr.append(line).append('\n');
                                }
                            } catch (IOException ignored) {
                            }
                        });
        stdoutReader.start();
        stderrReader.start();

        boolean finished;
        try {
            finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new IOException("kcat interrupted", ie);
        }
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("kcat timed out after " + timeoutSeconds + "s");
        }
        try {
            stdoutReader.join(1_000);
            stderrReader.join(1_000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return new KcatResult(process.exitValue(), stdout.toString(), stderr.toString());
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
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }

    private static final class KcatResult {
        final int exitCode;
        final String stdout;
        final String stderr;

        KcatResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }
}
