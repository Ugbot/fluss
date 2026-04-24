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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the Fluss Kafka bolt-on from Python via {@code confluent-kafka-python} (librdkafka
 * bindings). Complements the Java / kcat paths with a third client language and a third protocol
 * parser. Runs the script at {@code fluss-kafka/src/test/python/fluss_kafka_e2e.py} as a
 * subprocess.
 *
 * <p>Skipped when no Python + confluent-kafka is on PATH. Operators can still run the script
 * manually against any Fluss cluster.
 */
class KafkaPythonE2EITCase {

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

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void pythonConfluentKafkaClient_roundTripsAllFourScenarios() throws Exception {
        Path python = resolvePythonWithConfluentKafka();
        Assumptions.assumeTrue(
                python != null,
                "Python with confluent-kafka not available. "
                        + "Install with: python3 -m venv /tmp/fluss-py-venv && "
                        + "/tmp/fluss-py-venv/bin/pip install confluent-kafka");
        Path script =
                Path.of(
                        System.getProperty("user.dir"),
                        "src",
                        "test",
                        "python",
                        "fluss_kafka_e2e.py");
        assertThat(Files.isRegularFile(script))
                .withFailMessage("python e2e script missing at %s", script)
                .isTrue();

        ProcessBuilder pb = new ProcessBuilder(python.toString(), script.toString(), bootstrap());
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder out = new StringBuilder();
        Thread reader =
                new Thread(
                        () -> {
                            try (BufferedReader br =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getInputStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = br.readLine()) != null) {
                                    out.append(line).append('\n');
                                }
                            } catch (IOException ignored) {
                            }
                        });
        reader.start();
        boolean finished = process.waitFor(120, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new AssertionError("Python e2e timed out:\n" + out);
        }
        reader.join(2_000);
        int exitCode = process.exitValue();

        assertThat(exitCode)
                .withFailMessage("Python e2e exit=%d, output:\n%s", exitCode, out)
                .isEqualTo(0);
        String output = out.toString();
        assertThat(output).contains("list_topics").contains("PASS");
        assertThat(output).contains("produce_consume_round_trip").contains("PASS");
        assertThat(output).contains("consumer_group_offset_commit").contains("PASS");
        assertThat(output).contains("list_offsets_watermarks").contains("PASS");
        assertThat(output).contains("Summary: 4/4 passed");
    }

    private static Path resolvePythonWithConfluentKafka() {
        // Prefer a venv at a well-known path we document in the script header. Fall back to system
        // python if the venv isn't present and the module is importable anyway.
        Path venvPython = Path.of("/tmp/fluss-py-venv/bin/python");
        if (Files.isExecutable(venvPython) && hasConfluentKafka(venvPython)) {
            return venvPython;
        }
        String pathEnv = System.getenv("PATH");
        if (pathEnv != null) {
            for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
                Path candidate = Path.of(dir, "python3");
                if (Files.isExecutable(candidate) && hasConfluentKafka(candidate)) {
                    return candidate;
                }
            }
        }
        return null;
    }

    private static boolean hasConfluentKafka(Path pythonBinary) {
        try {
            Process p =
                    new ProcessBuilder(pythonBinary.toString(), "-c", "import confluent_kafka")
                            .redirectErrorStream(true)
                            .start();
            boolean ok = p.waitFor(10, TimeUnit.SECONDS);
            return ok && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
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
