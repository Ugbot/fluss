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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confluent Schema Registry compatibility-gate semantics on the Coordinator's REST listener.
 *
 * <p>Covers Phase SR-X.2 behaviour:
 *
 * <ul>
 *   <li>Registration is rejected with HTTP 409 when the proposed schema is incompatible with the
 *       configured level (default {@code BACKWARD}).
 *   <li>{@code POST /compatibility/subjects/{s}/versions/latest} never registers — {@code
 *       listVersions} is unchanged after a compat probe.
 *   <li>{@code PUT /config/{subject}} switches the effective level per subject, exercising
 *       FORWARD-pass (drop optional field) and FULL (both directions) on the same base history.
 * </ul>
 */
class SchemaRegistryCompatibilityITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;

    private static final int SR_PORT = freePort();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Base: one required field. */
    private static final String BASE =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"}]}";

    /**
     * Backward-compatible extension of {@link #BASE}: adds an optional field with a default. A new
     * reader (this schema) can read old data (BASE) because the extra field falls back to its
     * default.
     */
    private static final String ADD_OPTIONAL =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\",\"default\":0}]}";

    /**
     * Backward-incompatible with {@link #BASE}: adds a required {@code qty} with no default. A new
     * reader expects {@code qty} but old data written by {@link #BASE} lacks it.
     */
    private static final String ADD_REQUIRED =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\"}]}";

    /**
     * Backward-incompatible with {@link #ADD_OPTIONAL}: introduces a different required field
     * without a default. A reader with this schema cannot read rows written by {@link
     * #ADD_OPTIONAL} (no {@code note} to decode).
     */
    private static final String ADD_REQUIRED_NEW =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\",\"default\":0},"
                    + "{\"name\":\"note\",\"type\":\"string\"}]}";

    /**
     * Forward-compatible vs {@link #ADD_OPTIONAL}: drops the optional {@code qty} (which had a
     * default) and adds a different optional {@code note}. Text differs from {@link #BASE}, so this
     * is a distinct registration rather than an idempotent duplicate.
     *
     * <p>Old reader ({@link #ADD_OPTIONAL}) must read data written by this schema. The dropped
     * {@code qty} has a default, so the reader fills it in; the new {@code note} is ignored.
     */
    private static final String DROP_OPTIONAL =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}]}";

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
    void backwardAllowsOptionalAdditionRejectsRequiredAddition() throws Exception {
        String topic = "compat_backward_" + System.nanoTime();
        String subject = topic + "-value";
        createTopic(topic);

        // v1 registers fine.
        int v1 = register(subject, BASE);
        assertThat(v1).isGreaterThanOrEqualTo(0);

        // BACKWARD pass: add optional field with default.
        int v2 = register(subject, ADD_OPTIONAL);
        assertThat(v2).isNotEqualTo(v1);

        JsonNode versions = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(versions).hasSize(2);

        // BACKWARD fail: a different required field — reader expects 'note' (required) but the
        // latest prior ({@link #ADD_OPTIONAL}) does not write it. Must return 409.
        HttpResponse<String> bad =
                http("POST", "/subjects/" + subject + "/versions", registerBody(ADD_REQUIRED_NEW));
        assertThat(bad.statusCode())
                .as("register of incompatible schema should return 409, body=%s", bad.body())
                .isEqualTo(409);

        // History unchanged after the failed attempt.
        JsonNode versionsAfter = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(versionsAfter).hasSize(2);
    }

    @Test
    void compatibilityProbeDoesNotRegister() throws Exception {
        String topic = "compat_probe_" + System.nanoTime();
        String subject = topic + "-value";
        createTopic(topic);

        register(subject, BASE);

        JsonNode before = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(before).hasSize(1);

        // Probe with a compatible schema.
        HttpResponse<String> probeOk =
                http(
                        "POST",
                        "/compatibility/subjects/" + subject + "/versions/latest",
                        registerBody(ADD_OPTIONAL));
        assertThat(probeOk.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(probeOk.body()).get("is_compatible").asBoolean()).isTrue();

        // Probe with an incompatible one — HTTP 200 but is_compatible=false, with messages.
        HttpResponse<String> probeBad =
                http(
                        "POST",
                        "/compatibility/subjects/" + subject + "/versions/latest",
                        registerBody(ADD_REQUIRED));
        assertThat(probeBad.statusCode()).isEqualTo(200);
        JsonNode badJson = MAPPER.readTree(probeBad.body());
        assertThat(badJson.get("is_compatible").asBoolean()).isFalse();
        assertThat(badJson.get("messages").isArray()).isTrue();
        assertThat(badJson.get("messages").size()).isGreaterThan(0);

        // Either probe must NOT alter the version history.
        JsonNode after = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(after).hasSize(1);
    }

    @Test
    void forwardCompatibilityPermitsDroppingOptionalField() throws Exception {
        String topic = "compat_forward_" + System.nanoTime();
        String subject = topic + "-value";
        createTopic(topic);

        // Register base then optional-added v2 under BACKWARD (default).
        register(subject, BASE);
        register(subject, ADD_OPTIONAL);

        // Switch this subject to FORWARD.
        HttpResponse<String> putCfg = put("/config/" + subject, "{\"compatibility\":\"FORWARD\"}");
        assertThat(putCfg.statusCode()).isEqualTo(200);

        // FORWARD pass: drop the optional field. Old reader (ADD_OPTIONAL) must read data written
        // by new (DROP_OPTIONAL); it can, because the dropped field had a default.
        int v3 = register(subject, DROP_OPTIONAL);
        assertThat(v3).isGreaterThanOrEqualTo(0);

        JsonNode versions = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(versions).hasSize(3);
    }

    @Test
    void fullCompatibilityCombinesBothDirections() throws Exception {
        String topic = "compat_full_" + System.nanoTime();
        String subject = topic + "-value";
        createTopic(topic);

        register(subject, BASE);

        // Switch to FULL before the second register.
        HttpResponse<String> putCfg = put("/config/" + subject, "{\"compatibility\":\"FULL\"}");
        assertThat(putCfg.statusCode()).isEqualTo(200);

        // Adding an optional field with a default is both backward- and forward-compatible.
        int v2 = register(subject, ADD_OPTIONAL);
        assertThat(v2).isGreaterThanOrEqualTo(0);

        // Adding a new required field (ADD_REQUIRED_NEW) breaks BACKWARD — data written by the
        // latest prior (ADD_OPTIONAL) lacks 'note' and the new reader has no default for it.
        HttpResponse<String> bad =
                http("POST", "/subjects/" + subject + "/versions", registerBody(ADD_REQUIRED_NEW));
        assertThat(bad.statusCode())
                .as("register of BACKWARD-incompatible under FULL should 409, body=%s", bad.body())
                .isEqualTo(409);
    }

    // ---------- helpers ----------

    private static void createTopic(String topic) throws Exception {
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
    }

    private static int register(String subject, String schema) throws Exception {
        HttpResponse<String> resp =
                http("POST", "/subjects/" + subject + "/versions", registerBody(schema));
        assertThat(resp.statusCode())
                .as("register %s body=%s", subject, resp.body())
                .isEqualTo(200);
        return MAPPER.readTree(resp.body()).get("id").asInt();
    }

    private static String registerBody(String schema) {
        return "{\"schemaType\":\"AVRO\",\"schema\":" + escape(schema) + "}";
    }

    private static String get(String path) throws Exception {
        HttpResponse<String> resp = http("GET", path, null);
        assertThat(resp.statusCode()).as("GET %s", path).isEqualTo(200);
        return resp.body();
    }

    private static HttpResponse<String> put(String path, String body) throws Exception {
        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static HttpResponse<String> http(String method, String path, String body)
            throws Exception {
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json");
        switch (method) {
            case "POST":
                req.POST(
                        HttpRequest.BodyPublishers.ofString(
                                body == null ? "" : body, StandardCharsets.UTF_8));
                break;
            case "DELETE":
                req.DELETE();
                break;
            case "GET":
            default:
                req.GET();
                break;
        }
        return HTTP.send(req.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static String escape(String raw) {
        StringBuilder sb = new StringBuilder(raw.length() + 2);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        List<org.apache.fluss.cluster.ServerNode> nodes =
                CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, b.toString());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
        return conf;
    }
}
