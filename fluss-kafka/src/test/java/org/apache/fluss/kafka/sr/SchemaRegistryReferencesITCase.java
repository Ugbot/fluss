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
 * End-to-end coverage of Phase SR-X.5 — Kafka Schema Registry {@code references} arrays on register
 * / read / referenced-by / delete.
 *
 * <p>Drives every scenario over the live HTTP listener with a raw {@link java.net.http.HttpClient},
 * mirroring the pattern in {@link SchemaRegistryHttpITCase} and {@link
 * SchemaRegistrySoftDeleteITCase}. Six scenarios from design-doc 0013 §11 are covered:
 *
 * <ol>
 *   <li>Register subject A v1 (Avro) and subject B v1 with a single {@code (com.example.A, A, 1)}
 *       reference; both registers return 200 with non-negative ids.
 *   <li>{@code GET /subjects/{B}/versions/1} echoes the {@code references} array.
 *   <li>{@code GET /schemas/ids/{a-id}/referencedby} returns {@code [{subject: B, version: 1}]}.
 *   <li>{@code DELETE /subjects/{A}/versions/1?permanent=true} → 422 with referrer list.
 *   <li>Soft-then-hard-delete B v1, then hard-delete A v1, all succeed.
 *   <li>Register subject C v1 with a missing reference target → 422 with the Kafka SR-style
 *       "referenced subject ... does not exist" message.
 * </ol>
 *
 * <p>All six scenarios run sequentially in one {@code @Test} method to avoid the cluster-warm-up
 * tax the test extension pays whenever the {@code _catalog} database is recreated. Subjects are
 * randomised per-run so re-runs against a long-lived cluster don't collide.
 */
class SchemaRegistryReferencesITCase {

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

    /** Avro schema for subject A: a record named {@code com.example.A} with a single int field. */
    private static final String SCHEMA_A =
            "{\"type\":\"record\",\"name\":\"A\",\"namespace\":\"com.example\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

    /**
     * Avro schema for subject B that references {@code com.example.A} as the type of its {@code
     * inner} field. Avro's parser accumulates named types across {@code parse(String)} calls (see
     * design 0013 §6 — "Avro" notes), so the referent is fed into the parser before this text
     * during compatibility checks.
     */
    private static final String SCHEMA_B =
            "{\"type\":\"record\",\"name\":\"B\",\"namespace\":\"com.example\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"inner\",\"type\":\"com.example.A\"}]}";

    /** Subject C's body is irrelevant — the test only exercises the missing-reference 422 arm. */
    private static final String SCHEMA_C =
            "{\"type\":\"record\",\"name\":\"C\",\"namespace\":\"com.example\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

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
    void schemaReferencesLifecycle() throws Exception {
        // Each run uses fresh subject names so the test is repeatable against a single cluster
        // lifecycle and so two registers of A under different runs don't entangle id reservations.
        long stamp = System.nanoTime();
        String topicA = "ref_a_" + stamp;
        String topicB = "ref_b_" + stamp;
        String topicC = "ref_c_" + stamp;
        String subjectA = topicA + "-value";
        String subjectB = topicB + "-value";
        String subjectC = topicC + "-value";

        admin.createTopics(
                        java.util.Arrays.asList(
                                new NewTopic(topicA, 1, (short) 1),
                                new NewTopic(topicB, 1, (short) 1),
                                new NewTopic(topicC, 1, (short) 1)))
                .all()
                .get();

        // Scenario 1: register A v1 and B v1 (B carries one reference to A).
        int aId = registerExpectingOk(subjectA, SCHEMA_A, Collections.<JsonNode>emptyList());
        assertThat(aId).isGreaterThanOrEqualTo(0);

        JsonNode bRef = makeReference("com.example.A", subjectA, 1);
        int bId = registerExpectingOk(subjectB, SCHEMA_B, Collections.singletonList(bRef));
        assertThat(bId).isGreaterThanOrEqualTo(0);
        assertThat(bId).isNotEqualTo(aId);

        // Scenario 2: GET /subjects/{B}/versions/1 echoes the references array in Kafka SR shape.
        HttpResponse<String> bV1 = http("GET", "/subjects/" + subjectB + "/versions/1", null);
        assertThat(bV1.statusCode())
                .as("GET subjects/%s/versions/1 body=%s", subjectB, bV1.body())
                .isEqualTo(200);
        JsonNode bV1Body = MAPPER.readTree(bV1.body());
        assertThat(bV1Body.get("subject").asText()).isEqualTo(subjectB);
        assertThat(bV1Body.get("version").asInt()).isEqualTo(1);
        JsonNode echoedRefs = bV1Body.get("references");
        assertThat(echoedRefs).as("references array on /subjects/{s}/versions/{v}").isNotNull();
        assertThat(echoedRefs.isArray()).isTrue();
        assertThat(echoedRefs).hasSize(1);
        assertThat(echoedRefs.get(0).get("name").asText()).isEqualTo("com.example.A");
        assertThat(echoedRefs.get(0).get("subject").asText()).isEqualTo(subjectA);
        assertThat(echoedRefs.get(0).get("version").asInt()).isEqualTo(1);

        // Scenario 3: GET /schemas/ids/{a-id}/referencedby returns [{subject: B-subject, v: 1}].
        HttpResponse<String> referencedBy =
                http("GET", "/schemas/ids/" + aId + "/referencedby", null);
        assertThat(referencedBy.statusCode())
                .as("GET /schemas/ids/%s/referencedby body=%s", aId, referencedBy.body())
                .isEqualTo(200);
        JsonNode referrers = MAPPER.readTree(referencedBy.body());
        assertThat(referrers.isArray()).isTrue();
        assertThat(referrers).hasSize(1);
        assertThat(referrers.get(0).get("subject").asText()).isEqualTo(subjectB);
        assertThat(referrers.get(0).get("version").asInt()).isEqualTo(1);

        // Scenario 4: hard-delete A v1 → 422 because B v1 still references it.
        HttpResponse<String> deleteABlocked =
                http("DELETE", "/subjects/" + subjectA + "/versions/1?permanent=true", null);
        assertThat(deleteABlocked.statusCode())
                .as("hard-delete of referenced A body=%s", deleteABlocked.body())
                .isEqualTo(422);
        // The Kafka SR-shape error message must enumerate the blocking referrer.
        assertThat(deleteABlocked.body()).contains(subjectB);
        assertThat(deleteABlocked.body()).contains("v1");

        // Scenario 5: soft-delete B v1, then hard-delete B v1, then hard-delete A v1.
        HttpResponse<String> softB = http("DELETE", "/subjects/" + subjectB + "/versions/1", null);
        assertThat(softB.statusCode()).as("soft-delete B body=%s", softB.body()).isEqualTo(200);
        assertThat(softB.body().trim()).isEqualTo("1");

        // A soft-deleted referent still blocks hard-delete of the referent (graph still walkable);
        // we have to hard-delete the referrer too. This matches design 0013 §7 "soft-delete of a
        // referent" / Kafka SR semantics.
        HttpResponse<String> hardB =
                http("DELETE", "/subjects/" + subjectB + "/versions/1?permanent=true", null);
        assertThat(hardB.statusCode()).as("hard-delete B body=%s", hardB.body()).isEqualTo(200);
        assertThat(hardB.body().trim()).isEqualTo("1");

        HttpResponse<String> hardA =
                http("DELETE", "/subjects/" + subjectA + "/versions/1?permanent=true", null);
        assertThat(hardA.statusCode()).as("hard-delete A body=%s", hardA.body()).isEqualTo(200);
        assertThat(hardA.body().trim()).isEqualTo("1");

        // Sanity: the schema-id lookup for A is now 404.
        HttpResponse<String> lookupAGone = http("GET", "/schemas/ids/" + aId, null);
        assertThat(lookupAGone.statusCode()).isEqualTo(404);

        // Scenario 6: register C with a reference whose subject does not exist → 422 with the
        // Kafka SR-shape "referenced subject X version Y does not exist" message.
        JsonNode missingRef = makeReference("missing", "does-not-exist-subject-value", 1);
        HttpResponse<String> registerC =
                http(
                        "POST",
                        "/subjects/" + subjectC + "/versions",
                        registerBody(SCHEMA_C, Collections.singletonList(missingRef)));
        assertThat(registerC.statusCode())
                .as("register C with missing ref body=%s", registerC.body())
                .isEqualTo(422);
        assertThat(registerC.body()).contains("referenced subject");
        assertThat(registerC.body()).contains("does not exist");
    }

    // ---------- helpers ----------

    /**
     * Build a Jackson {@link JsonNode} encoding of one Kafka SR-shape reference triple {@code
     * {name, subject, version}}.
     */
    private static JsonNode makeReference(String name, String subject, int version) {
        org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode node =
                MAPPER.createObjectNode();
        node.put("name", name);
        node.put("subject", subject);
        node.put("version", version);
        return node;
    }

    /**
     * Issue {@code POST /subjects/{s}/versions} with the supplied schema text + references; assert
     * a 200 response and return the assigned Kafka SR schema id.
     */
    private static int registerExpectingOk(String subject, String schema, List<JsonNode> refs)
            throws Exception {
        HttpResponse<String> resp =
                http("POST", "/subjects/" + subject + "/versions", registerBody(schema, refs));
        assertThat(resp.statusCode())
                .as("register %s body=%s", subject, resp.body())
                .isEqualTo(200);
        JsonNode body = MAPPER.readTree(resp.body());
        return body.get("id").asInt();
    }

    /** Compose a register request body with optional references. */
    private static String registerBody(String schema, List<JsonNode> refs) {
        org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode body =
                MAPPER.createObjectNode();
        body.put("schemaType", "AVRO");
        body.put("schema", schema);
        if (refs != null && !refs.isEmpty()) {
            org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode arr =
                    body.putArray("references");
            for (JsonNode ref : refs) {
                arr.add(ref);
            }
        }
        try {
            return MAPPER.writeValueAsString(body);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpResponse<String> http(String method, String path, String body)
            throws Exception {
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(30))
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
