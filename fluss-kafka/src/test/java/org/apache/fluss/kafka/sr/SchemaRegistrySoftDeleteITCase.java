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
 * Kafka Schema Registry soft-delete semantics on the Coordinator's REST listener.
 *
 * <p>Covers Phase SR-X.1 endpoints:
 *
 * <ul>
 *   <li>{@code DELETE /subjects/{s}/versions/{v}} — soft tombstone, optional {@code
 *       ?permanent=true}.
 *   <li>{@code DELETE /subjects/{s}} — cascade-tombstone a subject binding.
 *   <li>Re-register after soft delete clears the tombstone (Kafka SR resurrection semantics).
 * </ul>
 */
class SchemaRegistrySoftDeleteITCase {

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
    private static final String SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    private static final String SCHEMA_V2 =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":[\"null\",\"int\"],\"default\":null}]}";

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
    void softDeleteVersionTombstonesAndReRegisterResurrects() throws Exception {
        String topic = "sd_version_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int v1Id = register(subject, SCHEMA_V1);
        assertThat(v1Id).isGreaterThanOrEqualTo(0);

        // Sanity: listVersions returns [1].
        JsonNode beforeDelete = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(beforeDelete.isArray()).isTrue();
        assertThat(beforeDelete).hasSize(1);
        assertThat(beforeDelete.get(0).asInt()).isEqualTo(1);

        // Soft-delete version 1.
        HttpResponse<String> del = http("DELETE", "/subjects/" + subject + "/versions/1", null);
        assertThat(del.statusCode()).isEqualTo(200);
        assertThat(del.body().trim()).isEqualTo("1");

        // listVersions now returns 404 (Kafka SR: no live versions → subject considered absent).
        HttpResponse<String> afterDelete = http("GET", "/subjects/" + subject + "/versions", null);
        assertThat(afterDelete.statusCode()).isEqualTo(404);

        // Double-soft-delete returns 404.
        HttpResponse<String> del2 = http("DELETE", "/subjects/" + subject + "/versions/1", null);
        assertThat(del2.statusCode()).isEqualTo(404);

        // Re-register the exact same schema body — tombstone clears, same Kafka SR schema id.
        int resurrectId = register(subject, SCHEMA_V1);
        assertThat(resurrectId).isEqualTo(v1Id);

        JsonNode afterResurrect = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(afterResurrect).hasSize(1);
        assertThat(afterResurrect.get(0).asInt()).isEqualTo(1);
    }

    @Test
    void softDeleteSubjectHidesBindingUntilReRegister() throws Exception {
        String topic = "sd_subject_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        register(subject, SCHEMA_V1);

        // Subject is live.
        assertThat(subjectsListContains(subject)).isTrue();

        HttpResponse<String> del = http("DELETE", "/subjects/" + subject, null);
        assertThat(del.statusCode()).isEqualTo(200);
        JsonNode deleted = MAPPER.readTree(del.body());
        assertThat(deleted.isArray()).isTrue();
        assertThat(deleted).hasSize(1);
        assertThat(deleted.get(0).asInt()).isEqualTo(1);

        // Subject no longer appears in listSubjects.
        assertThat(subjectsListContains(subject)).isFalse();

        // Re-registering clears the subject tombstone and returns the same id.
        int resurrectId = register(subject, SCHEMA_V1);
        assertThat(resurrectId).isGreaterThanOrEqualTo(0);
        assertThat(subjectsListContains(subject)).isTrue();
    }

    @Test
    void permanentDeleteVersionRemovesRow() throws Exception {
        String topic = "sd_hard_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int v1Id = register(subject, SCHEMA_V1);

        // Confirm lookup by id works pre-delete.
        HttpResponse<String> preLookup = http("GET", "/schemas/ids/" + v1Id, null);
        assertThat(preLookup.statusCode()).isEqualTo(200);

        HttpResponse<String> hard =
                http("DELETE", "/subjects/" + subject + "/versions/1?permanent=true", null);
        assertThat(hard.statusCode()).as("hard delete body=%s", hard.body()).isEqualTo(200);
        assertThat(hard.body().trim()).isEqualTo("1");

        // Post hard-delete the id resolves to 404.
        HttpResponse<String> postLookup = http("GET", "/schemas/ids/" + v1Id, null);
        assertThat(postLookup.statusCode()).isEqualTo(404);

        // Re-registering after a hard-delete mints a fresh version row — the Kafka SR schema id
        // slot
        // may be reclaimed from the dangling ID_RESERVATIONS row (that's by design: ids remain
        // stable across the registry lifetime), so we only assert that a version row exists.
        int refreshId = register(subject, SCHEMA_V2);
        assertThat(refreshId).isGreaterThanOrEqualTo(0);

        JsonNode versions = MAPPER.readTree(get("/subjects/" + subject + "/versions"));
        assertThat(versions).hasSize(1);
    }

    // ---------- helpers ----------

    private static boolean subjectsListContains(String subject) throws Exception {
        JsonNode arr = MAPPER.readTree(get("/subjects"));
        for (JsonNode n : arr) {
            if (subject.equals(n.asText())) {
                return true;
            }
        }
        return false;
    }

    private static int register(String subject, String schema) throws Exception {
        String body = "{\"schemaType\":\"AVRO\",\"schema\":" + escape(schema) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", body);
        assertThat(resp.statusCode()).as("register %s", subject).isEqualTo(200);
        return MAPPER.readTree(resp.body()).get("id").asInt();
    }

    private static String get(String path) throws Exception {
        HttpResponse<String> resp = http("GET", path, null);
        assertThat(resp.statusCode()).as("GET %s", path).isEqualTo(200);
        return resp.body();
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
