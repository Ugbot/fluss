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
 * End-to-end test of the Schema Registry HTTP listener on the Coordinator leader. Hits each Phase
 * A1 endpoint from a raw {@link java.net.http.HttpClient} — no Confluent client dependency.
 */
class SchemaRegistryHttpITCase {

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
    private static final String SCHEMA_ORDER =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    // BACKWARD-compatible with SCHEMA_ORDER: added optional field with default.
    private static final String SCHEMA_ORDER_V2 =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\",\"default\":0}]}";

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
    void registerAndRetrieveRoundTrip() throws Exception {
        String topic = "orders_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Ping
        HttpResponse<String> ping = http("GET", "/", null);
        assertThat(ping.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(ping.body()).get("compatibility").asText())
                .isEqualTo("BACKWARD");

        // POST /subjects/{s}/versions — first call
        String postBody = "{\"schemaType\":\"AVRO\",\"schema\":" + escape(SCHEMA_ORDER) + "}";
        HttpResponse<String> firstRegister =
                http("POST", "/subjects/" + subject + "/versions", postBody);
        assertThat(firstRegister.statusCode()).isEqualTo(200);
        int firstId = MAPPER.readTree(firstRegister.body()).get("id").asInt();
        assertThat(firstId).isGreaterThanOrEqualTo(0);

        // POST again with identical body — same id (idempotent).
        HttpResponse<String> secondRegister =
                http("POST", "/subjects/" + subject + "/versions", postBody);
        assertThat(secondRegister.statusCode()).isEqualTo(200);
        assertThat(MAPPER.readTree(secondRegister.body()).get("id").asInt()).isEqualTo(firstId);

        // POST with a new schema text — the catalog appends a new version and mints a fresh
        // Confluent id (deterministic on tableId + version + format). That's the correct
        // Confluent-SR shape: each distinct submission gets its own id.
        String v2Body = "{\"schemaType\":\"AVRO\",\"schema\":" + escape(SCHEMA_ORDER_V2) + "}";
        HttpResponse<String> rebind = http("POST", "/subjects/" + subject + "/versions", v2Body);
        assertThat(rebind.statusCode()).isEqualTo(200);
        int secondId = MAPPER.readTree(rebind.body()).get("id").asInt();
        assertThat(secondId).isNotEqualTo(firstId);

        // GET /subjects contains subject.
        HttpResponse<String> list = http("GET", "/subjects", null);
        assertThat(list.statusCode()).isEqualTo(200);
        JsonNode subjects = MAPPER.readTree(list.body());
        assertThat(subjects.isArray()).isTrue();
        boolean found = false;
        for (JsonNode s : subjects) {
            if (subject.equals(s.asText())) {
                found = true;
                break;
            }
        }
        assertThat(found).as("subjects list should contain %s", subject).isTrue();

        // GET /schemas/ids/{id} — returns the latest schema text (we rebound to v2).
        HttpResponse<String> byId = http("GET", "/schemas/ids/" + secondId, null);
        assertThat(byId.statusCode()).isEqualTo(200);
        JsonNode byIdJson = MAPPER.readTree(byId.body());
        assertThat(byIdJson.get("schemaType").asText()).isEqualTo("AVRO");
        assertThat(byIdJson.get("schema").asText()).isEqualTo(SCHEMA_ORDER_V2);

        // GET /subjects/{s}/versions/latest
        HttpResponse<String> latest =
                http("GET", "/subjects/" + subject + "/versions/latest", null);
        assertThat(latest.statusCode()).isEqualTo(200);
        JsonNode latestJson = MAPPER.readTree(latest.body());
        assertThat(latestJson.get("subject").asText()).isEqualTo(subject);
        assertThat(latestJson.get("id").asInt()).isEqualTo(secondId);
        assertThat(latestJson.get("schema").asText()).isEqualTo(SCHEMA_ORDER_V2);
    }

    @Test
    void missingTopicReturns404() throws Exception {
        String subject = "absent_" + System.nanoTime() + "-value";
        String postBody = "{\"schemaType\":\"AVRO\",\"schema\":" + escape(SCHEMA_ORDER) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", postBody);
        assertThat(resp.statusCode()).isEqualTo(404);
    }

    @Test
    void protobufSchemaTypeNowAccepted() throws Exception {
        // Phase T-MF.5: schemaType=PROTOBUF is accepted alongside AVRO and JSON. An empty
        // proto file (no message) is rejected by the translator, so we send a minimal valid
        // .proto and expect a 200 from the register endpoint.
        String topic = "proto_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        String proto = "syntax = \\\"proto3\\\";\\nmessage M { int32 id = 1; }";
        String postBody = "{\"schemaType\":\"PROTOBUF\",\"schema\":\"" + proto + "\"}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", postBody);
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    void unknownSchemaTypeReturns422() throws Exception {
        String topic = "unknown_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        String postBody = "{\"schemaType\":\"THRIFT\",\"schema\":\"struct X {}\"}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", postBody);
        // FormatRegistry doesn't know THRIFT → service raises UNSUPPORTED → 422.
        assertThat(resp.statusCode()).isBetween(400, 499);
    }

    @Test
    void keySubjectReturns400() throws Exception {
        String subject = "orders-key";
        String postBody = "{\"schemaType\":\"AVRO\",\"schema\":" + escape(SCHEMA_ORDER) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", postBody);
        assertThat(resp.statusCode()).isEqualTo(400);
    }

    @Test
    void unknownSchemaIdReturns404() throws Exception {
        HttpResponse<String> resp = http("GET", "/schemas/ids/999999999", null);
        assertThat(resp.statusCode()).isEqualTo(404);
    }

    private static HttpResponse<String> http(String method, String path, String body)
            throws Exception {
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json");
        if ("POST".equals(method)) {
            req.POST(
                    HttpRequest.BodyPublishers.ofString(
                            body == null ? "" : body, StandardCharsets.UTF_8));
        } else {
            req.GET();
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
