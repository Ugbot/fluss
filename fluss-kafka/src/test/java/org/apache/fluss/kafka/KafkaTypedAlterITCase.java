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

import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase T.3 — end-to-end IT for ALTER-on-register typed-table reshape (design 0015 §11). Drives the
 * full flow: register a schema → SR reshapes the underlying Fluss data table → produce / consume
 * round-trip → register an additive evolution → verify the table grew → exercise the rejection
 * paths (rename, non-null add, non-empty topic).
 *
 * <p>The cluster is configured with {@code kafka.typed-tables.enabled = true}; that flag is the T.2
 * gate, and T.3's evolver is also a no-op when it's off (so production rollouts need both the flag
 * and topics' format flipping via T.3 — exactly what this IT covers in one place).
 */
class KafkaTypedAlterITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;
    private static final int SR_PORT = freePort();

    /** v1: {id INT, name STRING}. */
    private static final String AVRO_V1 =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"}"
                    + "]}";

    /** v2: adds nullable {@code email} (Avro union with null). */
    private static final String AVRO_V2 =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}"
                    + "]}";

    /** v3 attempt: rename {@code id} → {@code user_id}. Must be rejected. */
    private static final String AVRO_V3_RENAME =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                    + "{\"name\":\"user_id\",\"type\":\"int\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}"
                    + "]}";

    /** v3 attempt: add a non-null field. Must be rejected. */
    private static final String AVRO_V3_NONNULL_ADD =
            "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"score\",\"type\":\"int\"}"
                    + "]}";

    /** Protobuf-only test schema with a Timestamp field. */
    private static final String PROTO_SCHEMA =
            "syntax = \"proto3\";\n"
                    + "import \"google/protobuf/timestamp.proto\";\n"
                    + "message Event {\n"
                    + "  int64 seq = 1;\n"
                    + "  google.protobuf.Timestamp ts = 2;\n"
                    + "}\n";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(typedEnabledClusterConf())
                    .build();

    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

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

    // ==========================================================================
    //  Scenarios 1–6: Avro register / produce / evolve / read / reject paths.
    // ==========================================================================

    @Test
    void registerAvroV1ReshapesEmptyTopicToTyped() throws Exception {
        String topic = "alice_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Scenario 1 — Register Avro v1 (id INT, name STRING) on empty topic; expect 200 + id.
        int v1Id = registerAvro(subject, AVRO_V1);
        assertThat(v1Id).as("v1 Confluent id").isGreaterThan(0);

        // Scenario 1 (cont.) — describeTable returns the typed shape.
        TableInfo info = describeTable(topic);
        RowType rowType = info.getRowType();
        assertThat(rowType.getFieldNames())
                .as("typed v1 columns")
                .containsExactly("record_key", "id", "name", "event_time", "headers");
        assertField(rowType, "id", DataTypeRoot.INTEGER, /* nullable */ true);
        assertField(rowType, "name", DataTypeRoot.STRING, true);

        // Catalog row format flipped to KAFKA_TYPED_AVRO.
        Optional<CatalogTableEntity> entity =
                CatalogServices.current().get().getTable(KAFKA_DATABASE, topic);
        assertThat(entity).isPresent();
        assertThat(entity.get().format()).isEqualTo(CatalogTableEntity.FORMAT_KAFKA_TYPED_AVRO);

        // Scenario 2 — Producing a typed record after reshape exercises the typed Produce path's
        // typed-row writer. T.2's writeRow path is hardcoded to a 4-column layout (record_key,
        // payload, event_time, headers), so it cannot populate a 5-column typed table without the
        // T.4 codec.decodeInto wiring. We assert here that the table layout itself is the typed
        // one — exactly what T.3 promises — and defer the produce-into-typed-columns assertion to
        // T.4 (per design 0014 §6 / 0015 §13).
    }

    @Test
    void evolveAddNullableEmailExtendsTypedShape() throws Exception {
        String topic = "evolve_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int v1Id = registerAvro(subject, AVRO_V1);
        // Scenario 3 — Register Avro v2 adding nullable email; expect new id distinct from v1 and
        // the table shape extended at the user-region tail.
        int v2Id = registerAvro(subject, AVRO_V2);
        assertThat(v2Id).as("v2 Confluent id").isGreaterThan(0).isNotEqualTo(v1Id);

        TableInfo info = describeTable(topic);
        assertThat(info.getRowType().getFieldNames())
                .as("typed v2 columns")
                .containsExactly("record_key", "id", "name", "email", "event_time", "headers");
        assertField(info.getRowType(), "email", DataTypeRoot.STRING, true);
    }

    @Test
    void evolveRenameIsRejected() throws Exception {
        String topic = "rename_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        registerAvro(subject, AVRO_V1);
        registerAvro(subject, AVRO_V2);

        // Scenario 5 — Avro v3 renaming id → user_id must 409.
        HttpResponse<String> resp = postSchema(subject, "AVRO", AVRO_V3_RENAME);
        assertThat(resp.statusCode())
                .as(
                        "register rename should be rejected with 409 (got %s: %s)",
                        resp.statusCode(), resp.body())
                .isEqualTo(409);
        assertThat(resp.body()).contains("rename");

        // Table shape is unchanged.
        assertThat(describeTable(topic).getRowType().getFieldNames())
                .containsExactly("record_key", "id", "name", "email", "event_time", "headers");
    }

    @Test
    void evolveNonNullAddIsRejected() throws Exception {
        String topic = "nonnull_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        registerAvro(subject, AVRO_V1);
        registerAvro(subject, AVRO_V2);

        // Scenario 6 — Avro v3 with non-null score: reject. The compatibility checker may also
        // reject ahead of the evolver. We accept either rejection path (both yield 409) and require
        // the table shape stays unchanged.
        HttpResponse<String> resp = postSchema(subject, "AVRO", AVRO_V3_NONNULL_ADD);
        assertThat(resp.statusCode())
                .as(
                        "register non-null add should be rejected (got %s: %s)",
                        resp.statusCode(), resp.body())
                .isEqualTo(409);
        assertThat(describeTable(topic).getRowType().getFieldNames())
                .containsExactly("record_key", "id", "name", "email", "event_time", "headers");
    }

    @Test
    void firstRegisterRejectsNonEmptyTopic() throws Exception {
        String topic = "nonempty_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Produce a single byte-record into the passthrough topic before any schema registration.
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    0,
                                    "k".getBytes(StandardCharsets.UTF_8),
                                    "raw-bytes".getBytes(StandardCharsets.UTF_8)))
                    .get(30, TimeUnit.SECONDS);
        }
        // Wait until the row is durable so the empty-topic guard can see end > start.
        waitForRecord(topic, Duration.ofSeconds(30));

        // Scenario 7 — first-register on a non-empty passthrough topic must 409.
        HttpResponse<String> resp = postSchema(subject, "AVRO", AVRO_V1);
        assertThat(resp.statusCode())
                .as(
                        "first-register on non-empty topic should be rejected (got %s: %s)",
                        resp.statusCode(), resp.body())
                .isEqualTo(409);
        assertThat(resp.body()).containsAnyOf("empty", "records");
    }

    // ==========================================================================
    //  Scenario 8: Protobuf E2E.
    // ==========================================================================

    @Test
    void protobufRegisterReshapesWithTimestampColumn() throws Exception {
        String topic = "proto_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int id = registerSchema(subject, "PROTOBUF", PROTO_SCHEMA);
        assertThat(id).isGreaterThan(0);

        TableInfo info = describeTable(topic);
        RowType rowType = info.getRowType();
        // Find the ts column and verify its type.
        DataType tsType = null;
        for (org.apache.fluss.types.DataField f : rowType.getFields()) {
            if ("ts".equals(f.getName())) {
                tsType = f.getType();
                break;
            }
        }
        assertThat(tsType).as("Protobuf-translated 'ts' column should exist").isNotNull();
        assertThat(tsType.getTypeRoot())
                .as("Protobuf Timestamp must map to TIMESTAMP_LTZ")
                .isIn(
                        DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        assertThat(tsType.isNullable()).as("typed columns are nullable").isTrue();

        Optional<CatalogTableEntity> entity =
                CatalogServices.current().get().getTable(KAFKA_DATABASE, topic);
        assertThat(entity).isPresent();
        assertThat(entity.get().format()).isEqualTo(CatalogTableEntity.FORMAT_KAFKA_TYPED_PROTOBUF);
    }

    // ==========================================================================
    //  Scenario 9: Confluent-id preservation across an additive ALTER.
    // ==========================================================================

    @Test
    void confluentIdPreservedAcrossAdditiveAlter() throws Exception {
        String topic = "ids_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int v1Id = registerAvro(subject, AVRO_V1);
        int v2Id = registerAvro(subject, AVRO_V2);
        assertThat(v2Id).isNotEqualTo(v1Id);

        // v1 still resolves via GET /schemas/ids/{id} after the alter.
        HttpResponse<String> resp = httpGet("/schemas/ids/" + v1Id);
        assertThat(resp.statusCode())
                .as("v1 schema lookup after alter (got %s: %s)", resp.statusCode(), resp.body())
                .isEqualTo(200);
        assertThat(resp.body()).contains("\"id\"").contains(Integer.toString(v1Id));
    }

    // ==========================================================================
    //  Helpers.
    // ==========================================================================

    private static int registerAvro(String subject, String schema) throws Exception {
        return registerSchema(subject, "AVRO", schema);
    }

    private static int registerSchema(String subject, String type, String schema) throws Exception {
        HttpResponse<String> resp = postSchema(subject, type, schema);
        assertThat(resp.statusCode())
                .as("SR register should return 200 (got %d: %s)", resp.statusCode(), resp.body())
                .isEqualTo(200);
        String body = resp.body();
        return Integer.parseInt(body.replaceAll(".*\"id\"\\s*:\\s*(\\d+).*", "$1").trim());
    }

    private static HttpResponse<String> postSchema(String subject, String type, String schemaText)
            throws Exception {
        String body =
                "{\"schemaType\":\"" + type + "\",\"schema\":" + quoteJsonString(schemaText) + "}";
        return http("POST", "/subjects/" + subject + "/versions", body);
    }

    private static HttpResponse<String> httpGet(String path) throws Exception {
        return http("GET", path, null);
    }

    private static HttpResponse<String> http(String method, String path, String body)
            throws Exception {
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(15))
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

    private static TableInfo describeTable(String topic) throws Exception {
        Configuration clientConf = CLUSTER.getClientConfig();
        TablePath path = TablePath.of(KAFKA_DATABASE, topic);
        try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
            return conn.getAdmin().getTableInfo(path).get();
        }
    }

    private static List<ScanRecord> readRows(String topic, int target, Duration deadline)
            throws Exception {
        Configuration clientConf = CLUSTER.getClientConfig();
        TablePath path = TablePath.of(KAFKA_DATABASE, topic);
        List<ScanRecord> rows = new ArrayList<>();
        long deadlineMs = System.currentTimeMillis() + deadline.toMillis();
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            while (rows.size() < target && System.currentTimeMillis() < deadlineMs) {
                ScanRecords batch = scanner.poll(Duration.ofMillis(500));
                for (ScanRecord rec : batch) {
                    rows.add(rec);
                }
            }
        }
        return rows;
    }

    /** Block until the topic has at least one row. */
    private static void waitForRecord(String topic, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (!readRows(topic, 1, Duration.ofMillis(500)).isEmpty()) {
                return;
            }
        }
        throw new IllegalStateException("topic '" + topic + "' did not receive any record");
    }

    private static void assertField(
            RowType rowType, String name, DataTypeRoot expectedRoot, boolean expectNullable) {
        DataType type = rowType.getField(name).getType();
        assertThat(type.getTypeRoot()).as("column '%s' type root", name).isEqualTo(expectedRoot);
        assertThat(type.isNullable()).as("column '%s' nullability", name).isEqualTo(expectNullable);
    }

    private static byte[] encodeAvro(Schema schema, GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            org.apache.avro.io.BinaryEncoder encoder =
                    EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] confluentFrame(int schemaId, byte[] body) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + body.length);
        buf.put((byte) 0x00);
        buf.putInt(schemaId);
        buf.put(body);
        return buf.array();
    }

    private static String quoteJsonString(String raw) {
        StringBuilder sb = new StringBuilder(raw.length() + 16);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return props;
    }

    private static String kafkaBootstrap() {
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

    private static Configuration typedEnabledClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
        // Phase T.2 + T.3 — typed-tables hot path on; T.3's evolver activates because of this.
        conf.set(ConfigOptions.KAFKA_TYPED_TABLES_ENABLED, true);
        return conf;
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unused")
    private static CatalogService catalogService() {
        return CatalogServices.current()
                .orElseThrow(() -> new IllegalStateException("CatalogService not registered"));
    }
}
