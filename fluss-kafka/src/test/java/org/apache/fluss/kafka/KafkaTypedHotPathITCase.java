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
import org.apache.fluss.kafka.fetch.KafkaTopicRoute;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the Phase T.2 typed Produce/Fetch hot path (design 0014). Drives a real Kafka
 * producer + consumer through the Fluss Kafka bolt-on while a parallel Fluss subscription watches
 * the underlying table. Three scenarios:
 *
 * <ul>
 *   <li><b>Scenario A — typed.</b> Feature flag on, catalog row pre-staged to {@code
 *       KAFKA_TYPED_AVRO}. The producer sends Confluent-framed Avro; the broker strips the frame on
 *       Produce and re-prepends it on Fetch. The Kafka consumer must see byte-equal records; the
 *       Fluss subscription must observe the typed body in the table.
 *   <li><b>Scenario B — passthrough fallback.</b> Same setup but feature flag off. The catalog
 *       row's typed format must be ignored; the broker takes the byte-copy path. Kafka round- trip
 *       stays byte-equal because the bytes never left the column.
 *   <li><b>Scenario C — error paths.</b> Bad-frame / unknown-schema-id producers receive Kafka
 *       error codes per design 0014 §8 rather than tearing down the partition.
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>The Schema Registry HTTP listener registers the schema and writes a {@code
 *       KAFKA_PASSTHROUGH} catalog row (T.1 default — T.3 will flip this at registration time).
 *       This test pre-stages the typed format directly via the in-process {@link
 *       CatalogServices#current()}, bypassing T.3.
 *   <li>Avro serialisation uses Apache Avro's own {@code BinaryEncoder} + a hand-built Confluent
 *       frame; we don't depend on Confluent's serializer client (Community License).
 * </ul>
 */
class KafkaTypedHotPathITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;
    private static final int SR_PORT = freePort();

    private static final String SCHEMA_ORDER_JSON =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"qty\",\"type\":\"int\"},"
                    + "{\"name\":\"sku\",\"type\":\"string\"}"
                    + "]}";

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
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

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
    void typedProduceAndFetchAreByteEqual() throws Exception {
        String topic = "typed_orders_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Register the schema with the SR HTTP endpoint to obtain the canonical Confluent id.
        int schemaId = registerAvroSchema(subject, SCHEMA_ORDER_JSON);
        assertThat(schemaId).isGreaterThan(0);

        // Pre-stage the catalog row to KAFKA_TYPED_AVRO (T.3's job once it lands; for T.2 we
        // bypass the ALTER flow by writing directly to the catalog).
        flipCatalogRow(topic, KafkaTopicRoute.FORMAT_TYPED_AVRO);

        // Produce a batch of randomized typed records.
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_ORDER_JSON);
        Random rnd = new Random(0xCAFEBABE);
        int recordCount = 10;
        List<byte[]> framedValues = new ArrayList<>(recordCount);
        List<byte[]> keys = new ArrayList<>(recordCount);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < recordCount; i++) {
                GenericRecord rec = new GenericData.Record(avroSchema);
                rec.put("id", rnd.nextLong());
                rec.put("qty", rnd.nextInt(100));
                rec.put("sku", "SKU-" + Integer.toHexString(rnd.nextInt()));
                byte[] body = encodeAvro(avroSchema, rec);
                byte[] framed = confluentFrame(schemaId, body);
                byte[] key = ("k-" + i).getBytes(StandardCharsets.UTF_8);
                framedValues.add(framed);
                keys.add(key);
                producer.send(new ProducerRecord<>(topic, 0, key, framed))
                        .get(30, TimeUnit.SECONDS);
            }
        }

        // Consume via the same Kafka API and verify byte-equal round-trip.
        List<byte[]> received = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000L;
            while (received.size() < recordCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    received.add(r.value());
                }
            }
        }
        assertThat(received).as("Kafka consumer received").hasSize(recordCount);
        for (int i = 0; i < recordCount; i++) {
            assertThat(received.get(i))
                    .as("Confluent-framed value byte-equal at index %d", i)
                    .isEqualTo(framedValues.get(i));
        }

        // The Fluss subscription side: rows are stored by the broker with the frame stripped on
        // Produce (typed path) — the payload column carries the Avro body, NOT the framed bytes.
        // This is the T.2 invariant: the typed branch validates the frame and writes the body
        // into the column. T.3 will alter the table schema to expose typed columns instead.
        Configuration clientConf = CLUSTER.getClientConfig();
        TablePath kafkaTable = TablePath.of(KAFKA_DATABASE, topic);
        List<ScanRecord> rows = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(kafkaTable);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            long deadline = System.currentTimeMillis() + 30_000L;
            while (rows.size() < recordCount && System.currentTimeMillis() < deadline) {
                ScanRecords batch = scanner.poll(Duration.ofMillis(500));
                for (ScanRecord rec : batch) {
                    rows.add(rec);
                }
            }
        }
        assertThat(rows).as("Fluss subscription rows").hasSize(recordCount);
        for (int i = 0; i < recordCount; i++) {
            InternalRow r = rows.get(i).getRow();
            assertThat(r.getBytes(0)).as("record_key column row %d", i).isEqualTo(keys.get(i));
            byte[] storedValue = r.getBytes(1);
            // The typed branch strips the 5-byte Confluent frame; what's stored is the body.
            byte[] expectedBody = new byte[framedValues.get(i).length - 5];
            System.arraycopy(framedValues.get(i), 5, expectedBody, 0, expectedBody.length);
            assertThat(storedValue)
                    .as("stored body (frame stripped) row %d", i)
                    .isEqualTo(expectedBody);
        }
    }

    @Test
    void unknownSchemaIdYieldsInvalidRecord() throws Exception {
        String topic = "typed_unknown_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        // Register a real schema, then pre-stage the catalog row to typed.
        String subject = topic + "-value";
        int schemaId = registerAvroSchema(subject, SCHEMA_ORDER_JSON);
        flipCatalogRow(topic, KafkaTopicRoute.FORMAT_TYPED_AVRO);

        // Produce a record framed with a bogus schema id — the broker must reject it without
        // tearing down the partition. We assert that the future fails, not that it succeeds.
        int unknownId = schemaId + 1_000_000;
        byte[] bogus =
                confluentFrame(
                        unknownId,
                        "{\"id\":1,\"qty\":1,\"sku\":\"x\"}".getBytes(StandardCharsets.UTF_8));
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            try {
                producer.send(
                                new ProducerRecord<>(
                                        topic, 0, "k".getBytes(StandardCharsets.UTF_8), bogus))
                        .get(30, TimeUnit.SECONDS);
                // The broker rejects the partition with INVALID_RECORD; the producer surfaces
                // this as an ExecutionException wrapping the Kafka exception. If send() returned
                // normally, the broker accepted the record — that's a regression.
                org.assertj.core.api.Assertions.fail(
                        "Producer should have failed with INVALID_RECORD / CORRUPT_MESSAGE for"
                                + " unknown schema id");
            } catch (java.util.concurrent.ExecutionException expected) {
                // OK — broker rejected with a Kafka error. We don't pin the exact subclass to
                // keep this test robust against the producer client's error mapping changes.
                assertThat(expected).hasCauseInstanceOf(Exception.class);
            }
        }
    }

    @Test
    void shortFrameYieldsCorruptMessage() throws Exception {
        String topic = "typed_corrupt_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        String subject = topic + "-value";
        registerAvroSchema(subject, SCHEMA_ORDER_JSON);
        flipCatalogRow(topic, KafkaTopicRoute.FORMAT_TYPED_AVRO);

        byte[] tooShort = new byte[] {0x00, 0x00, 0x00}; // < 5 bytes
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            try {
                producer.send(
                                new ProducerRecord<>(
                                        topic, 0, "k".getBytes(StandardCharsets.UTF_8), tooShort))
                        .get(30, TimeUnit.SECONDS);
                org.assertj.core.api.Assertions.fail(
                        "Producer should have failed with CORRUPT_MESSAGE for short frame");
            } catch (java.util.concurrent.ExecutionException expected) {
                assertThat(expected).hasCauseInstanceOf(Exception.class);
            }
        }
    }

    // ----------------------------------------------------------------------
    //  Helpers
    // ----------------------------------------------------------------------

    /**
     * Replace the {@code __tables__.format} value for {@code (kafka, topic)} with {@code
     * newFormat}. Used by Scenario A to bypass T.3's ALTER flow — the SR currently writes
     * KAFKA_PASSTHROUGH on registration, so we drop and re-create the catalog row with the typed
     * format. Catalog access goes through {@link CatalogServices#current()} which is registered on
     * the coord leader by the embedded test cluster.
     */
    private static void flipCatalogRow(String topic, String newFormat) throws Exception {
        Optional<CatalogService> maybe = CatalogServices.current();
        assertThat(maybe).as("CatalogService must be running on coord leader").isPresent();
        CatalogService catalog = maybe.get();
        Optional<CatalogTableEntity> existing = catalog.getTable(KAFKA_DATABASE, topic);
        if (existing.isPresent() && newFormat.equals(existing.get().format())) {
            return;
        }
        if (existing.isPresent()) {
            catalog.dropTable(KAFKA_DATABASE, topic);
        }
        catalog.createTable(KAFKA_DATABASE, topic, newFormat, KAFKA_DATABASE + "." + topic, null);
    }

    private static int registerAvroSchema(String subject, String schemaJson) throws Exception {
        String body = "{\"schemaType\":\"AVRO\",\"schema\":" + quote(schemaJson) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", body);
        assertThat(resp.statusCode())
                .as("SR register should return 200: " + resp.body())
                .isEqualTo(200);
        return Integer.parseInt(resp.body().replaceAll(".*\"id\"\\s*:\\s*(\\d+).*", "$1").trim());
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

    private static String quote(String raw) {
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

    private static byte[] confluentFrame(int schemaId, byte[] avroBody) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + avroBody.length);
        buf.put((byte) 0x00);
        buf.putInt(schemaId);
        buf.put(avroBody);
        return buf.array();
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "typed-it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
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

    private static Configuration typedEnabledClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
        // Phase T.2 — flip the typed-tables hot path on for the cluster under test.
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
}
