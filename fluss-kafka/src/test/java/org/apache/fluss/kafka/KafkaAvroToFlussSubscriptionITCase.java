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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo: a Kafka producer publishes a SR-framed Avro record, and a native Fluss
 * subscription (via {@link LogScanner}) receives it from the backing {@code kafka.<topic>} table.
 *
 * <p>Flow under test:
 *
 * <ol>
 *   <li>Register an Avro subject ({@code orders-value}) with the SR HTTP endpoint. Get the Kafka SR
 *       schema id back.
 *   <li>Serialise a {@link GenericRecord} via Apache Avro's own {@code BinaryEncoder}, prepend the
 *       Kafka SR wire frame {@code [0x00][4-byte schema id]}, and publish via plain {@code
 *       kafka-clients} producer.
 *   <li>Open a Fluss {@link Connection}, subscribe via {@link LogScanner} to {@code kafka.orders},
 *       and read the row back.
 *   <li>Assert the payload column is the exact Kafka SR frame we sent; decode the body with {@code
 *       GenericDatumReader} and compare every field.
 * </ol>
 *
 * <p>We don't depend on the Community License serializer client — Apache Avro + a hand-constructed
 * Kafka SR frame gives the same wire bytes.
 */
class KafkaAvroToFlussSubscriptionITCase {

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
                    .setClusterConf(kafkaClusterConf())
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
    void avroProducerIsReceivedByFlussSubscription() throws Exception {
        String topic = "orders_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // 1. Register the Avro schema with our SR. Get back the deterministic Kafka SR schema id.
        int schemaId = registerAvroSchema(subject, SCHEMA_ORDER_JSON);
        assertThat(schemaId).isGreaterThan(0);

        // 2. Build an Avro record, encode with Apache Avro, wrap in the Kafka SR frame, produce.
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_ORDER_JSON);
        GenericRecord originalRecord = new GenericData.Record(avroSchema);
        originalRecord.put("id", 42L);
        originalRecord.put("qty", 7);
        originalRecord.put("sku", "BOLT-M8");

        byte[] srFramed = kafkaSrFrame(schemaId, encodeAvro(avroSchema, originalRecord));

        byte[] key = "order-42".getBytes(StandardCharsets.UTF_8);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            RecordMetadata md =
                    producer.send(new ProducerRecord<>(topic, 0, key, srFramed))
                            .get(30, TimeUnit.SECONDS);
            assertThat(md.topic()).isEqualTo(topic);
        }

        // 3. Open a Fluss subscription over the backing table and read the row back.
        Configuration clientConf = CLUSTER.getClientConfig();
        TablePath kafkaTable = TablePath.of(KAFKA_DATABASE, topic);
        List<ScanRecord> collected = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(kafkaTable);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);

            long deadline = System.currentTimeMillis() + 30_000L;
            while (collected.isEmpty() && System.currentTimeMillis() < deadline) {
                ScanRecords batch = scanner.poll(Duration.ofMillis(500));
                for (ScanRecord rec : batch) {
                    collected.add(rec);
                }
            }
        }

        // 4. Assert the row shape: KafkaDataTable = (record_key BYTES, payload BYTES, event_time,
        // headers). The payload column carries the exact Kafka SR frame we sent.
        assertThat(collected)
                .as("Fluss subscription should have received the produced Kafka record")
                .hasSize(1);
        InternalRow row = collected.get(0).getRow();
        assertThat(row.getBytes(0)).as("record_key column").isEqualTo(key);
        byte[] receivedPayload = row.getBytes(1);
        assertThat(receivedPayload)
                .as("payload column must be the SR-framed Avro bytes byte-for-byte")
                .isEqualTo(srFramed);

        // 5. Decode the Avro body with the schema and verify semantic equality.
        assertThat(receivedPayload[0]).as("Kafka SR magic byte").isEqualTo((byte) 0x00);
        int decodedSchemaId = ByteBuffer.wrap(receivedPayload, 1, 4).getInt();
        assertThat(decodedSchemaId).as("Kafka SR schema id prefix").isEqualTo(schemaId);

        byte[] avroBody = new byte[receivedPayload.length - 5];
        System.arraycopy(receivedPayload, 5, avroBody, 0, avroBody.length);
        GenericRecord decoded = decodeAvro(avroSchema, avroBody);
        assertThat(decoded.get("id")).isEqualTo(42L);
        assertThat(decoded.get("qty")).isEqualTo(7);
        assertThat(decoded.get("sku").toString()).isEqualTo("BOLT-M8");
    }

    // ----------------------------------------------------------------------
    //  Helpers
    // ----------------------------------------------------------------------

    private static int registerAvroSchema(String subject, String schemaJson) throws Exception {
        String body = "{\"schemaType\":\"AVRO\",\"schema\":" + quote(schemaJson) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", body);
        assertThat(resp.statusCode())
                .as("SR register should return 200: " + resp.body())
                .isEqualTo(200);
        String response = resp.body();
        int idPos = response.indexOf("\"id\":");
        int end = response.indexOf(response.charAt(idPos + 5) == ' ' ? "," : ",", idPos);
        // Cheap parse: the body is {"id":NNN} (numeric).
        String idStr = response.replaceAll(".*\"id\"\\s*:\\s*(\\d+).*", "$1");
        return Integer.parseInt(idStr.trim());
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

    /** Serialise a JSON string as a quoted Java string literal. */
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

    private static GenericRecord decodeAvro(Schema schema, byte[] avroBody) {
        try {
            org.apache.avro.io.BinaryDecoder decoder =
                    DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroBody), null);
            return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the Kafka SR wire format: {@code [0x00][big-endian int32 schema id][avro body]}. This
     * is exactly what {@code io.confluent.kafka.serializers.KafkaAvroSerializer} produces.
     */
    private static byte[] kafkaSrFrame(int schemaId, byte[] avroBody) {
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
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
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
