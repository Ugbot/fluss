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
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo in the reverse direction: a native Fluss writer appends a row directly to the
 * {@code kafka.<topic>} backing table, and a {@link KafkaConsumer} reads the Kafka SR-framed Avro
 * bytes back through the Kafka bolt-on's Fetch path.
 *
 * <p>Flow under test:
 *
 * <ol>
 *   <li>Create the topic via {@code kafka.AdminClient.createTopics}; this creates the backing Fluss
 *       table {@code kafka.<topic>} with the right catalog markers so the broker recognises it.
 *   <li>Register an Avro subject with the SR HTTP endpoint; capture the Kafka SR schema id.
 *   <li>Use a native Fluss {@link AppendWriter} to write a row whose {@code payload} column is the
 *       Kafka SR-framed Avro bytes {@code [0x00][schema id][avro body]}. The {@code record_key}
 *       column carries the Kafka message key; {@code event_time} is a {@link TimestampLtz}; {@code
 *       headers} carries a single header.
 *   <li>Open a plain {@link KafkaConsumer} pointed at the same Kafka listener and consume from
 *       offset 0.
 *   <li>Assert key, value (byte-for-byte == Kafka SR frame), timestamp, and one header all
 *       round-trip; decode the Avro body and compare every field.
 * </ol>
 */
class FlussWriteToKafkaConsumerITCase {

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
    void flussWriteIsConsumedByKafkaConsumer() throws Exception {
        String topic = "orders_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        int schemaId = registerAvroSchema(subject, SCHEMA_ORDER_JSON);
        assertThat(schemaId).isGreaterThan(0);

        // 1. Build a Kafka SR-framed Avro payload.
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_ORDER_JSON);
        GenericRecord originalRecord = new GenericData.Record(avroSchema);
        originalRecord.put("id", 99L);
        originalRecord.put("qty", 3);
        originalRecord.put("sku", "NUT-M8");
        byte[] srFramed = kafkaSrFrame(schemaId, encodeAvro(avroSchema, originalRecord));
        byte[] keyBytes = "order-99".getBytes(StandardCharsets.UTF_8);

        long recordTimestamp = 1_700_000_000_000L;
        byte[] headerValue = "svc-a".getBytes(StandardCharsets.UTF_8);

        // 2. Write directly via Fluss's official client SDK into the backing kafka.<topic> table.
        // The table's schema matches KafkaDataTable.schema() (record_key BYTES, payload BYTES,
        // event_time TIMESTAMP_LTZ(3) NOT NULL, headers ARRAY<ROW<name STRING, value BYTES>>).
        // We build each column's value with Fluss's public row-builder helpers; no reaching into
        // internal row representations.
        Configuration clientConf = CLUSTER.getClientConfig();
        TablePath kafkaTable = TablePath.of(KAFKA_DATABASE, topic);
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
                Table table = conn.getTable(kafkaTable)) {
            AppendWriter writer = table.newAppend().createWriter();

            // ARRAY<ROW<name STRING, value BYTES>>. Each element is itself a row; wrap in a
            // GenericArray (the SDK's InternalArray implementation) so the writer can walk it.
            GenericArray headers =
                    new GenericArray(
                            new Object[] {
                                GenericRow.of(BinaryString.fromString("source"), headerValue)
                            });

            // Four-column row: matches KafkaDataTable.schema() column-for-column.
            GenericRow row =
                    GenericRow.of(
                            keyBytes,
                            srFramed,
                            TimestampLtz.fromEpochMillis(recordTimestamp),
                            headers);

            writer.append(row).get();
            writer.flush();
        }

        // 3. Consume via a plain KafkaConsumer — reads through the Kafka bolt-on's Fetch path.
        List<ConsumerRecord<byte[], byte[]>> consumed = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000L;
            while (consumed.isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    consumed.add(r);
                }
            }
        }
        assertThat(consumed)
                .as("Kafka consumer should see the row Fluss wrote natively")
                .hasSize(1);

        // 4. Assert the consumed Kafka record round-trips our bytes intact.
        ConsumerRecord<byte[], byte[]> rec = consumed.get(0);
        assertThat(rec.topic()).isEqualTo(topic);
        assertThat(rec.partition()).isEqualTo(0);
        assertThat(rec.key()).as("Kafka record key").isEqualTo(keyBytes);
        assertThat(rec.value())
                .as("Kafka record value == Kafka SR-framed Avro bytes we wrote")
                .isEqualTo(srFramed);

        // The Kafka broker may override record timestamps; assert it's at least within a minute
        // of our write (the event_time column we set, projected by KafkaFetchTranscoder).
        assertThat(rec.timestamp())
                .as("record timestamp from fluss event_time column")
                .isBetween(recordTimestamp - 60_000L, recordTimestamp + 60_000L);

        List<Header> headerList = new ArrayList<>();
        rec.headers().forEach(headerList::add);
        assertThat(headerList).as("headers array survives round-trip").hasSize(1);
        Header header = headerList.get(0);
        assertThat(header.key()).isEqualTo("source");
        assertThat(header.value()).isEqualTo(headerValue);

        // 5. Kafka SR framing + Avro semantic decode.
        assertThat(rec.value()[0]).as("Kafka SR magic byte").isEqualTo((byte) 0x00);
        int decodedSchemaId = ByteBuffer.wrap(rec.value(), 1, 4).getInt();
        assertThat(decodedSchemaId).as("embedded schema id").isEqualTo(schemaId);

        byte[] avroBody = new byte[rec.value().length - 5];
        System.arraycopy(rec.value(), 5, avroBody, 0, avroBody.length);
        GenericRecord decoded = decodeAvro(avroSchema, avroBody);
        assertThat(decoded.get("id")).isEqualTo(99L);
        assertThat(decoded.get("qty")).isEqualTo(3);
        assertThat(decoded.get("sku").toString()).isEqualTo("NUT-M8");
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

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fluss-to-kafka-it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
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
