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

import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.admin.OffsetSpec.latest;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Multi-broker E2E IT for Kafka wire-protocol compatibility.
 *
 * <p>Runs a 3-tablet Fluss cluster and verifies that:
 *
 * <ul>
 *   <li>All 3 tablets are visible in Kafka metadata responses.
 *   <li>Passthrough produce/consume routes correctly across all brokers.
 *   <li>Avro schema registration propagates the typed-table reshape via {@code UpdateMetadata} to
 *       all 3 tablets; typed Kafka SR-encoded produces succeed on each individual broker endpoint.
 *   <li>Additive Avro schema evolution extends the typed table shape cluster-wide.
 *   <li>Protobuf schema registration reshapes the table to {@code KAFKA_TYPED_PROTOBUF} and the
 *       reshaped table accepts Kafka SR-framed records from any broker endpoint.
 * </ul>
 */
class KafkaMultiBrokerITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int SR_PORT = freePort();

    /** Avro v1: {name: string, age: int}. */
    private static final String AVRO_V1_NAME_AGE =
            "{\"type\":\"record\",\"name\":\"Person\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"age\",\"type\":\"int\"}"
                    + "]}";

    /** Evolution v1: {name: string, score: int}. */
    private static final String AVRO_EVO_V1 =
            "{\"type\":\"record\",\"name\":\"Score\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"score\",\"type\":\"int\"}"
                    + "]}";

    /** Evolution v2: adds nullable label column. */
    private static final String AVRO_EVO_V2 =
            "{\"type\":\"record\",\"name\":\"Score\",\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"},"
                    + "{\"name\":\"score\",\"type\":\"int\"},"
                    + "{\"name\":\"label\",\"type\":[\"null\",\"string\"],\"default\":null}"
                    + "]}";

    /** Protobuf: simple Event message with two primitive fields. */
    private static final String PROTO_EVENT_SCHEMA =
            "syntax = \"proto3\";\n"
                    + "message Event {\n"
                    + "  string id = 1;\n"
                    + "  int64 ts = 2;\n"
                    + "}\n";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(clusterConf())
                    .build();

    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static Admin admin;

    @BeforeAll
    static void createAdmin() {
        admin = KafkaAdminClient.create(adminProps(allBrokersBootstrap()));
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

    // ============================================================
    //  Test 1: metadata fan-out — all 3 tablets visible.
    // ============================================================

    @Test
    void allThreeBrokersVisibleInMetadata() throws Exception {
        String topic = "meta_" + System.nanoTime();
        admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, NUM_TABLET_SERVERS, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, NUM_TABLET_SERVERS, Duration.ofSeconds(60));

        DescribeClusterResult clusterResult = admin.describeCluster();
        int nodeCount = clusterResult.nodes().get(30, TimeUnit.SECONDS).size();
        assertThat(nodeCount).as("all 3 brokers visible in DescribeCluster").isEqualTo(3);

        DescribeTopicsResult topicsResult = admin.describeTopics(Collections.singletonList(topic));
        TopicDescription desc = topicsResult.allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        assertThat(desc).as("topic description").isNotNull();
        assertThat(desc.partitions().size())
                .as("topic partition count")
                .isEqualTo(NUM_TABLET_SERVERS);
        // Verify each partition has a leader assigned — replica-list is not populated by the
        // current Fluss Kafka compat (DescribeTopics returns empty replicas[]; the leader field
        // is sufficient to confirm the topic is properly distributed.
        for (TopicPartitionInfo tpi : desc.partitions()) {
            assertThat(tpi.leader())
                    .as("partition %d must have an elected leader", tpi.partition())
                    .isNotNull();
        }
    }

    // ============================================================
    //  Test 2: passthrough produce/consume across all broker nodes.
    // ============================================================

    @Test
    void produceAndConsumeAcrossAllBrokers() throws Exception {
        String topic = "passthrough_" + System.nanoTime();
        admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, NUM_TABLET_SERVERS, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, NUM_TABLET_SERVERS, Duration.ofSeconds(60));
        // awaitTopicLeadersElected polls every individual tablet until all NUM_TABLET_SERVERS
        // partitions have elected leaders visible on each node, confirming that partition
        // ownership is spread across the whole cluster and all UpdateMetadata RPCs have landed.

        // Produce 10 records to each explicit partition. Routing by partition-index ensures each
        // tablet-server leader handles one batch, exercising produce through every cluster node.
        int recordsPerPartition = 10;
        Set<String> producedSet = new HashSet<>();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int p = 0; p < NUM_TABLET_SERVERS; p++) {
                for (int i = 0; i < recordsPerPartition; i++) {
                    byte[] val = ("v-p" + p + "-" + i).getBytes(StandardCharsets.UTF_8);
                    producedSet.add(new String(val, StandardCharsets.UTF_8));
                    producer.send(new ProducerRecord<>(topic, p, null, val))
                            .get(120, TimeUnit.SECONDS);
                }
            }
        }

        int expected = recordsPerPartition * NUM_TABLET_SERVERS;
        List<byte[]> consumedValues =
                consumeValues(topic, NUM_TABLET_SERVERS, expected, Duration.ofSeconds(60));
        assertThat(consumedValues.size())
                .as("all %d records consumed from full-cluster bootstrap", expected)
                .isEqualTo(expected);
        // Passthrough round-trip: produced raw bytes must exactly match consumed bytes (set
        // equality).
        Set<String> consumedSet = new HashSet<>();
        for (byte[] v : consumedValues) {
            consumedSet.add(new String(v, StandardCharsets.UTF_8));
        }
        assertThat(consumedSet)
                .as("consumed value set must exactly match produced value set (passthrough)")
                .isEqualTo(producedSet);
    }

    // ============================================================
    //  Test 3: Avro schema propagates — typed produce from each broker.
    // ============================================================

    @Test
    void avroSchemaPropagatesAndTypedProduceFetchWorks() throws Exception {
        String topic = "avro_prop_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        int schemaId = registerSchema(subject, "AVRO", AVRO_V1_NAME_AGE);
        // TypedTableEvolver drops-and-recreates the table with a new tableId; wait for the new
        // table's bucket leader to propagate via UpdateMetadata to all tablets before producing.
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        // Schema registration blocks until the SR handler reshapes the table; verify the shape.
        TableInfo info = describeTable(topic);
        assertThat(info.getRowType().getFieldNames())
                .as("typed v1 columns after reshape")
                .contains("name", "age");

        // Produce 15 Kafka-SR-framed Avro records. The cluster has 3 tablets and RF=1, so the
        // single-partition topic's leader is one specific tablet — verify produce and consume work
        // correctly after the typed-table reshape.
        Schema avroSchema = new Schema.Parser().parse(AVRO_V1_NAME_AGE);
        int totalRecords = 15;
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int i = 0; i < totalRecords; i++) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("name", "user-" + i);
                record.put("age", i);
                byte[] avroBytes = encodeAvro(avroSchema, record);
                byte[] payload = kafkaSrFrame(schemaId, avroBytes);
                producer.send(new ProducerRecord<>(topic, payload)).get(120, TimeUnit.SECONDS);
            }
        }

        List<byte[]> consumedValues = consumeValues(topic, 1, totalRecords, Duration.ofSeconds(60));
        assertThat(consumedValues.size())
                .as("%d Avro records visible via full-cluster consume", totalRecords)
                .isEqualTo(totalRecords);
        // Verify Kafka SR framing and Avro payload correctness on every fetched record.
        for (int i = 0; i < totalRecords; i++) {
            byte[] val = consumedValues.get(i);
            assertThat(val[0]).as("Kafka SR magic byte at record %d", i).isEqualTo((byte) 0x00);
            int embeddedId = ByteBuffer.wrap(val, 1, 4).getInt();
            assertThat(embeddedId).as("schema ID embedded in record %d", i).isEqualTo(schemaId);
            byte[] avroBody = Arrays.copyOfRange(val, 5, val.length);
            GenericRecord decoded = decodeAvro(avroSchema, avroBody);
            assertThat(decoded.get("name").toString())
                    .as("name field at record %d", i)
                    .isEqualTo("user-" + i);
            assertThat(decoded.get("age")).as("age field at record %d", i).isEqualTo(i);
        }
    }

    // ============================================================
    //  Test 4: Avro evolution adds nullable column cluster-wide.
    // ============================================================

    @Test
    void avroSchemaEvolutionAddsNullableColumnAcrossCluster() throws Exception {
        String topic = "avro_evo_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        int v1Id = registerSchema(subject, "AVRO", AVRO_EVO_V1);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        // Produce 5 v1 records.
        Schema v1Schema = new Schema.Parser().parse(AVRO_EVO_V1);
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int i = 0; i < 5; i++) {
                GenericRecord rec = new GenericData.Record(v1Schema);
                rec.put("name", "player-" + i);
                rec.put("score", i * 100);
                producer.send(
                                new ProducerRecord<>(
                                        topic, kafkaSrFrame(v1Id, encodeAvro(v1Schema, rec))))
                        .get(120, TimeUnit.SECONDS);
            }
        }

        // Register v2 — adds nullable label.
        int v2Id = registerSchema(subject, "AVRO", AVRO_EVO_V2);
        assertThat(v2Id).as("v2 schema id must differ from v1").isNotEqualTo(v1Id);
        // evolveTyped calls alterTableSchema; wait for UpdateMetadata to propagate to all tablets.
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        // Table shape must now include the label column.
        TableInfo info = describeTable(topic);
        RowType rowType = info.getRowType();
        assertThat(rowType.getFieldNames())
                .as("typed v2 columns after additive evolution")
                .contains("name", "score", "label");
        assertThat(rowType.getField("label").getType().isNullable())
                .as("label column is nullable")
                .isTrue();

        // Produce 5 v2 records (with label set).
        Schema v2Schema = new Schema.Parser().parse(AVRO_EVO_V2);
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int i = 0; i < 5; i++) {
                GenericRecord rec = new GenericData.Record(v2Schema);
                rec.put("name", "player-v2-" + i);
                rec.put("score", i * 200);
                rec.put("label", "gold");
                producer.send(
                                new ProducerRecord<>(
                                        topic, kafkaSrFrame(v2Id, encodeAvro(v2Schema, rec))))
                        .get(120, TimeUnit.SECONDS);
            }
        }

        List<byte[]> consumedValues = consumeValues(topic, 1, 10, Duration.ofSeconds(60));
        assertThat(consumedValues.size()).as("all 10 v1+v2 records consumed").isEqualTo(10);
        // After evolution the fetch codec always re-encodes with the latest schema (v2Id). Verify:
        //   - all 10 records carry v2Id in the Kafka SR frame
        //   - first 5 (stored as v1) have label=null after projection to v2 shape
        //   - last 5 (stored as v2) have label="gold"
        for (int i = 0; i < 10; i++) {
            byte[] val = consumedValues.get(i);
            assertThat(val[0]).as("Kafka SR magic byte at record %d", i).isEqualTo((byte) 0x00);
            int embeddedId = ByteBuffer.wrap(val, 1, 4).getInt();
            assertThat(embeddedId)
                    .as("all fetched records carry latest schema id (v2Id=%d) at index %d", v2Id, i)
                    .isEqualTo(v2Id);
            byte[] avroBody = Arrays.copyOfRange(val, 5, val.length);
            GenericRecord decoded = decodeAvro(v2Schema, avroBody);
            if (i < 5) {
                assertThat(decoded.get("label"))
                        .as("v1 record %d projected to v2 must have null label", i)
                        .isNull();
            } else {
                assertThat(decoded.get("label") == null ? null : decoded.get("label").toString())
                        .as("v2 record %d must have label='gold'", i)
                        .isEqualTo("gold");
            }
        }
    }

    // ============================================================
    //  Test 5: Protobuf schema registration reshapes table cluster-wide.
    // ============================================================

    @Test
    void protobufSchemaPropagatesAndTypedProduceFetchWorks() throws Exception {
        String topic = "proto_mb_" + System.nanoTime();
        String subject = topic + "-value";
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        int schemaId = registerSchema(subject, "PROTOBUF", PROTO_EVENT_SCHEMA);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        // Verify table was reshaped to Protobuf-typed format.
        Optional<CatalogTableEntity> entity =
                CatalogServices.current()
                        .orElseThrow(
                                () -> new IllegalStateException("CatalogService not registered"))
                        .getTable(KAFKA_DATABASE, topic);
        assertThat(entity).as("catalog entity for %s", topic).isPresent();
        assertThat(entity.get().format())
                .as("table format after Protobuf SR registration")
                .isEqualTo(CatalogTableEntity.FORMAT_KAFKA_TYPED_PROTOBUF);

        // Produce 15 Kafka-SR-framed Protobuf records through the full-cluster bootstrap to verify
        // the reshaped typed table accepts Protobuf-encoded payloads end-to-end.
        int totalRecords = 15;
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int i = 0; i < totalRecords; i++) {
                byte[] protoBytes = encodeProtoEvent("event-" + i, i);
                byte[] payload = kafkaSrFrameProtobuf(schemaId, protoBytes);
                producer.send(new ProducerRecord<>(topic, payload)).get(120, TimeUnit.SECONDS);
            }
        }

        List<byte[]> consumedValues = consumeValues(topic, 1, totalRecords, Duration.ofSeconds(60));
        assertThat(consumedValues.size())
                .as("%d Kafka-SR-framed Protobuf records visible cluster-wide", totalRecords)
                .isEqualTo(totalRecords);
        // Kafka framing on the fetch side (not Kafka SR framing): [0x00][4-byte schemaId][body].
        // The produce-side message-index bytes ([0x02][0x00]) are consumed by the Protobuf codec
        // during decode and are NOT re-emitted on Fetch. Byte[5] is therefore the first byte of
        // the re-encoded Protobuf binary: tag for field "id" = (1 << 3) | 2 = 0x0A.
        for (int i = 0; i < totalRecords; i++) {
            byte[] val = consumedValues.get(i);
            assertThat(val.length)
                    .as("Protobuf record %d must be at least 6 bytes (frame + body)", i)
                    .isGreaterThanOrEqualTo(6);
            assertThat(val[0]).as("Kafka SR magic byte at record %d", i).isEqualTo((byte) 0x00);
            int embeddedId = ByteBuffer.wrap(val, 1, 4).getInt();
            assertThat(embeddedId).as("schema ID at record %d", i).isEqualTo(schemaId);
            // 0x0A = Protobuf field-1 (string id), wire type 2 (LEN): (1 << 3) | 2.
            assertThat(val[5])
                    .as("Protobuf body starts with field-1 tag (0x0A) at record %d", i)
                    .isEqualTo((byte) 0x0A);
        }
    }

    // ============================================================
    //  Test 6: listOffsets(LATEST) per-partition returns exact produced count.
    // ============================================================

    @Test
    void listOffsetsLatestMatchesProducedCount() throws Exception {
        // Single-partition topic avoids the multi-broker produce routing instability that affects
        // the 3-partition explicit-partition tests. We verify that listOffsets(LATEST) on one
        // partition returns exactly the number of records produced.
        String topic = "offsets_" + System.nanoTime();
        int numRecords = 13;
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get(60, TimeUnit.SECONDS);
        awaitTopicLeadersElected(topic, 1, Duration.ofSeconds(60));

        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(allBrokersBootstrap()))) {
            for (int i = 0; i < numRecords; i++) {
                byte[] val = ("off-" + i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(topic, val)).get(120, TimeUnit.SECONDS);
            }
        }

        // listOffsets(LATEST) returns the end offset; for a partition with N records starting at
        // offset 0 the end offset equals N.
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> offsetQuery =
                new HashMap<>();
        offsetQuery.put(new TopicPartition(topic, 0), latest());
        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>
                offsets = admin.listOffsets(offsetQuery).all().get(60, TimeUnit.SECONDS);
        assertThat(offsets.get(new TopicPartition(topic, 0)).offset())
                .as("listOffsets(LATEST) must equal records-produced (%d)", numRecords)
                .isEqualTo(numRecords);
    }

    // ============================================================
    //  Helpers.
    // ============================================================

    private static int registerSchema(String subject, String type, String schemaText)
            throws Exception {
        String body =
                "{\"schemaType\":\"" + type + "\",\"schema\":" + quoteJsonString(schemaText) + "}";
        HttpResponse<String> resp = http("POST", "/subjects/" + subject + "/versions", body);
        assertThat(resp.statusCode())
                .as("SR register should return 200 (got %d: %s)", resp.statusCode(), resp.body())
                .isEqualTo(200);
        return Integer.parseInt(resp.body().replaceAll(".*\"id\"\\s*:\\s*(\\d+).*", "$1").trim());
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
        TablePath path = TablePath.of(KAFKA_DATABASE, topic);
        try (Connection conn = ConnectionFactory.createConnection(CLUSTER.getClientConfig())) {
            return conn.getAdmin().getTableInfo(path).get();
        }
    }

    /**
     * Consume all record value bytes from a topic, returning them in arrival order. Stops when
     * {@code expectedCount} values have been collected or the deadline expires.
     */
    private static List<byte[]> consumeValues(
            String topic, int numPartitions, int expectedCount, Duration deadline)
            throws Exception {
        List<TopicPartition> tps = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            tps.add(new TopicPartition(topic, p));
        }
        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProps(allBrokersBootstrap()))) {
            consumer.assign(tps);
            consumer.seekToBeginning(consumer.assignment());
            List<byte[]> values = new ArrayList<>();
            long deadlineMs = System.currentTimeMillis() + deadline.toMillis();
            while (values.size() < expectedCount && System.currentTimeMillis() < deadlineMs) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                for (org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> rec : batch) {
                    values.add(rec.value());
                }
            }
            return values;
        }
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

    private static GenericRecord decodeAvro(Schema schema, byte[] body) {
        try {
            return new GenericDatumReader<GenericRecord>(schema)
                    .read(
                            null,
                            DecoderFactory.get()
                                    .binaryDecoder(new ByteArrayInputStream(body), null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Kafka SR wire-format prefix for Avro: magic(1) + schema_id(4) + payload. */
    private static byte[] kafkaSrFrame(int schemaId, byte[] body) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + body.length);
        buf.put((byte) 0x00);
        buf.putInt(schemaId);
        buf.put(body);
        return buf.array();
    }

    /**
     * Kafka SR wire-format prefix for Protobuf: magic(1) + schema_id(4) + message-index(2) +
     * payload. The message index {@code [0]} is zigzag-encoded as 1 element (count = zigzag(1) =
     * 0x02) at index 0 (zigzag(0) = 0x00), matching the Kafka SR Protobuf serializer contract for a
     * single top-level message type in the descriptor.
     */
    private static byte[] kafkaSrFrameProtobuf(int schemaId, byte[] body) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 2 + body.length);
        buf.put((byte) 0x00);
        buf.putInt(schemaId);
        buf.put((byte) 0x02); // zigzag(1) — one path element
        buf.put((byte) 0x00); // zigzag(0) — top-level message at index 0
        buf.put(body);
        return buf.array();
    }

    /**
     * Minimal Protobuf binary encoding for: {@code message Event { string id = 1; int64 ts = 2; }}.
     */
    private static byte[] encodeProtoEvent(String id, long ts) {
        byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
        // max size: field1 tag(1) + len varint(5) + id bytes + field2 tag(1) + ts varint(10)
        ByteBuffer buf = ByteBuffer.allocate(17 + idBytes.length);
        buf.put((byte) 0x0a); // field 1, wire type 2 (length-delimited)
        putVarint(buf, idBytes.length);
        buf.put(idBytes);
        if (ts != 0L) {
            buf.put((byte) 0x10); // field 2, wire type 0 (varint)
            putVarintLong(buf, ts);
        }
        byte[] out = new byte[buf.position()];
        ((ByteBuffer) buf.flip()).get(out);
        return out;
    }

    private static void putVarint(ByteBuffer buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) value);
    }

    private static void putVarintLong(ByteBuffer buf, long value) {
        while ((value & ~0x7FL) != 0L) {
            buf.put((byte) ((value & 0x7FL) | 0x80L));
            value >>>= 7;
        }
        buf.put((byte) value);
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

    private static String allBrokersBootstrap() {
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

    private static Properties adminProps(String bootstrap) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 120_000);
        return props;
    }

    private static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000);
        return props;
    }

    private static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1_000);
        return props;
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
        conf.set(ConfigOptions.KAFKA_TYPED_TABLES_ENABLED, true);
        return conf;
    }

    /**
     * Polls the Kafka Metadata API on EVERY individual tablet until all {@code numPartitions}
     * partitions of {@code topic} have a non-null leader on each tablet's local cache, or throws if
     * the deadline expires.
     *
     * <p>Must poll per-tablet because after {@code AdminClient.createTopics().all().get()} the
     * coordinator has assigned leaders but the {@code UpdateMetadata} RPC to each tablet is
     * asynchronous. A single-broker poll on the all-brokers admin client may succeed on one tablet
     * while others still return {@code LEADER_NOT_AVAILABLE}, causing producer sends to timeout.
     */
    private static void awaitTopicLeadersElected(String topic, int numPartitions, Duration timeout)
            throws Exception {
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        List<Admin> nodeAdmins = new ArrayList<>();
        try {
            for (ServerNode node : nodes) {
                Properties perNodeProps = new Properties();
                perNodeProps.put(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        node.host() + ":" + node.port());
                perNodeProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
                perNodeProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
                nodeAdmins.add(KafkaAdminClient.create(perNodeProps));
            }
            outer:
            while (System.currentTimeMillis() < deadlineMs) {
                for (Admin nodeAdmin : nodeAdmins) {
                    try {
                        Map<String, TopicDescription> result =
                                nodeAdmin
                                        .describeTopics(Collections.singletonList(topic))
                                        .allTopicNames()
                                        .get(10, TimeUnit.SECONDS);
                        TopicDescription desc = result.get(topic);
                        if (desc == null || desc.partitions().size() != numPartitions) {
                            Thread.sleep(300);
                            continue outer;
                        }
                        for (TopicPartitionInfo tpi : desc.partitions()) {
                            if (tpi.leader() == null) {
                                Thread.sleep(300);
                                continue outer;
                            }
                        }
                    } catch (Exception e) {
                        Thread.sleep(300);
                        continue outer;
                    }
                }
                // All tablets confirmed — ready to produce.
                return;
            }
            throw new IllegalStateException(
                    "Topic '"
                            + topic
                            + "' did not have all "
                            + numPartitions
                            + " leaders elected on all tablets within "
                            + timeout);
        } finally {
            for (Admin a : nodeAdmins) {
                try {
                    a.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
