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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end Produce + Fetch round-trip: KafkaProducer sends records through Phase 2B, then
 * KafkaConsumer in assign mode reads them back through Phase 2C.
 */
class KafkaRoundTripITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

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
    void singleRecordRoundTripsKeyValueAndHeaders() throws Exception {
        String topic = "rt_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        byte[] key = "the-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic, /* partition */ 0, key, value);
            record.headers()
                    .add(new RecordHeader("traceid", "abc-123".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("nulled", null));
            RecordMetadata meta = producer.send(record).get();
            assertThat(meta.offset()).isGreaterThanOrEqualTo(0L);
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(topic))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            ConsumerRecord<byte[], byte[]> round = pollSingle(consumer);
            assertThat(round).isNotNull();
            assertThat(round.topic()).isEqualTo(topic);
            assertThat(round.partition()).isEqualTo(0);
            assertThat(round.key()).containsExactly(key);
            assertThat(round.value()).containsExactly(value);
            assertThat(round.headers().toArray()).hasSize(2);
            assertThat(round.headers().lastHeader("traceid").value())
                    .containsExactly("abc-123".getBytes(StandardCharsets.UTF_8));
            assertThat(round.headers().lastHeader("nulled").value()).isNull();
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void multipleRecordsPreserveOrder() throws Exception {
        String topic = "order_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        List<byte[]> payloads = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < 16; i++) {
                byte[] v = ("msg-" + i).getBytes(StandardCharsets.UTF_8);
                payloads.add(v);
                producer.send(new ProducerRecord<>(topic, 0, null, v)).get();
            }
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(topic))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            List<byte[]> received = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (received.size() < payloads.size() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    received.add(r.value());
                }
            }
            assertThat(received).hasSize(payloads.size());
            for (int i = 0; i < payloads.size(); i++) {
                assertThat(received.get(i)).containsExactly(payloads.get(i));
            }
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void nullKeyRoundTripsAsNull() throws Exception {
        String topic = "nullk_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        byte[] value = "just-value".getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>(topic, 0, null, value)).get();
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(topic))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            ConsumerRecord<byte[], byte[]> r = pollSingle(consumer);
            assertThat(r).isNotNull();
            assertThat(r.key()).isNull();
            assertThat(r.value()).containsExactly(value);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void fanOutAcrossPartitionsAndConsumeAll() throws Exception {
        String topic = "fan_" + System.nanoTime();
        int partitions = 4;
        admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                .all()
                .get();

        int perPartition = 5;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int p = 0; p < partitions; p++) {
                for (int i = 0; i < perPartition; i++) {
                    byte[] v = ("p" + p + "-" + i).getBytes(StandardCharsets.UTF_8);
                    producer.send(new ProducerRecord<>(topic, p, null, v)).get();
                }
            }
        }

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(topic))) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                tps.add(new TopicPartition(topic, p));
            }
            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            int expected = partitions * perPartition;
            int received = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (received < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(2));
                received += batch.count();
            }
            assertThat(received).isEqualTo(expected);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    private static ConsumerRecord<byte[], byte[]> pollSingle(
            KafkaConsumer<byte[], byte[]> consumer) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
            if (!batch.isEmpty()) {
                return batch.iterator().next();
            }
        }
        return null;
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

    private static Properties consumerProps(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rt-" + topic);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
        return conf;
    }
}
