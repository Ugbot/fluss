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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SASL_PLAINTEXT exercise over the Kafka listener.
 *
 * <p>Stands up a cluster where the {@code KAFKA} listener is configured as SASL_PLAINTEXT with the
 * {@code PLAIN} mechanism and a single user ({@code alice}). Drives the broker with unmodified
 * kafka-clients 3.9 producer/consumer/admin. The SASL frames are handled by {@link
 * org.apache.fluss.kafka.auth.KafkaSaslTranscoder}, which delegates to Fluss's {@link
 * org.apache.fluss.security.auth.ServerAuthenticator} SPI — no Kafka-specific auth code.
 */
class KafkaSaslPlainITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;

    private static final String USERNAME = "alice";
    private static final String PASSWORD = "alice-secret";

    private static final String SERVER_JAAS_INFO =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "    user_"
                    + USERNAME
                    + "=\""
                    + PASSWORD
                    + "\";";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(saslKafkaClusterConf())
                    .build();

    private static Admin admin;

    @BeforeAll
    static void createAdmin() {
        admin = KafkaAdminClient.create(adminProps());
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void goldenPath_producerAndConsumerRoundTripWithCorrectCredentials() throws Exception {
        String topic = "sasl_ok_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        byte[] value = ("payload-" + System.nanoTime()).getBytes(StandardCharsets.UTF_8);

        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(USERNAME, PASSWORD))) {
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, 0, null, value)).get();
            assertThat(meta.offset()).isGreaterThanOrEqualTo(0L);
        }

        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProps(USERNAME, PASSWORD, topic))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            ConsumerRecord<byte[], byte[]> record = pollSingle(consumer);
            assertThat(record).isNotNull();
            assertThat(record.value()).containsExactly(value);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void badPassword_saslAuthenticationExceptionSurfacedToClient() {
        String topic = "sasl_bad_" + System.nanoTime();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps(USERNAME, "this-is-not-the-password"))) {
            byte[] value = "never-arrives".getBytes(StandardCharsets.UTF_8);
            assertThatThrownBy(
                            () -> producer.send(new ProducerRecord<>(topic, 0, null, value)).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    void noSaslClient_onSaslListener_producerSendNeverSucceeds() {
        String topic = "sasl_none_" + System.nanoTime();
        Properties plainProps = new Properties();
        plainProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        plainProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        plainProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        plainProps.put(ProducerConfig.ACKS_CONFIG, "all");
        // Short timeouts so the test fails fast rather than waiting 30s on a broker that will
        // refuse every business API until SASL completes.
        plainProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        plainProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        plainProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(plainProps)) {
            byte[] value = "never-arrives".getBytes(StandardCharsets.UTF_8);
            // Accept any failure: SASL rejection, timeout, or connection-level error — all are
            // legitimate outcomes of speaking PLAINTEXT to a SASL listener.
            assertThatThrownBy(
                            () -> producer.send(new ProducerRecord<>(topic, 0, null, value)).get())
                    .isInstanceOf(Exception.class);
        }
    }

    // --- helpers ---

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
        putSaslPlainClientProps(props, USERNAME, PASSWORD);
        return props;
    }

    private static Properties producerProps(String user, String pass) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20_000);
        putSaslPlainClientProps(props, user, pass);
        return props;
    }

    private static Properties consumerProps(String user, String pass, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sasl-" + topic);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        putSaslPlainClientProps(props, user, pass);
        return props;
    }

    private static void putSaslPlainClientProps(Properties props, String user, String pass) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\""
                        + user
                        + "\" password=\""
                        + pass
                        + "\";");
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

    private static Configuration saslKafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        // Declare the KAFKA listener as SASL in the server's protocol map. Listeners omitted from
        // the map (e.g. the internal FLUSS listener) default to PLAINTEXT per the option's
        // contract.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), KAFKA_LISTENER + ":sasl");
        conf.setString(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "PLAIN");
        // Per-listener, per-mechanism JAAS config. Matches the key shape that Fluss's
        // SaslServerAuthenticator looks up in Configuration. Also set the mechanism-level
        // fallback so both resolution paths find the config.
        conf.setString(
                "security.sasl.listener.name."
                        + KAFKA_LISTENER.toLowerCase()
                        + ".plain.jaas.config",
                SERVER_JAAS_INFO);
        conf.setString("security.sasl.plain.jaas.config", SERVER_JAAS_INFO);
        return conf;
    }
}
