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
import org.apache.kafka.common.TopicPartition;
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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test that two real Kafka consumers in the same group rebalance correctly and jointly
 * consume every record. Exercises the JoinGroup / SyncGroup / Heartbeat state machine with the
 * asynchronous follower-wait path and the generation bump triggered by the second joiner.
 */
class KafkaMultiConsumerRebalanceITCase {

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
    void twoConsumersJointlyConsumeEveryRecord() throws Exception {
        String topic = "rb_" + System.nanoTime();
        String group = "gr_" + System.nanoTime();
        int partitions = 4;
        int preSeed = 2;
        int postSeed = 8;
        int expected = partitions * (preSeed + postSeed);

        admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)))
                .all()
                .get();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            ConcurrentMap<String, ConcurrentMap<String, Boolean>> received =
                    new ConcurrentHashMap<>();
            ConcurrentMap<String, Set<Integer>> partitionsPerConsumer = new ConcurrentHashMap<>();
            AtomicBoolean keepRunning = new AtomicBoolean(true);
            CountDownLatch allSeen = new CountDownLatch(expected);
            CountDownLatch firstAssigned = new CountDownLatch(1);

            // Seed a small initial batch so consumer A starts with something to consume.
            send(producer, topic, partitions, preSeed, 0);

            Thread a =
                    consumerThread(
                            "A",
                            group,
                            topic,
                            received,
                            partitionsPerConsumer,
                            keepRunning,
                            allSeen,
                            firstAssigned);
            a.start();
            // Wait until A has a stable assignment, so B's join demonstrably triggers a rebalance.
            assertThat(firstAssigned.await(30, TimeUnit.SECONDS))
                    .as("consumer A should have an assignment before B joins")
                    .isTrue();

            CountDownLatch bAssigned = new CountDownLatch(1);
            Thread b =
                    consumerThread(
                            "B",
                            group,
                            topic,
                            received,
                            partitionsPerConsumer,
                            keepRunning,
                            allSeen,
                            bAssigned);
            b.start();

            // Give B up to 60s to land a stable assignment after the rebalance. Only then do we
            // produce the post-seed batch — otherwise A can drain every partition before B takes
            // ownership of anything.
            assertThat(bAssigned.await(60, TimeUnit.SECONDS))
                    .as("consumer B should receive a partition assignment after joining")
                    .isTrue();
            send(producer, topic, partitions, postSeed, preSeed);

            boolean complete = allSeen.await(90, TimeUnit.SECONDS);
            keepRunning.set(false);
            a.join(10_000);
            b.join(10_000);

            assertThat(complete)
                    .as("both consumers together should consume all produced records")
                    .isTrue();
            Set<String> seen = new HashSet<>();
            for (ConcurrentMap<String, Boolean> byConsumer : received.values()) {
                seen.addAll(byConsumer.keySet());
            }
            assertThat(seen)
                    .as("every produced record should have been observed at least once")
                    .hasSize(expected);
            // Records should have been split: both consumers did useful work.
            assertThat(received.getOrDefault("A", new ConcurrentHashMap<>()))
                    .as("consumer A received records")
                    .isNotEmpty();
            assertThat(received.getOrDefault("B", new ConcurrentHashMap<>()))
                    .as("consumer B received records after the rebalance")
                    .isNotEmpty();
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    private static void send(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            int partitions,
            int perPartition,
            int offsetStart)
            throws Exception {
        for (int p = 0; p < partitions; p++) {
            for (int i = 0; i < perPartition; i++) {
                int idx = offsetStart + i;
                producer.send(
                                new ProducerRecord<>(
                                        topic,
                                        p,
                                        null,
                                        ("p" + p + "-" + idx).getBytes(StandardCharsets.UTF_8)))
                        .get();
            }
        }
    }

    private Thread consumerThread(
            String name,
            String group,
            String topic,
            ConcurrentMap<String, ConcurrentMap<String, Boolean>> received,
            ConcurrentMap<String, Set<Integer>> partitionsPerConsumer,
            AtomicBoolean keepRunning,
            CountDownLatch allSeen,
            CountDownLatch readySignal) {
        ConcurrentMap<String, Boolean> myRecords = new ConcurrentHashMap<>();
        received.put(name, myRecords);
        Runnable body =
                () -> {
                    try (KafkaConsumer<byte[], byte[]> consumer =
                            new KafkaConsumer<>(consumerProps(group))) {
                        consumer.subscribe(Collections.singletonList(topic));
                        while (keepRunning.get()) {
                            ConsumerRecords<byte[], byte[]> batch =
                                    consumer.poll(Duration.ofMillis(500));
                            Set<Integer> parts = new HashSet<>();
                            for (TopicPartition tp : consumer.assignment()) {
                                parts.add(tp.partition());
                            }
                            partitionsPerConsumer.put(name, parts);
                            if (readySignal != null && !parts.isEmpty()) {
                                readySignal.countDown();
                            }
                            for (ConsumerRecord<byte[], byte[]> r : batch) {
                                String key = r.partition() + ":" + r.offset();
                                if (myRecords.put(key, true) == null) {
                                    allSeen.countDown();
                                }
                            }
                        }
                    } catch (Throwable t) {
                        // Swallow — the test assertions cover correctness; this just avoids a
                        // failing thread from terminating the JVM in test mode.
                        t.printStackTrace();
                    }
                };
        Thread t = new Thread(body, "kafka-consumer-" + name);
        t.setDaemon(true);
        return t;
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
        return props;
    }

    private static Properties consumerProps(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 40_000);
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
