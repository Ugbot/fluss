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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Kafka {@code OFFSET_FOR_LEADER_EPOCH} (API key 23) against the Fluss Kafka
 * bolt-on. kafka-clients 3.9's public {@code Admin} does not expose this API, so we drive it with a
 * raw socket: serialise the request via {@link OffsetsForLeaderEpochRequest}, ship it over TCP to
 * the broker, and parse the response with {@link AbstractResponse#parseResponse(ByteBuffer,
 * RequestHeader)}. Coverage matches the contract documented on {@link
 * org.apache.fluss.kafka.fetch.KafkaOffsetForLeaderEpochTranscoder}.
 */
class KafkaOffsetForLeaderEpochITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final short OFFSET_FOR_LEADER_EPOCH_VERSION = (short) 4;
    private static final AtomicInteger CORRELATION_IDS = new AtomicInteger();

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
    void currentEpochReturnsHighWatermark() throws Exception {
        String topic = "ofle_current_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            long lastOffset = produceRecords(topic, 7);
            // The high watermark equals the log-end-offset once acks=all is observed.
            long expectedHwm = lastOffset + 1;

            // Ask for the current leader epoch — a fresh-from-creation partition should be at
            // epoch 0 on the Fluss side. Use NO_PARTITION_LEADER_EPOCH for "I don't know yet",
            // and separately for the requested leader epoch we pass 0.
            EpochEndOffset result =
                    sendSinglePartitionRequest(topic, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH, 0);

            assertThat(Errors.forCode(result.errorCode())).isEqualTo(Errors.NONE);
            assertThat(result.leaderEpoch()).isEqualTo(0);
            assertThat(result.endOffset()).isEqualTo(expectedHwm);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void futureEpochIsFenced() throws Exception {
        String topic = "ofle_future_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            produceRecords(topic, 1);
            EpochEndOffset result =
                    sendSinglePartitionRequest(topic, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH, 42);
            assertThat(Errors.forCode(result.errorCode())).isEqualTo(Errors.FENCED_LEADER_EPOCH);
            assertThat(result.endOffset()).isEqualTo(-1L);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void currentLeaderEpochMismatchIsFenced() throws Exception {
        String topic = "ofle_currentmismatch_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            produceRecords(topic, 1);
            // CurrentLeaderEpoch > broker's current → UNKNOWN_LEADER_EPOCH.
            EpochEndOffset ahead = sendSinglePartitionRequest(topic, 0, 42, 0);
            assertThat(Errors.forCode(ahead.errorCode())).isEqualTo(Errors.UNKNOWN_LEADER_EPOCH);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void unknownTopicReturnsUnknownTopicOrPartition() throws Exception {
        String ghost = "ofle_ghost_" + System.nanoTime();
        EpochEndOffset result =
                sendSinglePartitionRequest(ghost, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH, 0);
        assertThat(Errors.forCode(result.errorCode())).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(result.endOffset()).isEqualTo(-1L);
    }

    // --- helpers -------------------------------------------------------------

    private static long produceRecords(String topic, int count) throws Exception {
        Random rng = new Random();
        RecordMetadata tail = null;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < count; i++) {
                byte[] key = bytesOf(rng, 8);
                byte[] value = bytesOf(rng, 32);
                tail = producer.send(new ProducerRecord<>(topic, 0, key, value)).get();
            }
            producer.flush();
        }
        assertThat(tail).isNotNull();
        return tail.offset();
    }

    private static EpochEndOffset sendSinglePartitionRequest(
            String topic, int partition, int currentLeaderEpoch, int leaderEpoch) throws Exception {
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        topics.add(
                new OffsetForLeaderTopic()
                        .setTopic(topic)
                        .setPartitions(
                                Collections.singletonList(
                                        new OffsetForLeaderPartition()
                                                .setPartition(partition)
                                                .setCurrentLeaderEpoch(currentLeaderEpoch)
                                                .setLeaderEpoch(leaderEpoch))));
        OffsetForLeaderEpochRequestData data =
                new OffsetForLeaderEpochRequestData()
                        .setReplicaId(OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID)
                        .setTopics(topics);
        OffsetsForLeaderEpochRequest request =
                new OffsetsForLeaderEpochRequest(data, OFFSET_FOR_LEADER_EPOCH_VERSION);

        OffsetsForLeaderEpochResponse response = sendOverSocket(request);
        List<OffsetForLeaderTopicResult> topicResults = response.data().topics().valuesList();
        assertThat(topicResults).hasSize(1);
        OffsetForLeaderTopicResult topicResult = topicResults.get(0);
        assertThat(topicResult.topic()).isEqualTo(topic);
        assertThat(topicResult.partitions()).hasSize(1);
        return topicResult.partitions().get(0);
    }

    private static OffsetsForLeaderEpochResponse sendOverSocket(
            OffsetsForLeaderEpochRequest request) throws Exception {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        RequestHeader header =
                new RequestHeader(
                        ApiKeys.OFFSET_FOR_LEADER_EPOCH,
                        request.version(),
                        "fluss-it-ofle",
                        correlationId);
        ByteBuffer body = request.serializeWithHeader(header);

        Node target = resolveLeader(request.data());
        try (Socket socket = new Socket(target.host(), target.port())) {
            socket.setSoTimeout(30_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(body.remaining());
            // serializeWithHeader returns a buffer positioned at 0, limit=size.
            byte[] bodyBytes = new byte[body.remaining()];
            body.get(bodyBytes);
            out.write(bodyBytes);
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            int respLen = in.readInt();
            assertThat(respLen).isPositive();
            byte[] respBytes = new byte[respLen];
            in.readFully(respBytes);
            ByteBuffer respBuffer = ByteBuffer.wrap(respBytes);
            AbstractResponse parsed = AbstractResponse.parseResponse(respBuffer, header);
            assertThat(parsed).isInstanceOf(OffsetsForLeaderEpochResponse.class);
            return (OffsetsForLeaderEpochResponse) parsed;
        }
    }

    /**
     * Resolve the broker that is leader for the (first) partition referenced by the request. For
     * unknown topics {@code describeTopics} fails fast; in that case we fall back to the first live
     * broker so the test still exercises the UNKNOWN_TOPIC_OR_PARTITION path.
     */
    private static Node resolveLeader(OffsetForLeaderEpochRequestData data) throws Exception {
        OffsetForLeaderTopic first = data.topics().iterator().next();
        int partition = first.partitions().get(0).partition();
        try {
            TopicDescription desc =
                    admin.describeTopics(Collections.singletonList(first.topic()))
                            .allTopicNames()
                            .get()
                            .get(first.topic());
            for (TopicPartitionInfo info : desc.partitions()) {
                if (info.partition() == partition && info.leader() != null) {
                    return info.leader();
                }
            }
        } catch (Exception ignored) {
            // Topic may not exist; fall through to any broker so we can still exercise the
            // UNKNOWN_TOPIC_OR_PARTITION branch.
        }
        ServerNode fallback = CLUSTER.getTabletServerNodes(KAFKA_LISTENER).get(0);
        return new Node(fallback.id(), fallback.host(), fallback.port());
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

    private static byte[] bytesOf(Random rng, int len) {
        byte[] bytes = new byte[len];
        rng.nextBytes(bytes);
        return bytes;
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
