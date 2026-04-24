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
import org.apache.fluss.kafka.admin.KafkaDescribeProducersTranscoder;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end DescribeProducers test driving {@link KafkaDescribeProducersTranscoder} directly
 * against a real 3-node Fluss cluster's {@link ReplicaManager}. The transcoder's {@link
 * DescribeProducersResponseData} output is the Kafka wire boundary; we exercise it directly rather
 * than routing through kafka-clients' {@code DescribeProducersHandler}, which adds partition-leader
 * routing plumbing unrelated to Phase I.2.
 *
 * <p>Phase I.2 coverage:
 *
 * <ul>
 *   <li>Unknown topic (catalog lookup miss) — {@code UNKNOWN_TOPIC_OR_PARTITION}.
 *   <li>Bucket not hosted by the local tablet server (synthetic tableId) — {@code
 *       UNKNOWN_TOPIC_OR_PARTITION}.
 *   <li>Populated happy path: an idempotent {@code KafkaProducer} with {@code
 *       enable.idempotence=true} writes 10 records to a leader; the transcoder reads back one
 *       {@code ProducerState} with {@code producerId > 0}, {@code producerEpoch == 0}, {@code
 *       lastSequence == 9}, {@code lastTimestamp > 0}, {@code coordinatorEpoch == -1}, {@code
 *       currentTxnStartOffset == -1}.
 * </ul>
 */
class KafkaDescribeProducersITCase {

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
    void transcoderReportsUnknownTopicForGhostTopic() throws Exception {
        // No topic created; every server must return UNKNOWN_TOPIC_OR_PARTITION because the
        // catalog lookup misses for the ghost name.
        ReplicaManager rm = CLUSTER.getTabletServerById(0).getReplicaManager();
        String ghost = "ghost_" + System.nanoTime();
        PartitionResponse pr = invokeTranscoder(rm, ghost, -1L, 0);
        assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        assertThat(pr.activeProducers()).isEmpty();
    }

    @Test
    void transcoderReportsUnknownForUnhostedBucket() throws Exception {
        // Even after creating a topic, picking a bucket id the tablet server does not host
        // (e.g. tableId well beyond any real allocation) must come back as UNKNOWN.
        String topic = "describeproducers_misroute_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            ReplicaManager rm = CLUSTER.getTabletServerById(0).getReplicaManager();
            // A huge synthetic tableId guarantees the local tablet server does not host it.
            long fakeTableId = Long.MAX_VALUE - 7L;
            Optional<org.apache.fluss.rpc.replica.ReplicaSnapshot> snap =
                    rm.getReplicaSnapshot(new TableBucket(fakeTableId, 0));
            assertThat(snap).isEmpty();

            PartitionResponse pr = invokeTranscoder(rm, topic, fakeTableId, 0);
            assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            assertThat(pr.activeProducers()).isEmpty();
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void idempotentProducerRoundTripsWriterState() throws Exception {
        String topic = "idempotent_" + System.nanoTime();
        // One partition, replication-1 to keep leader resolution trivial and avoid ISR flicker.
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            // Resolve the bucket's leader so we invoke the transcoder against the correct server.
            TablePath path = TablePath.of(KAFKA_DATABASE, topic);
            long tableId =
                    CLUSTER.newCoordinatorClient()
                            .getTableInfo(RpcMessageTestUtils.newGetTableInfoRequest(path))
                            .get()
                            .getTableId();
            TableBucket tb = new TableBucket(tableId, 0);
            CLUSTER.waitUntilAllReplicaReady(tb);
            int leaderServer = CLUSTER.waitAndGetLeader(tb);
            ReplicaManager rm = CLUSTER.getTabletServerById(leaderServer).getReplicaManager();

            // Idempotent producer: enable.idempotence=true forces the broker to allocate a
            // producer id, and every record carries a sequence number. After flushing 10
            // records, WriterStateManager holds one entry with lastBatchSequence=9.
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            props.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
            props.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);

            int recordCount = 10;
            long minTimestampBefore = System.currentTimeMillis();
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
                Future<RecordMetadata> last = null;
                for (int i = 0; i < recordCount; i++) {
                    byte[] key = ("k-" + i).getBytes();
                    byte[] value = ("v-" + i).getBytes();
                    last = producer.send(new ProducerRecord<>(topic, 0, key, value));
                }
                // Await every in-flight send so WriterStateManager sees all 10 appends.
                producer.flush();
                if (last != null) {
                    last.get();
                }
            }

            // Invoke transcoder on the leader server with a catalog stub that resolves the topic
            // to the real tableId so the transcoder hits getProducerStates.
            PartitionResponse pr = invokeTranscoder(rm, topic, tableId, 0);
            assertThat(pr.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(pr.activeProducers())
                    .as("active idempotent producers for bucket %s", tb)
                    .hasSize(1);
            DescribeProducersResponseData.ProducerState state = pr.activeProducers().get(0);
            assertThat(state.producerId()).as("producerId").isGreaterThan(0L);
            assertThat(state.producerEpoch())
                    .as("producerEpoch filler — Fluss has no epoch concept")
                    .isEqualTo(0);
            assertThat(state.lastSequence())
                    .as("lastSequence = recordCount - 1 (Kafka sequences are zero-based)")
                    .isEqualTo(recordCount - 1);
            assertThat(state.lastTimestamp())
                    .as("lastTimestamp is a real wall-clock millis reading")
                    .isGreaterThanOrEqualTo(minTimestampBefore);
            assertThat(state.coordinatorEpoch())
                    .as("coordinatorEpoch filler — Fluss has no txn coordinator")
                    .isEqualTo(-1);
            assertThat(state.currentTxnStartOffset())
                    .as("currentTxnStartOffset filler — Fluss has no txn producer")
                    .isEqualTo(-1L);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    /** Invoke the transcoder against a real server's ReplicaManager. */
    private static PartitionResponse invokeTranscoder(
            ReplicaManager rm, String topic, long tableId, int partitionIndex) throws Exception {
        KafkaTopicsCatalog catalog =
                new KafkaTopicsCatalog() {
                    @Override
                    public void register(KafkaTopicInfo info) {}

                    @Override
                    public void deregister(String t) {}

                    @Override
                    public Optional<KafkaTopicInfo> lookup(String t) {
                        if (tableId >= 0 && t.equals(topic)) {
                            return Optional.of(
                                    new KafkaTopicInfo(
                                            t,
                                            new org.apache.fluss.metadata.TablePath(
                                                    KAFKA_DATABASE, t),
                                            tableId,
                                            KafkaTopicInfo.TimestampType.CREATE_TIME,
                                            null,
                                            org.apache.kafka.common.Uuid.randomUuid()));
                        }
                        return Optional.empty();
                    }

                    @Override
                    public List<KafkaTopicInfo> listAll() {
                        return Collections.emptyList();
                    }
                };
        KafkaDescribeProducersTranscoder transcoder =
                new KafkaDescribeProducersTranscoder(catalog, rm);
        DescribeProducersRequestData req = new DescribeProducersRequestData();
        req.setTopics(
                Collections.singletonList(
                        new TopicRequest()
                                .setName(topic)
                                .setPartitionIndexes(Collections.singletonList(partitionIndex))));
        DescribeProducersResponseData resp = transcoder.describeProducers(req);
        return resp.topics().get(0).partitions().get(0);
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
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
