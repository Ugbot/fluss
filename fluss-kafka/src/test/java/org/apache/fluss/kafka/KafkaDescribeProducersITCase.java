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
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end DescribeProducers test driving {@link KafkaDescribeProducersTranscoder} directly
 * against a real 3-node Fluss cluster's {@link ReplicaManager}. The transcoder's {@link
 * DescribeProducersResponseData} output is the Kafka wire boundary; we exercise it directly rather
 * than routing through kafka-clients' {@code DescribeProducersHandler}, which adds partition-leader
 * routing plumbing unrelated to Phase I.2.
 *
 * <p>Happy-path producer-tracking coverage (idempotent writes -> non-empty activeProducers) is
 * deferred: Fluss's {@code WriterStateManager} tracks writer ids per bucket, and the transcoder
 * (plus {@link ReplicaManager#getProducerStates}) already projects them through; but driving a
 * kafka-clients idempotent producer against a 3-node Fluss cluster is unreliable today because the
 * coordinator's replica-activation handshake for Kafka-created topics races with the producer's
 * retry loop. Until the bolt-on exposes a synchronous "topic-is-ready" gate, the transcoder's
 * contract is validated by the not-hosted / unknown-topic cases exercised here; populated-producer
 * coverage lands when the handshake race is resolved (TODO Phase I follow-up).
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
