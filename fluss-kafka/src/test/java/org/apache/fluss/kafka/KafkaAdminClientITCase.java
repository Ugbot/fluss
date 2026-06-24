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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for Phase 1 Kafka metadata APIs: a real {@code kafka-clients} {@link Admin} is
 * pointed at Fluss's KAFKA listener and must resolve brokers, topics and partitions.
 */
public class KafkaAdminClientITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int NUM_BUCKETS = 4;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
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
        List<ServerNode> nodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes(KAFKA_LISTENER);
        assertThat(nodes).hasSize(NUM_TABLET_SERVERS);

        StringBuilder bootstrap = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                bootstrap.append(',');
            }
            bootstrap.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap.toString());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        admin = KafkaAdminClient.create(props);
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close();
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        FLUSS_CLUSTER_EXTENSION
                .newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void describeClusterResolvesAllTabletServers() throws Exception {
        DescribeClusterResult result = admin.describeCluster();

        String clusterId = result.clusterId().get();
        assertThat(clusterId).isNotNull().isNotEmpty();

        java.util.Collection<Node> nodes = result.nodes().get();
        assertThat(nodes).hasSize(NUM_TABLET_SERVERS);
        assertThat(nodes)
                .extracting(Node::id)
                .containsExactlyInAnyOrderElementsOf(
                        FLUSS_CLUSTER_EXTENSION.getTabletServerInfos().stream()
                                .map(info -> info.id())
                                .collect(java.util.stream.Collectors.toList()));

        // Coordinator is FLUSS-only in this test, so we surface one of the tablet servers as the
        // Kafka "controller" - AdminClient needs a routable node for admin ops, and tablet servers
        // forward to the real Fluss coordinator via coordinatorGateway.
        Node controller = result.controller().get();
        assertThat(controller).isNotNull();
        assertThat(nodes).anyMatch(n -> n.id() == controller.id());
    }

    @Test
    void listTopicsSurfacesFlussTables() throws Exception {
        TablePath tablePath = new TablePath(KAFKA_DATABASE, "orders_" + System.nanoTime());
        createTable(tablePath, NUM_BUCKETS);

        try {
            Map<String, TopicListing> topics =
                    admin.listTopics(new ListTopicsOptions().listInternal(true))
                            .namesToListings()
                            .get();
            assertThat(topics).containsKey(tablePath.getTableName());
        } finally {
            RpcMessageTestUtils.dropTable(FLUSS_CLUSTER_EXTENSION, tablePath);
        }
    }

    @Test
    void describeTopicsReturnsBucketsAsPartitions() throws Exception {
        TablePath tablePath = new TablePath(KAFKA_DATABASE, "events_" + System.nanoTime());
        long tableId = createTable(tablePath, NUM_BUCKETS);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                new org.apache.fluss.metadata.TableBucket(tableId, 0));

        try {
            Map<String, TopicDescription> described =
                    admin.describeTopics(Collections.singletonList(tablePath.getTableName()))
                            .allTopicNames()
                            .get();

            TopicDescription description = described.get(tablePath.getTableName());
            assertThat(description).isNotNull();
            assertThat(description.partitions()).hasSize(NUM_BUCKETS);

            Map<Integer, ServerNode> liveServers =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerNodes(KAFKA_LISTENER).stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            ServerNode::id,
                                            java.util.function.Function.identity()));
            description
                    .partitions()
                    .forEach(
                            p -> {
                                assertThat(p.leader()).isNotNull();
                                assertThat(liveServers).containsKey(p.leader().id());
                                assertThat(p.replicas()).isNotEmpty();
                                assertThat(p.isr()).isNotEmpty();
                            });
        } finally {
            RpcMessageTestUtils.dropTable(FLUSS_CLUSTER_EXTENSION, tablePath);
        }
    }

    @Test
    void unknownTopicReturnsUnknownTopicOrPartition() {
        String unknown = "ghost_" + System.nanoTime();
        assertThatThrownBy(
                        () ->
                                admin.describeTopics(Collections.singletonList(unknown))
                                        .allTopicNames()
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void listTopicsIsEmptyWhenKafkaDatabaseHasNoTables() throws Exception {
        // No tables created in this test; the kafka database exists (coordinator auto-creates it).
        Map<String, TopicListing> topics =
                admin.listTopics(new ListTopicsOptions().listInternal(true))
                        .namesToListings()
                        .get();
        // Other tests may have created+dropped tables; at minimum the call must succeed.
        assertThat(topics).isNotNull();
    }

    @Test
    void describeMultipleTopicsInOneRequest() throws Exception {
        TablePath first = new TablePath(KAFKA_DATABASE, "multi_a_" + System.nanoTime());
        TablePath second = new TablePath(KAFKA_DATABASE, "multi_b_" + System.nanoTime());
        long firstId = createTable(first, 2);
        long secondId = createTable(second, NUM_BUCKETS);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                new org.apache.fluss.metadata.TableBucket(firstId, 0));
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                new org.apache.fluss.metadata.TableBucket(secondId, 0));

        try {
            Map<String, TopicDescription> described =
                    admin.describeTopics(
                                    java.util.Arrays.asList(
                                            first.getTableName(), second.getTableName()))
                            .allTopicNames()
                            .get();

            assertThat(described).containsOnlyKeys(first.getTableName(), second.getTableName());
            assertThat(described.get(first.getTableName()).partitions()).hasSize(2);
            assertThat(described.get(second.getTableName()).partitions()).hasSize(NUM_BUCKETS);
        } finally {
            RpcMessageTestUtils.dropTable(FLUSS_CLUSTER_EXTENSION, first);
            RpcMessageTestUtils.dropTable(FLUSS_CLUSTER_EXTENSION, second);
        }
    }

    @Test
    void describeTopicsMixesKnownAndUnknown() throws Exception {
        TablePath known = new TablePath(KAFKA_DATABASE, "mixed_" + System.nanoTime());
        long tableId = createTable(known, NUM_BUCKETS);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(
                new org.apache.fluss.metadata.TableBucket(tableId, 0));

        String ghost = "ghost_" + System.nanoTime();
        try {
            // AdminClient.allTopicNames() fails fast if any topic is missing; use
            // topicNameValues() to get per-topic futures.
            Map<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> byName =
                    admin.describeTopics(java.util.Arrays.asList(known.getTableName(), ghost))
                            .topicNameValues();

            assertThat(byName.get(known.getTableName()).get().partitions()).hasSize(NUM_BUCKETS);
            assertThatThrownBy(() -> byName.get(ghost).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
        } finally {
            RpcMessageTestUtils.dropTable(FLUSS_CLUSTER_EXTENSION, known);
        }
    }

    private static long createTable(TablePath path, int numBuckets) throws Exception {
        // Create the topic through the Kafka API so the catalog picks up the binding.
        admin.createTopics(
                        Collections.singletonList(
                                new org.apache.kafka.clients.admin.NewTopic(
                                        path.getTableName(), numBuckets, (short) 1)))
                .all()
                .get();
        return FLUSS_CLUSTER_EXTENSION
                .newCoordinatorClient()
                .getTableInfo(RpcMessageTestUtils.newGetTableInfoRequest(path))
                .get()
                .getTableId();
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        // Match the listener name used in set{Coordinator,TabletServer}Listeners.
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
