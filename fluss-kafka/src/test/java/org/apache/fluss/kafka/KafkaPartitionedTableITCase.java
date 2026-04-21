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
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 1 IT for partitioned Fluss tables: each partition surfaces as {@code table.partition} in
 * the Kafka Metadata response; the bare table name is not a valid topic.
 */
class KafkaPartitionedTableITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int NUM_BUCKETS = 2;

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
        StringBuilder bootstrap = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
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

    @Test
    void partitionsSurfaceAsDottedTopics() throws Exception {
        TablePath tablePath = new TablePath(KAFKA_DATABASE, "events_" + System.nanoTime());
        createPartitionedTable(tablePath, NUM_BUCKETS);

        String partitionA = "p1";
        String partitionB = "p2";
        RpcMessageTestUtils.createPartition(
                CLUSTER,
                tablePath,
                new PartitionSpec(Collections.singletonMap("b", partitionA)),
                false);
        RpcMessageTestUtils.createPartition(
                CLUSTER,
                tablePath,
                new PartitionSpec(Collections.singletonMap("b", partitionB)),
                false);
        CLUSTER.waitUntilPartitionAllReady(tablePath, 2);

        String topicA = tablePath.getTableName() + "." + partitionA;
        String topicB = tablePath.getTableName() + "." + partitionB;

        try {
            Map<String, TopicListing> listed =
                    admin.listTopics(new ListTopicsOptions().listInternal(true))
                            .namesToListings()
                            .get();
            assertThat(listed).containsKeys(topicA, topicB);
            // The bare table name is NOT a legal topic for a partitioned table.
            assertThat(listed).doesNotContainKey(tablePath.getTableName());

            Map<String, TopicDescription> described =
                    admin.describeTopics(Arrays.asList(topicA, topicB)).allTopicNames().get();

            Set<Integer> liveServerIds =
                    CLUSTER.getTabletServerNodes(KAFKA_LISTENER).stream()
                            .map(ServerNode::id)
                            .collect(Collectors.toSet());

            for (TopicDescription d : described.values()) {
                assertThat(d.partitions()).hasSize(NUM_BUCKETS);
                d.partitions()
                        .forEach(
                                p -> {
                                    assertThat(p.leader()).isNotNull();
                                    assertThat(liveServerIds).contains(p.leader().id());
                                });
            }
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, tablePath);
        }
    }

    @Test
    void bareNameForPartitionedTableIsUnknown() throws Exception {
        TablePath tablePath = new TablePath(KAFKA_DATABASE, "bare_" + System.nanoTime());
        createPartitionedTable(tablePath, NUM_BUCKETS);

        try {
            assertThatThrownBy(
                            () ->
                                    admin.describeTopics(
                                                    Collections.singletonList(
                                                            tablePath.getTableName()))
                                            .allTopicNames()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, tablePath);
        }
    }

    @Test
    void dottedNameForNonPartitionedTableIsUnknown() throws Exception {
        TablePath tablePath = new TablePath(KAFKA_DATABASE, "flat_" + System.nanoTime());
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(NUM_BUCKETS).build();
        RpcMessageTestUtils.createTable(CLUSTER, tablePath, descriptor);

        try {
            String dotted = tablePath.getTableName() + ".anything";
            assertThatThrownBy(
                            () ->
                                    admin.describeTopics(Collections.singletonList(dotted))
                                            .allTopicNames()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, tablePath);
        }
    }

    private static void createPartitionedTable(TablePath path, int numBuckets) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(numBuckets)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .customProperty(
                                org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog
                                        .PROP_BINDING_MARKER,
                                "true")
                        .customProperty(
                                org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog
                                        .PROP_TOPIC_NAME,
                                path.getTableName())
                        .customProperty(
                                org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog
                                        .PROP_TIMESTAMP_TYPE,
                                "CreateTime")
                        .customProperty(
                                org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog
                                        .PROP_TOPIC_ID,
                                org.apache.kafka.common.Uuid.randomUuid().toString())
                        .build();
        RpcMessageTestUtils.createTable(CLUSTER, path, descriptor);
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
