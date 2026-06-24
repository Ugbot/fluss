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
import org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog;
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 consumer smoke: real {@link KafkaConsumer} in assign mode exercising the Metadata path.
 * We avoid {@code subscribe(...)} + group coordination and {@code poll(...)} because those depend
 * on API keys the broker has not yet implemented.
 */
class KafkaConsumerITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int NUM_BUCKETS = 3;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

    @Test
    void listTopicsAndPartitionsForReflectFlussState() throws Exception {
        TablePath path = new TablePath(KAFKA_DATABASE, "consumer_topic_" + System.nanoTime());
        long tableId = createTable(path, NUM_BUCKETS);
        CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableId, 0));

        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer()) {
            Map<String, List<PartitionInfo>> allTopics = consumer.listTopics();
            assertThat(allTopics).containsKey(path.getTableName());

            List<PartitionInfo> partitions = consumer.partitionsFor(path.getTableName());
            assertThat(partitions).hasSize(NUM_BUCKETS);

            Set<Integer> liveServerIds =
                    CLUSTER.getTabletServerNodes(KAFKA_LISTENER).stream()
                            .map(ServerNode::id)
                            .collect(Collectors.toSet());
            for (PartitionInfo partition : partitions) {
                assertThat(partition.leader()).isNotNull();
                assertThat(liveServerIds).contains(partition.leader().id());
            }
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, path);
        }
    }

    @Test
    void assignToAllPartitionsBindsWithoutGroupCoordinator() throws Exception {
        TablePath path = new TablePath(KAFKA_DATABASE, "assign_topic_" + System.nanoTime());
        long tableId = createTable(path, NUM_BUCKETS);
        CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableId, 0));

        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer()) {
            List<PartitionInfo> partitions = consumer.partitionsFor(path.getTableName());
            List<TopicPartition> assigned = new ArrayList<>(partitions.size());
            for (PartitionInfo info : partitions) {
                assigned.add(new TopicPartition(info.topic(), info.partition()));
            }

            consumer.assign(assigned);
            assertThat(consumer.assignment()).containsExactlyInAnyOrderElementsOf(assigned);
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, path);
        }
    }

    private static long createTable(TablePath path, int numBuckets) throws Exception {
        // Create a Kafka-bound data table directly through the Fluss gateway so the consumer sees
        // it via the Kafka catalog without going through the admin wire protocol.
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(KafkaDataTable.schema())
                        .distributedBy(numBuckets)
                        .customProperty(CustomPropertiesTopicsCatalog.PROP_BINDING_MARKER, "true")
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TOPIC_NAME, path.getTableName())
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TIMESTAMP_TYPE, "CreateTime")
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TOPIC_ID,
                                Uuid.randomUuid().toString())
                        .build();
        return RpcMessageTestUtils.createTable(CLUSTER, path, descriptor);
    }

    private static KafkaConsumer<byte[], byte[]> newConsumer() {
        StringBuilder bootstrap = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                bootstrap.append(',');
            }
            bootstrap.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap.toString());
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        // Assign mode does not require a real group, but the config key is mandatory in some
        // versions - pass a unique value to keep the KafkaConsumer constructor happy.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fluss-consumer-it-" + System.nanoTime());
        return new KafkaConsumer<>(props);
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
