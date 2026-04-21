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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 smoke test: a real {@link KafkaProducer} pointed at the KAFKA listener must resolve a
 * Fluss table's buckets as Kafka partitions via {@code partitionsFor}. Does NOT call {@code send()}
 * because the Kafka PRODUCE handler is not yet implemented.
 */
class KafkaProducerITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;
    private static final int NUM_BUCKETS = 4;

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
    void partitionsForReturnsAllBucketsWithLiveLeaders() throws Exception {
        TablePath path = new TablePath(KAFKA_DATABASE, "producer_topic_" + System.nanoTime());
        long tableId = createTable(path, NUM_BUCKETS);
        CLUSTER.waitUntilAllReplicaReady(new TableBucket(tableId, 0));

        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            List<PartitionInfo> partitions = producer.partitionsFor(path.getTableName());

            assertThat(partitions).hasSize(NUM_BUCKETS);
            Set<Integer> partitionIds =
                    partitions.stream().map(PartitionInfo::partition).collect(Collectors.toSet());
            assertThat(partitionIds).containsExactlyInAnyOrder(0, 1, 2, 3);

            Set<Integer> liveServerIds =
                    CLUSTER.getTabletServerNodes(KAFKA_LISTENER).stream()
                            .map(ServerNode::id)
                            .collect(Collectors.toSet());

            for (PartitionInfo partition : partitions) {
                assertThat(partition.leader())
                        .as("partition %s has a leader", partition.partition())
                        .isNotNull();
                assertThat(liveServerIds).contains(partition.leader().id());
                assertThat(partition.replicas()).isNotEmpty();
                assertThat(partition.inSyncReplicas()).isNotEmpty();
            }
        } finally {
            RpcMessageTestUtils.dropTable(CLUSTER, path);
        }
    }

    @Test
    void partitionsForUnknownTopicFailsCleanly() {
        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            assertThat(catching(() -> producer.partitionsFor("ghost_" + System.nanoTime())))
                    .isNotNull();
        }
    }

    private static Throwable catching(ThrowingRunnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static long createTable(TablePath path, int numBuckets) throws Exception {
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

    private static KafkaProducer<byte[], byte[]> newProducer() {
        StringBuilder bootstrap = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                bootstrap.append(',');
            }
            bootstrap.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap.toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20_000);
        return new KafkaProducer<>(props);
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
