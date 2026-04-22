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

package org.apache.fluss.kafka.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.KafkaServerContext;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maps Fluss cluster state to Kafka {@link MetadataResponseData} / {@link
 * DescribeClusterResponseData}. Pure with respect to its inputs so it can be unit-tested without
 * spinning up a cluster.
 */
@Internal
public final class KafkaMetadataBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataBuilder.class);

    private final KafkaServerContext context;
    private final KafkaTopicsCatalog catalog;
    private final KafkaTopicMapping topicMapping;
    private final String kafkaListenerName;

    public KafkaMetadataBuilder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, String kafkaListenerName) {
        this.context = context;
        this.catalog = catalog;
        this.topicMapping = new KafkaTopicMapping(context.kafkaDatabase());
        this.kafkaListenerName = kafkaListenerName;
    }

    /** Build a {@link MetadataResponseData} for the given request. */
    public MetadataResponseData buildMetadataResponse(MetadataRequest request) {
        TabletServerMetadataCache cache = context.metadataCache();
        MetadataManager manager = context.metadataManager();

        MetadataResponseData response = new MetadataResponseData();
        response.setThrottleTimeMs(0);
        response.setClusterId(context.clusterId());
        response.setControllerId(resolveControllerId(cache));
        response.setBrokers(buildBrokers(cache));

        List<String> topicNames = resolveRequestedTopics(request, manager);
        List<MetadataResponseTopic> topics = new ArrayList<>(topicNames.size());
        for (String topic : topicNames) {
            topics.add(buildTopic(topic, cache, manager));
        }
        MetadataResponseData.MetadataResponseTopicCollection topicCollection =
                new MetadataResponseData.MetadataResponseTopicCollection(topics.size());
        topicCollection.addAll(topics);
        response.setTopics(topicCollection);
        return response;
    }

    /** Build a {@link DescribeClusterResponseData} for the given listener name. */
    public DescribeClusterResponseData buildDescribeClusterResponse() {
        TabletServerMetadataCache cache = context.metadataCache();

        DescribeClusterResponseData response = new DescribeClusterResponseData();
        response.setThrottleTimeMs(0);
        response.setClusterId(context.clusterId());
        response.setControllerId(resolveControllerId(cache));
        response.setErrorCode(Errors.NONE.code());

        DescribeClusterResponseData.DescribeClusterBrokerCollection brokers =
                new DescribeClusterResponseData.DescribeClusterBrokerCollection();
        for (ServerNode node : cache.getAllAliveTabletServers(kafkaListenerName).values()) {
            brokers.add(
                    new DescribeClusterResponseData.DescribeClusterBroker()
                            .setBrokerId(node.id())
                            .setHost(node.host())
                            .setPort(node.port())
                            .setRack(node.rack()));
        }
        response.setBrokers(brokers);
        return response;
    }

    private MetadataResponseData.MetadataResponseBrokerCollection buildBrokers(
            TabletServerMetadataCache cache) {
        MetadataResponseData.MetadataResponseBrokerCollection brokers =
                new MetadataResponseData.MetadataResponseBrokerCollection();
        for (ServerNode node : cache.getAllAliveTabletServers(kafkaListenerName).values()) {
            brokers.add(
                    new MetadataResponseBroker()
                            .setNodeId(node.id())
                            .setHost(node.host())
                            .setPort(node.port())
                            .setRack(node.rack()));
        }
        return brokers;
    }

    private int resolveControllerId(TabletServerMetadataCache cache) {
        ServerNode coordinator = cache.getCoordinatorServer(kafkaListenerName);
        if (coordinator != null) {
            return coordinator.id();
        }
        // Operators typically don't advertise the Fluss coordinator on the KAFKA listener.
        // To keep AdminClient routing of admin ops (CreateTopics, DeleteTopics, ...) working we
        // claim the lowest-id alive tablet server as the Kafka controller. Tablet servers forward
        // admin RPCs to the real Fluss coordinator via coordinatorGateway, so this indirection is
        // transparent to the client.
        java.util.Map<Integer, ServerNode> alive =
                cache.getAllAliveTabletServers(kafkaListenerName);
        if (alive.isEmpty()) {
            LOG.warn(
                    "No alive tablet server on KAFKA listener '{}'; reporting controllerId=-1.",
                    kafkaListenerName);
            return -1;
        }
        int lowest = Integer.MAX_VALUE;
        for (Integer id : alive.keySet()) {
            if (id < lowest) {
                lowest = id;
            }
        }
        return lowest;
    }

    private List<String> resolveRequestedTopics(MetadataRequest request, MetadataManager manager) {
        if (request.isAllTopics()) {
            return discoverAllTopics(manager);
        }
        List<String> requested = request.topics();
        if (requested == null || requested.isEmpty()) {
            return Collections.emptyList();
        }
        // Preserve client-supplied order, deduplicate.
        return new ArrayList<>(new LinkedHashSet<>(requested));
    }

    private List<String> discoverAllTopics(MetadataManager manager) {
        List<KafkaTopicInfo> bindings;
        try {
            bindings = catalog.listAll();
        } catch (Exception e) {
            LOG.error("Failed to list Kafka bindings from catalog; returning empty topic list", e);
            return Collections.emptyList();
        }

        List<String> topics = new ArrayList<>(bindings.size());
        for (KafkaTopicInfo info : bindings) {
            TablePath path = info.dataTablePath();
            TableInfo table;
            try {
                table = manager.getTable(path);
            } catch (TableNotExistException removed) {
                continue;
            }
            if (table.isPartitioned()) {
                Map<String, Long> partitions;
                try {
                    partitions = manager.listPartitionIds(path);
                } catch (TableNotExistException removed) {
                    continue;
                }
                for (String partitionName : partitions.keySet()) {
                    String topic = KafkaTopicMapping.topicFor(info.topic(), partitionName);
                    if (!KafkaTopicMapping.isValidTopicName(topic)) {
                        LOG.warn(
                                "Hiding Fluss partition '{}' of table '{}' from Kafka metadata: "
                                        + "rendered topic name '{}' violates Kafka's topic regex.",
                                partitionName,
                                path,
                                topic);
                        continue;
                    }
                    topics.add(topic);
                }
            } else {
                topics.add(info.topic());
            }
        }
        return topics;
    }

    private MetadataResponseTopic buildTopic(
            String topic, TabletServerMetadataCache cache, MetadataManager manager) {
        MetadataResponseTopic responseTopic =
                new MetadataResponseTopic()
                        .setName(topic)
                        .setTopicId(Uuid.ZERO_UUID)
                        .setIsInternal(false);

        KafkaTopicMapping.ResolvedTopic resolved = topicMapping.resolve(topic);
        if (resolved == null) {
            return responseTopic
                    .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setPartitions(Collections.emptyList());
        }

        TableInfo info;
        try {
            info = manager.getTable(resolved.tablePath());
        } catch (TableNotExistException notFound) {
            return responseTopic
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setPartitions(Collections.emptyList());
        }

        if (info.isPartitioned() != resolved.isPartitioned()) {
            // Bare name on a partitioned table, or dotted name on a non-partitioned table.
            return responseTopic
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setPartitions(Collections.emptyList());
        }

        Map<Integer, BucketMetadata> buckets;
        if (resolved.isPartitioned()) {
            Long partitionId = resolvePartitionId(manager, resolved);
            if (partitionId == null) {
                return responseTopic
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setPartitions(Collections.emptyList());
            }
            buckets =
                    cache.getPartitionMetadata(
                                    org.apache.fluss.metadata.PhysicalTablePath.of(
                                            resolved.tablePath(), resolved.partitionName()))
                            .map(pm -> toBucketMap(pm.getBucketMetadataList()))
                            .orElse(Collections.emptyMap());
        } else {
            buckets =
                    cache.getTableMetadata(resolved.tablePath())
                            .map(tm -> toBucketMap(tm.getBucketMetadataList()))
                            .orElse(Collections.emptyMap());
        }

        Set<Integer> aliveServerIds = cache.getAllAliveTabletServers(kafkaListenerName).keySet();
        int numBuckets = info.getNumBuckets();
        List<MetadataResponsePartition> partitions = new ArrayList<>(numBuckets);
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            partitions.add(buildPartition(bucketId, buckets.get(bucketId), aliveServerIds));
        }
        return responseTopic.setErrorCode(Errors.NONE.code()).setPartitions(partitions);
    }

    @Nullable
    private Long resolvePartitionId(
            MetadataManager manager, KafkaTopicMapping.ResolvedTopic resolved) {
        try {
            return manager.listPartitionIds(resolved.tablePath()).get(resolved.partitionName());
        } catch (TableNotExistException removed) {
            return null;
        }
    }

    private MetadataResponsePartition buildPartition(
            int bucketId, @Nullable BucketMetadata bucket, Set<Integer> aliveServerIds) {
        MetadataResponsePartition partition =
                new MetadataResponsePartition().setPartitionIndex(bucketId);

        if (bucket == null || !bucket.getLeaderId().isPresent()) {
            return partition
                    .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                    .setLeaderId(-1)
                    .setLeaderEpoch(-1)
                    .setReplicaNodes(
                            bucket == null ? Collections.emptyList() : bucket.getReplicas())
                    .setIsrNodes(Collections.emptyList())
                    .setOfflineReplicas(Collections.emptyList());
        }

        int leaderId = bucket.getLeaderId().getAsInt();
        int leaderEpoch = bucket.getLeaderEpoch().orElse(-1);
        List<Integer> replicas = bucket.getReplicas();
        List<Integer> offline =
                replicas.stream()
                        .filter(id -> !aliveServerIds.contains(id))
                        .collect(Collectors.toList());
        // ISR is not tracked in the bucket cache today; treat all live replicas as in-sync.
        // Remote consumers tolerate an approximation; Fluss ISR lives on the coordinator.
        List<Integer> isr =
                replicas.stream().filter(aliveServerIds::contains).collect(Collectors.toList());

        return partition
                .setErrorCode(
                        aliveServerIds.contains(leaderId)
                                ? Errors.NONE.code()
                                : Errors.LEADER_NOT_AVAILABLE.code())
                .setLeaderId(aliveServerIds.contains(leaderId) ? leaderId : -1)
                .setLeaderEpoch(leaderEpoch)
                .setReplicaNodes(replicas)
                .setIsrNodes(isr)
                .setOfflineReplicas(offline);
    }

    private static Map<Integer, BucketMetadata> toBucketMap(List<BucketMetadata> list) {
        Map<Integer, BucketMetadata> map = new java.util.HashMap<>(list.size());
        for (BucketMetadata meta : list) {
            map.put(meta.getBucketId(), meta);
        }
        return map;
    }
}
