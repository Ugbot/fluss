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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.replica.ProducerStateSnapshot;
import org.apache.fluss.rpc.replica.ReplicaSnapshot;
import org.apache.fluss.server.replica.ReplicaManager;

import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.message.DescribeProducersResponseData.ProducerState;
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Handles Kafka {@code DESCRIBE_PRODUCERS} (API key 61) by projecting Fluss's {@code
 * WriterStateManager} state for each requested bucket.
 *
 * <p>Error mapping, per the Kafka protocol contract:
 *
 * <ul>
 *   <li>Unknown topic / partition — {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}.
 *   <li>Bucket not hosted locally — {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} (the client should
 *       re-issue against the correct broker after a metadata refresh).
 *   <li>Bucket hosted as follower, not leader — {@link Errors#NOT_LEADER_OR_FOLLOWER}.
 *   <li>Bucket hosted and leader — {@link Errors#NONE}; {@code activeProducers} lists one {@link
 *       ProducerState} per active writer (empty list when the bucket has never received an
 *       idempotent producer record).
 * </ul>
 */
@Internal
public final class KafkaDescribeProducersTranscoder {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaDescribeProducersTranscoder.class);

    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;

    public KafkaDescribeProducersTranscoder(
            KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.catalog = catalog;
        this.replicaManager = replicaManager;
    }

    public DescribeProducersResponseData describeProducers(DescribeProducersRequestData request) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();
        response.setThrottleTimeMs(0);
        List<TopicResponse> topicResults = new ArrayList<>();
        response.setTopics(topicResults);

        for (TopicRequest topic : request.topics()) {
            topicResults.add(buildTopicResult(topic));
        }
        return response;
    }

    private TopicResponse buildTopicResult(TopicRequest topic) {
        TopicResponse topicResult = new TopicResponse();
        topicResult.setName(topic.name());
        List<PartitionResponse> partitionResults = new ArrayList<>();
        topicResult.setPartitions(partitionResults);

        Optional<KafkaTopicInfo> info;
        try {
            info = catalog.lookup(topic.name());
        } catch (Exception e) {
            LOG.error("Catalog lookup failed for DescribeProducers on topic '{}'", topic.name(), e);
            info = Optional.empty();
        }
        if (!info.isPresent()) {
            for (Integer partitionId : partitionsOrEmpty(topic)) {
                partitionResults.add(
                        errorPartition(partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
            return topicResult;
        }

        long tableId = info.get().flussTableId();
        for (Integer partitionId : partitionsOrEmpty(topic)) {
            TableBucket bucket = new TableBucket(tableId, partitionId);
            partitionResults.add(buildPartitionResult(partitionId, bucket));
        }
        return topicResult;
    }

    private PartitionResponse buildPartitionResult(int partitionId, TableBucket bucket) {
        PartitionResponse pr = new PartitionResponse();
        pr.setPartitionIndex(partitionId);
        pr.setActiveProducers(Collections.emptyList());

        Optional<ReplicaSnapshot> maybeReplica = replicaManager.getReplicaSnapshot(bucket);
        if (!maybeReplica.isPresent()) {
            return pr.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(
                            "Bucket "
                                    + bucket
                                    + " is not hosted on this broker; refresh metadata and"
                                    + " retry against the bucket leader.");
        }
        if (!maybeReplica.get().isLeader()) {
            return pr.setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                    .setErrorMessage("Broker is not the leader for bucket " + bucket + ".");
        }

        Optional<List<ProducerStateSnapshot>> producers;
        try {
            producers = replicaManager.getProducerStates(bucket);
        } catch (Throwable t) {
            LOG.error("getProducerStates failed for {}", bucket, t);
            return pr.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(t.getClass().getSimpleName() + ": " + t.getMessage());
        }
        if (!producers.isPresent()) {
            return pr.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Bucket " + bucket + " is not hosted locally.");
        }

        List<ProducerState> wire = new ArrayList<>(producers.get().size());
        for (ProducerStateSnapshot s : producers.get()) {
            wire.add(
                    new ProducerState()
                            .setProducerId(s.producerId())
                            .setProducerEpoch(s.producerEpoch())
                            .setLastSequence(s.lastSequence())
                            .setLastTimestamp(s.lastTimestamp())
                            .setCoordinatorEpoch(s.coordinatorEpoch())
                            .setCurrentTxnStartOffset(s.currentTxnStartOffset()));
        }
        return pr.setErrorCode(Errors.NONE.code()).setActiveProducers(wire);
    }

    private static PartitionResponse errorPartition(int partitionId, Errors error) {
        return new PartitionResponse()
                .setPartitionIndex(partitionId)
                .setActiveProducers(Collections.emptyList())
                .setErrorCode(error.code())
                .setErrorMessage(error.message());
    }

    private static List<Integer> partitionsOrEmpty(TopicRequest topic) {
        List<Integer> p = topic.partitionIndexes();
        return p == null ? Collections.emptyList() : p;
    }
}
