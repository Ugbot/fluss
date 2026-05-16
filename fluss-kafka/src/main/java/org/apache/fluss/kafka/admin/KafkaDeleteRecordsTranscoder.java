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
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.replica.ReplicaSnapshot;
import org.apache.fluss.server.replica.ReplicaManager;

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Handles Kafka {@code DELETE_RECORDS} by advancing the Fluss log-start offset of each requested
 * bucket via {@link ReplicaManager#deleteRecords(TableBucket, long)}. A per-partition target offset
 * of {@link DeleteRecordsRequest#HIGH_WATERMARK} (i.e. {@code -1}) is interpreted as "trim up to
 * the current high-watermark", matching Kafka's broker semantics.
 */
@Internal
public final class KafkaDeleteRecordsTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDeleteRecordsTranscoder.class);

    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;

    public KafkaDeleteRecordsTranscoder(KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.catalog = catalog;
        this.replicaManager = replicaManager;
    }

    public DeleteRecordsResponseData deleteRecords(DeleteRecordsRequestData request) {
        DeleteRecordsResponseData response = new DeleteRecordsResponseData();
        response.setThrottleTimeMs(0);
        DeleteRecordsTopicResultCollection topicResults = new DeleteRecordsTopicResultCollection();
        response.setTopics(topicResults);

        for (DeleteRecordsTopic topic : request.topics()) {
            DeleteRecordsTopicResult topicResult =
                    new DeleteRecordsTopicResult().setName(topic.name());
            DeleteRecordsPartitionResultCollection partResults =
                    new DeleteRecordsPartitionResultCollection();
            topicResult.setPartitions(partResults);
            topicResults.add(topicResult);

            Optional<KafkaTopicInfo> info;
            try {
                info = catalog.lookup(topic.name());
            } catch (Exception e) {
                LOG.error("Catalog lookup failed for DeleteRecords of '{}'", topic.name(), e);
                info = Optional.empty();
            }
            if (!info.isPresent()) {
                for (DeleteRecordsPartition p : topic.partitions()) {
                    partResults.add(
                            new DeleteRecordsPartitionResult()
                                    .setPartitionIndex(p.partitionIndex())
                                    .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                continue;
            }

            long tableId = info.get().flussTableId();
            for (DeleteRecordsPartition p : topic.partitions()) {
                partResults.add(resolve(new TableBucket(tableId, p.partitionIndex()), p));
            }
        }
        return response;
    }

    private DeleteRecordsPartitionResult resolve(TableBucket bucket, DeleteRecordsPartition p) {
        DeleteRecordsPartitionResult result =
                new DeleteRecordsPartitionResult().setPartitionIndex(p.partitionIndex());

        Optional<ReplicaSnapshot> maybeSnapshot = replicaManager.getReplicaSnapshot(bucket);
        if (!maybeSnapshot.isPresent()) {
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        }
        ReplicaSnapshot snapshot = maybeSnapshot.get();

        long requestedOffset = p.offset();
        long targetOffset;
        if (requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK) {
            // Kafka sentinel: trim up to the current high-watermark.
            targetOffset = snapshot.logHighWatermark();
        } else if (requestedOffset < 0L) {
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code());
        } else {
            targetOffset = requestedOffset;
        }

        try {
            long newLowWatermark = replicaManager.deleteRecords(bucket, targetOffset);
            return result.setLowWatermark(newLowWatermark).setErrorCode(Errors.NONE.code());
        } catch (UnknownTableOrBucketException e) {
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        } catch (NotLeaderOrFollowerException e) {
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code());
        } catch (LogOffsetOutOfRangeException e) {
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code());
        } catch (Throwable t) {
            LOG.error(
                    "DeleteRecords for bucket {} (target offset {}) failed",
                    bucket,
                    targetOffset,
                    t);
            return result.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        }
    }
}
