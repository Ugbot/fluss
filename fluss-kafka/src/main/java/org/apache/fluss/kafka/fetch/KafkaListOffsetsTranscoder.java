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

package org.apache.fluss.kafka.fetch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.replica.ReplicaSnapshot;
import org.apache.fluss.server.replica.ReplicaManager;

import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Handles Kafka {@code LIST_OFFSETS} by resolving EARLIEST to the Fluss log-start offset and LATEST
 * to the Fluss high-watermark. Timestamp-based lookups are not yet supported.
 */
@Internal
public final class KafkaListOffsetsTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaListOffsetsTranscoder.class);

    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;

    public KafkaListOffsetsTranscoder(KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.catalog = catalog;
        this.replicaManager = replicaManager;
    }

    public ListOffsetsResponseData listOffsets(ListOffsetsRequestData request) {
        ListOffsetsResponseData response = new ListOffsetsResponseData();
        response.setThrottleTimeMs(0);

        for (ListOffsetsTopic topic : request.topics()) {
            ListOffsetsTopicResponse topicResp =
                    new ListOffsetsTopicResponse().setName(topic.name());
            response.topics().add(topicResp);

            Optional<KafkaTopicInfo> info;
            try {
                info = catalog.lookup(topic.name());
            } catch (Exception e) {
                LOG.error("Catalog lookup failed for ListOffsets of '{}'", topic.name(), e);
                info = Optional.empty();
            }
            if (!info.isPresent()) {
                for (ListOffsetsPartition p : topic.partitions()) {
                    topicResp
                            .partitions()
                            .add(
                                    new ListOffsetsPartitionResponse()
                                            .setPartitionIndex(p.partitionIndex())
                                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                            .setOffset(-1L)
                                            .setTimestamp(-1L));
                }
                continue;
            }

            long tableId = info.get().flussTableId();
            for (ListOffsetsPartition p : topic.partitions()) {
                topicResp
                        .partitions()
                        .add(resolve(new TableBucket(tableId, p.partitionIndex()), p));
            }
        }
        return response;
    }

    private ListOffsetsPartitionResponse resolve(TableBucket bucket, ListOffsetsPartition p) {
        ListOffsetsPartitionResponse resp =
                new ListOffsetsPartitionResponse().setPartitionIndex(p.partitionIndex());
        long requestedTs = p.timestamp();

        Optional<ReplicaSnapshot> maybeSnapshot = replicaManager.getReplicaSnapshot(bucket);
        if (!maybeSnapshot.isPresent()) {
            return resp.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setOffset(-1L)
                    .setTimestamp(-1L);
        }
        ReplicaSnapshot snapshot = maybeSnapshot.get();

        long offset;
        if (requestedTs == ListOffsetsRequest.EARLIEST_TIMESTAMP
                || requestedTs == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            offset = snapshot.logStartOffset();
        } else {
            // LATEST_TIMESTAMP (-1), LATEST_TIERED_TIMESTAMP (-5), MAX_TIMESTAMP (-3), and any
            // positive epoch-millis timestamp all resolve to the local log end offset. Fluss
            // doesn't index per-message timestamps, so we can't return the exact offset of the
            // first record at-or-after `requestedTs` — returning LSO is the documented
            // "best-effort" semantic that kafka-clients accept (the consumer falls back to
            // poll-from-end behaviour). Returning UNSUPPORTED_VERSION here breaks librdkafka.
            offset = snapshot.localLogEndOffset();
        }

        return resp.setErrorCode(Errors.NONE.code())
                .setOffset(offset)
                .setTimestamp(-1L)
                .setLeaderEpoch(snapshot.leaderEpoch());
    }
}
