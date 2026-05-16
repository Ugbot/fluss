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

import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Handles Kafka {@code OFFSET_FOR_LEADER_EPOCH} (API key 23).
 *
 * <p>Kafka consumers use this API after a leader change to ask "what was the last offset written
 * under leader epoch N?" so they can truncate anything they locally buffered past that point. Fluss
 * does track a {@code leaderEpoch} per bucket ({@link ReplicaSnapshot#leaderEpoch()}) but does not
 * currently materialise a per-offset epoch history the way Kafka does. Without that history we
 * implement the pragmatic contract that a kafka-clients 3.x consumer actually needs for stable
 * reassignment:
 *
 * <ul>
 *   <li>Queried epoch {@code ==} current leader epoch → return {@code (currentEpoch, HWM)}. HWM is
 *       the Fluss equivalent of Kafka's log-end-offset from the consumer's point of view (everyone
 *       read-up-to HWM).
 *   <li>Queried epoch {@code <} current leader epoch → return {@code (-1, -1)}. Kafka's {@code
 *       OffsetsForLeaderEpochClient} treats {@code endOffset == -1} as "broker doesn't have a
 *       record of that epoch, keep whatever you have locally", which is exactly the right behaviour
 *       when we can't prove a truncation is needed.
 *   <li>Queried epoch {@code >} current leader epoch → {@link Errors#FENCED_LEADER_EPOCH}. The
 *       consumer's view of metadata is ahead of ours, so it must refresh.
 *   <li>{@code CurrentLeaderEpoch} (v2+) newer than ours → {@link Errors#UNKNOWN_LEADER_EPOCH};
 *       older than ours → {@link Errors#FENCED_LEADER_EPOCH}. Matches Kafka's fencing protocol.
 *   <li>Unknown topic → {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}.
 *   <li>Bucket not locally hosted → {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} (broker has no
 *       replica). Bucket locally hosted but not leader → {@link Errors#NOT_LEADER_OR_FOLLOWER}.
 * </ul>
 *
 * <p>Because we don't persist the full epoch history, two real-but-tolerable behaviours differ from
 * Apache Kafka: we never report a truncation point for an older epoch, and we cannot answer for the
 * epoch immediately preceding a leader failover during the short window between the old leader
 * stepping down and its log being replayed. Consumers recover by re-subscribing.
 */
@Internal
public final class KafkaOffsetForLeaderEpochTranscoder {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaOffsetForLeaderEpochTranscoder.class);

    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;

    public KafkaOffsetForLeaderEpochTranscoder(
            KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.catalog = catalog;
        this.replicaManager = replicaManager;
    }

    public OffsetForLeaderEpochResponseData offsetForLeaderEpoch(
            OffsetForLeaderEpochRequestData request) {
        OffsetForLeaderEpochResponseData response = new OffsetForLeaderEpochResponseData();
        response.setThrottleTimeMs(0);
        OffsetForLeaderTopicResultCollection topics = new OffsetForLeaderTopicResultCollection();
        response.setTopics(topics);

        for (OffsetForLeaderTopic topic : request.topics()) {
            OffsetForLeaderTopicResult topicResult =
                    new OffsetForLeaderTopicResult().setTopic(topic.topic());
            topics.add(topicResult);

            Optional<KafkaTopicInfo> info;
            try {
                info = catalog.lookup(topic.topic());
            } catch (Exception e) {
                LOG.error(
                        "Catalog lookup failed for OffsetForLeaderEpoch of '{}'", topic.topic(), e);
                info = Optional.empty();
            }
            if (!info.isPresent()) {
                for (OffsetForLeaderPartition p : topic.partitions()) {
                    topicResult.partitions().add(unknownTopic(p.partition()));
                }
                continue;
            }

            long tableId = info.get().flussTableId();
            for (OffsetForLeaderPartition p : topic.partitions()) {
                topicResult.partitions().add(resolve(new TableBucket(tableId, p.partition()), p));
            }
        }
        return response;
    }

    private EpochEndOffset resolve(TableBucket bucket, OffsetForLeaderPartition p) {
        EpochEndOffset result =
                new EpochEndOffset()
                        .setPartition(p.partition())
                        .setLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                        .setEndOffset(-1L);

        Optional<ReplicaSnapshot> maybeSnapshot;
        try {
            maybeSnapshot = replicaManager.getReplicaSnapshot(bucket);
        } catch (Throwable t) {
            LOG.error("Failed to read replica snapshot for OffsetForLeaderEpoch {}", bucket, t);
            return result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        }
        if (!maybeSnapshot.isPresent()) {
            return result.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        }
        ReplicaSnapshot snapshot = maybeSnapshot.get();
        if (!snapshot.isLeader()) {
            return result.setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code());
        }

        int currentLeaderEpoch = snapshot.leaderEpoch();
        int clientCurrentLeaderEpoch = p.currentLeaderEpoch();
        // v2+ fencing: if the client's view of the leader epoch disagrees with ours, surface the
        // appropriate fencing error so it refreshes metadata. v0/v1 omit this field and default to
        // -1 (NO_PARTITION_LEADER_EPOCH) which means "client has no opinion, trust the broker".
        if (clientCurrentLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            if (clientCurrentLeaderEpoch > currentLeaderEpoch) {
                return result.setErrorCode(Errors.UNKNOWN_LEADER_EPOCH.code());
            }
            if (clientCurrentLeaderEpoch < currentLeaderEpoch) {
                return result.setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
            }
        }

        int requestedEpoch = p.leaderEpoch();
        if (requestedEpoch > currentLeaderEpoch) {
            // The client thinks the partition advanced past an epoch we've never seen; from its
            // perspective we are behind.
            return result.setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
        }
        if (requestedEpoch < currentLeaderEpoch) {
            // We don't track per-offset epoch history. Returning {-1, -1} tells the consumer
            // "no truncation point available", which is the conservative choice that keeps the
            // Kafka client's buffered data intact across a no-op leader bounce.
            return result.setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                    .setEndOffset(-1L);
        }
        // Current epoch: the end-of-epoch offset is the current HWM — all records up to HWM were
        // produced under this leader (no prior epoch could have written past the HWM locally held
        // by this leader, and writes beyond HWM are uncommitted).
        return result.setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(currentLeaderEpoch)
                .setEndOffset(snapshot.logHighWatermark());
    }

    private static EpochEndOffset unknownTopic(int partition) {
        return new EpochEndOffset()
                .setPartition(partition)
                .setLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setEndOffset(-1L)
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }
}
