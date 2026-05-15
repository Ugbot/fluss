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
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadataProvider;

import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handles Kafka {@code ELECT_LEADERS} (API key 43) by inspecting each bucket's replica assignment
 * and reporting whether a preferred-replica election is needed.
 *
 * <p><b>Phase I.1 — honest stub.</b> Fluss core does not yet expose a public preferred-leader
 * election primitive: leader transitions are driven implicitly by the coordinator's {@code
 * TableBucketStateMachine} through the package-private {@code ReplicaLeaderElection} strategies
 * (see {@code fluss-server/.../coordinator/statemachine/ReplicaLeaderElection.java}). Landing a
 * real election primitive requires a new {@code CoordinatorGateway.electLeaders} RPC plus a new
 * {@code PreferredReplicaPartitionLeaderElection} strategy — roughly 400 LOC of core changes
 * documented in {@code dev-docs/design/0011-kafka-admin-polish.md} §3 / §9. This transcoder
 * therefore implements the read-only half of the contract and surfaces the stub case explicitly:
 *
 * <ul>
 *   <li>Unknown topic / partition — {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}.
 *   <li>Preferred replica (first entry in the assignment list, Fluss convention) already the
 *       current leader — {@link Errors#ELECTION_NOT_NEEDED}. Kafka AdminClient tools treat this as
 *       "no-op, cluster balanced".
 *   <li>Preferred replica differs from current leader — {@link
 *       Errors#PREFERRED_LEADER_NOT_AVAILABLE}. The error message explicitly names the missing core
 *       primitive so Cruise Control / {@code kafka-leader-election.sh} operators do not silently
 *       believe a re-balance completed. Once Fluss exposes a public election primitive, this branch
 *       should be replaced by the actual RPC and the error reserved for "election fired but could
 *       not complete" (preferred replica not in ISR, coordinator timeout).
 *   <li>Unclean election type (electionType=1) — {@link Errors#INVALID_REQUEST}. Fluss has no
 *       analogue for unclean leader election and will not add one; refuse explicitly.
 * </ul>
 *
 * <p>When the caller omits {@code topicPartitions} entirely (Kafka convention: "elect everything"),
 * we short-circuit and advertise {@link Errors#NONE} with an empty per-topic list. kafka-clients'
 * {@code DescribeClusterOptions} / {@code AdminClient.electLeaders(PREFERRED, null)} accepts that
 * shape as a no-op, and {@code kafka-leader-election.sh --all-topic-partitions} falls back to
 * per-partition calls. Surfacing a wire error on the "elect everything" shape would break Cruise
 * Control's bootstrap path.
 */
@Internal
public final class KafkaElectLeadersTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaElectLeadersTranscoder.class);

    /** Kafka's ElectionType enum value for PREFERRED. */
    private static final byte ELECTION_TYPE_PREFERRED = 0;

    /** Kafka's ElectionType enum value for UNCLEAN. */
    private static final byte ELECTION_TYPE_UNCLEAN = 1;

    private final KafkaTopicsCatalog catalog;
    private final ClusterMetadataProvider metadataCache;

    public KafkaElectLeadersTranscoder(
            KafkaTopicsCatalog catalog, ClusterMetadataProvider metadataCache) {
        this.catalog = catalog;
        this.metadataCache = metadataCache;
    }

    public ElectLeadersResponseData electLeaders(ElectLeadersRequestData request) {
        ElectLeadersResponseData response = new ElectLeadersResponseData();
        response.setThrottleTimeMs(0);
        response.setErrorCode(Errors.NONE.code());
        List<ReplicaElectionResult> topicResults = new ArrayList<>();
        response.setReplicaElectionResults(topicResults);

        byte electionType = request.electionType();
        if (electionType == ELECTION_TYPE_UNCLEAN) {
            // Unclean election = "pick any replica even if it lost data". Fluss has no analogue
            // here; refuse explicitly rather than silently succeed.
            response.setErrorCode(Errors.INVALID_REQUEST.code());
            return response;
        }
        if (electionType != ELECTION_TYPE_PREFERRED) {
            response.setErrorCode(Errors.INVALID_REQUEST.code());
            return response;
        }

        ElectLeadersRequestData.TopicPartitionsCollection topicPartitions =
                request.topicPartitions();
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            // "Elect everything" — treat as a no-op (empty per-topic list) rather than scanning
            // the whole catalog; kafka-clients accepts that shape and reports success.
            return response;
        }

        for (TopicPartitions topic : topicPartitions) {
            topicResults.add(buildTopicResult(topic));
        }
        return response;
    }

    private ReplicaElectionResult buildTopicResult(TopicPartitions topic) {
        ReplicaElectionResult topicResult = new ReplicaElectionResult();
        topicResult.setTopic(topic.topic());
        List<PartitionResult> partitionResults = new ArrayList<>();
        topicResult.setPartitionResult(partitionResults);

        Optional<KafkaTopicInfo> info;
        try {
            info = catalog.lookup(topic.topic());
        } catch (Exception e) {
            LOG.error("Catalog lookup failed for ElectLeaders on topic '{}'", topic.topic(), e);
            info = Optional.empty();
        }
        if (!info.isPresent()) {
            for (Integer p : partitionsOrEmpty(topic)) {
                partitionResults.add(errorPartition(p, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
            return topicResult;
        }

        Map<Integer, BucketMetadata> buckets =
                metadataCache
                        .getTableMetadata(info.get().dataTablePath())
                        .map(
                                tm -> {
                                    Map<Integer, BucketMetadata> out = new HashMap<>();
                                    for (BucketMetadata bm : tm.getBucketMetadataList()) {
                                        out.put(bm.getBucketId(), bm);
                                    }
                                    return out;
                                })
                        .orElse(Collections.emptyMap());
        // Partitioned Fluss tables aren't currently exposed as Kafka partitions on this path, so
        // only the non-partitioned table metadata is consulted above. Phase I+ extensions that
        // surface partitioned topics should fall back to
        // ClusterMetadataProvider#getPartitionMetadata here.
        for (Integer partitionId : partitionsOrEmpty(topic)) {
            partitionResults.add(classifyPartition(partitionId, buckets.get(partitionId)));
        }
        return topicResult;
    }

    private static PartitionResult classifyPartition(int partitionId, BucketMetadata bucket) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionId(partitionId);
        if (bucket == null || !bucket.getLeaderId().isPresent()) {
            return pr.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Partition " + partitionId + " has no leader assigned.");
        }
        List<Integer> replicas = bucket.getReplicas();
        if (replicas == null || replicas.isEmpty()) {
            return pr.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("Partition " + partitionId + " has no replica assignment.");
        }
        int preferred = replicas.get(0);
        int currentLeader = bucket.getLeaderId().getAsInt();
        if (preferred == currentLeader) {
            return pr.setErrorCode(Errors.ELECTION_NOT_NEEDED.code()).setErrorMessage(null);
        }
        // Fluss core has no public preferred-leader-election primitive yet (see class Javadoc).
        // Surface the Kafka wire-level error that signals "preferred replica exists but isn't
        // currently the leader" with an explicit, greppable message so operators see the stub
        // status rather than believe a no-op rebalance fired. AdminClient treats the error as
        // transient and will surface it as PreferredLeaderNotAvailableException on .all().
        return pr.setErrorCode(Errors.PREFERRED_LEADER_NOT_AVAILABLE.code())
                .setErrorMessage(
                        "Preferred replica "
                                + preferred
                                + " is not the current leader ("
                                + currentLeader
                                + "); Fluss does not yet expose a public preferred-leader"
                                + " election primitive (see design 0011 §3).");
    }

    private static PartitionResult errorPartition(int partitionId, Errors error) {
        return new PartitionResult()
                .setPartitionId(partitionId)
                .setErrorCode(error.code())
                .setErrorMessage(error.message());
    }

    private static List<Integer> partitionsOrEmpty(TopicPartitions tp) {
        List<Integer> p = tp.partitions();
        return p == null ? Collections.emptyList() : p;
    }
}
