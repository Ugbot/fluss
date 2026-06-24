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

package org.apache.fluss.kafka.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.kafka.KafkaServerContext;

import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartitions;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopics;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Transcoders for Kafka consumer-group coordination APIs. Phase 2D supports the minimum a Kafka
 * consumer in assign mode with manual commit/fetch-offset needs.
 */
@Internal
public final class KafkaGroupTranscoder {

    private final KafkaServerContext context;
    private final OffsetStore offsets;
    private final KafkaGroupRegistry registry;
    private final String kafkaListenerName;

    public KafkaGroupTranscoder(
            KafkaServerContext context,
            OffsetStore offsets,
            KafkaGroupRegistry registry,
            String kafkaListenerName) {
        this.context = context;
        this.offsets = offsets;
        this.registry = registry;
        this.kafkaListenerName = kafkaListenerName;
    }

    // --------------------------- FindCoordinator ---------------------------

    public FindCoordinatorResponseData findCoordinator(FindCoordinatorRequestData request) {
        FindCoordinatorResponseData response = new FindCoordinatorResponseData();
        response.setThrottleTimeMs(0);

        Optional<ServerNode> maybeCoordinator = pickCoordinator();

        // v0-v3 used a single key; v4+ uses coordinatorKeys with one entry per key.
        List<String> keys = request.coordinatorKeys();
        if (keys == null || keys.isEmpty()) {
            String singleKey = request.key();
            populateSingleCoordinator(response, maybeCoordinator, singleKey);
            return response;
        }

        for (String key : keys) {
            Coordinator coord = new Coordinator().setKey(key);
            if (maybeCoordinator.isPresent()) {
                ServerNode node = maybeCoordinator.get();
                coord.setNodeId(node.id())
                        .setHost(node.host())
                        .setPort(node.port())
                        .setErrorCode(Errors.NONE.code());
            } else {
                coord.setNodeId(-1)
                        .setHost("")
                        .setPort(-1)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
            }
            response.coordinators().add(coord);
        }
        return response;
    }

    private void populateSingleCoordinator(
            FindCoordinatorResponseData response,
            Optional<ServerNode> maybeCoordinator,
            String key) {
        if (!maybeCoordinator.isPresent()) {
            response.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("No Kafka-facing tablet server available")
                    .setNodeId(-1)
                    .setHost("")
                    .setPort(-1);
            return;
        }
        ServerNode node = maybeCoordinator.get();
        response.setErrorCode(Errors.NONE.code())
                .setNodeId(node.id())
                .setHost(node.host())
                .setPort(node.port());
    }

    /**
     * Picks a coordinator. In Phase 2D every group/transaction is routed to the lowest-id alive
     * tablet server so that the in-memory offset store stays consistent. Kafka's real partitioning
     * (hash(groupId) mod __consumer_offsets__ partitions) arrives when the offset store migrates to
     * a Fluss PK table.
     */
    private Optional<ServerNode> pickCoordinator() {
        Map<Integer, ServerNode> alive =
                context.metadataCache().getAllAliveTabletServers(kafkaListenerName);
        if (alive.isEmpty()) {
            return Optional.empty();
        }
        int lowest = Integer.MAX_VALUE;
        ServerNode picked = null;
        for (Map.Entry<Integer, ServerNode> e : alive.entrySet()) {
            if (e.getKey() < lowest) {
                lowest = e.getKey();
                picked = e.getValue();
            }
        }
        return Optional.ofNullable(picked);
    }

    // --------------------------- OffsetCommit ------------------------------

    public OffsetCommitResponseData offsetCommit(OffsetCommitRequestData request) {
        OffsetCommitResponseData response = new OffsetCommitResponseData();
        response.setThrottleTimeMs(0);

        String groupId = request.groupId();
        for (OffsetCommitRequestTopic topic : request.topics()) {
            OffsetCommitResponseTopic topicResp =
                    new OffsetCommitResponseTopic().setName(topic.name());
            response.topics().add(topicResp);
            for (OffsetCommitRequestPartition p : topic.partitions()) {
                OffsetCommitResponsePartition partResp =
                        new OffsetCommitResponsePartition().setPartitionIndex(p.partitionIndex());
                try {
                    offsets.commit(
                            groupId,
                            topic.name(),
                            p.partitionIndex(),
                            p.committedOffset(),
                            p.committedLeaderEpoch(),
                            p.committedMetadata());
                    partResp.setErrorCode(Errors.NONE.code());
                } catch (Exception e) {
                    partResp.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
                }
                topicResp.partitions().add(partResp);
            }
        }
        return response;
    }

    // --------------------------- OffsetFetch -------------------------------

    public OffsetFetchResponseData offsetFetch(OffsetFetchRequestData request) {
        OffsetFetchResponseData response = new OffsetFetchResponseData();
        response.setThrottleTimeMs(0);

        // v0-v7 uses groupId + topics on the top-level request.
        if (request.groupId() != null && !request.groupId().isEmpty()) {
            response.setErrorCode(Errors.NONE.code());
            populateV0Topics(response, request.groupId(), request.topics());
            return response;
        }

        // v8+ uses the nested groups[] structure with per-group results.
        for (OffsetFetchRequestGroup group : request.groups()) {
            response.groups().add(buildGroupResponse(group));
        }
        return response;
    }

    private void populateV0Topics(
            OffsetFetchResponseData response,
            String groupId,
            List<OffsetFetchRequestTopic> topics) {
        if (topics == null) {
            return;
        }
        for (OffsetFetchRequestTopic topic : topics) {
            OffsetFetchResponseTopic topicResp =
                    new OffsetFetchResponseTopic().setName(topic.name());
            response.topics().add(topicResp);
            for (Integer partition : topic.partitionIndexes()) {
                topicResp.partitions().add(buildV0Partition(groupId, topic.name(), partition));
            }
        }
    }

    private OffsetFetchResponsePartition buildV0Partition(
            String groupId, String topic, int partition) {
        OffsetFetchResponsePartition resp =
                new OffsetFetchResponsePartition().setPartitionIndex(partition);
        Optional<OffsetStore.CommittedOffset> committed;
        try {
            committed = offsets.fetch(groupId, topic, partition);
        } catch (Exception e) {
            return resp.setCommittedOffset(-1L)
                    .setCommittedLeaderEpoch(-1)
                    .setMetadata(null)
                    .setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
        }
        if (!committed.isPresent()) {
            return resp.setCommittedOffset(-1L)
                    .setCommittedLeaderEpoch(-1)
                    .setMetadata(null)
                    .setErrorCode(Errors.NONE.code());
        }
        OffsetStore.CommittedOffset co = committed.get();
        return resp.setCommittedOffset(co.offset())
                .setCommittedLeaderEpoch(co.leaderEpoch())
                .setMetadata(co.metadata())
                .setErrorCode(Errors.NONE.code());
    }

    private OffsetFetchResponseGroup buildGroupResponse(OffsetFetchRequestGroup group) {
        OffsetFetchResponseGroup groupResp =
                new OffsetFetchResponseGroup().setGroupId(group.groupId());
        groupResp.setErrorCode(Errors.NONE.code());
        if (group.topics() == null) {
            return groupResp;
        }
        for (OffsetFetchRequestTopics topic : group.topics()) {
            OffsetFetchResponseTopics topicResp =
                    new OffsetFetchResponseTopics().setName(topic.name());
            groupResp.topics().add(topicResp);
            for (Integer partition : topic.partitionIndexes()) {
                topicResp
                        .partitions()
                        .add(buildV8Partition(group.groupId(), topic.name(), partition));
            }
        }
        return groupResp;
    }

    private OffsetFetchResponsePartitions buildV8Partition(
            String groupId, String topic, int partition) {
        OffsetFetchResponsePartitions resp =
                new OffsetFetchResponsePartitions().setPartitionIndex(partition);
        Optional<OffsetStore.CommittedOffset> committed;
        try {
            committed = offsets.fetch(groupId, topic, partition);
        } catch (Exception e) {
            return resp.setCommittedOffset(-1L)
                    .setCommittedLeaderEpoch(-1)
                    .setMetadata(null)
                    .setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
        }
        if (!committed.isPresent()) {
            return resp.setCommittedOffset(-1L)
                    .setCommittedLeaderEpoch(-1)
                    .setMetadata(null)
                    .setErrorCode(Errors.NONE.code());
        }
        OffsetStore.CommittedOffset co = committed.get();
        return resp.setCommittedOffset(co.offset())
                .setCommittedLeaderEpoch(co.leaderEpoch())
                .setMetadata(co.metadata())
                .setErrorCode(Errors.NONE.code());
    }

    // --------------------------- ListGroups / DescribeGroups ---------------

    /**
     * Phase 2D minimum: list the groups we know about from the in-memory offset store. Group type
     * is always {@code consumer}; state is {@code Empty} because we don't track live membership
     * yet. Kafka tooling (e.g. {@code kafka-consumer-groups.sh --list}) accepts this shape.
     */
    public ListGroupsResponseData listGroups(ListGroupsRequestData request) {
        ListGroupsResponseData response = new ListGroupsResponseData();
        response.setThrottleTimeMs(0).setErrorCode(Errors.NONE.code());

        Set<String> statesFilter =
                request.statesFilter() == null
                        ? Collections.emptySet()
                        : new java.util.HashSet<>(request.statesFilter());
        Set<String> groupIds;
        try {
            groupIds = offsets.knownGroupIds();
        } catch (Exception e) {
            response.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
            return response;
        }
        for (String groupId : groupIds) {
            String state = "Empty";
            if (!statesFilter.isEmpty() && !statesFilter.contains(state)) {
                continue;
            }
            response.groups()
                    .add(
                            new ListedGroup()
                                    .setGroupId(groupId)
                                    .setProtocolType("consumer")
                                    .setGroupState(state));
        }
        return response;
    }

    /**
     * Phase 2D minimum: describe each requested group. We report {@code Empty} state and no
     * members; callers that care about live membership drive it off of the offsets we commit.
     */
    public DescribeGroupsResponseData describeGroups(DescribeGroupsRequestData request) {
        DescribeGroupsResponseData response = new DescribeGroupsResponseData();
        response.setThrottleTimeMs(0);

        for (String groupId : request.groups()) {
            DescribedGroup dg = new DescribedGroup().setGroupId(groupId);
            boolean exists;
            try {
                exists = offsets.groupExists(groupId);
            } catch (Exception e) {
                dg.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code())
                        .setGroupState("Dead")
                        .setProtocolType("")
                        .setProtocolData("")
                        .setMembers(Collections.emptyList());
                response.groups().add(dg);
                continue;
            }
            if (!exists) {
                dg.setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                        .setGroupState("Dead")
                        .setProtocolType("")
                        .setProtocolData("")
                        .setMembers(Collections.emptyList());
            } else {
                dg.setErrorCode(Errors.NONE.code())
                        .setGroupState("Empty")
                        .setProtocolType("consumer")
                        .setProtocolData("")
                        .setMembers(Collections.emptyList());
            }
            response.groups().add(dg);
        }
        return response;
    }

    // --------------------------- DeleteGroups ------------------------------

    /**
     * Delete consumer groups: drop in-memory coordinator state and erase all committed offsets for
     * each requested groupId. A group that still has live members is refused with {@code
     * NON_EMPTY_GROUP}; an unknown group (no members and no committed offsets) is refused with
     * {@code GROUP_ID_NOT_FOUND}. The {@link KafkaGroupRegistry.GroupState} for a deleted group is
     * evicted so that a subsequent rejoin starts at generation 0.
     */
    public DeleteGroupsResponseData deleteGroups(DeleteGroupsRequestData request) {
        DeleteGroupsResponseData response = new DeleteGroupsResponseData();
        response.setThrottleTimeMs(0);
        DeletableGroupResultCollection results = new DeletableGroupResultCollection();

        List<String> groupNames =
                request.groupsNames() == null ? Collections.emptyList() : request.groupsNames();
        for (String groupId : groupNames) {
            DeletableGroupResult result = new DeletableGroupResult().setGroupId(groupId);

            KafkaGroupRegistry.GroupState state = registry.get(groupId);
            if (state != null && state.memberCount() > 0) {
                result.setErrorCode(Errors.NON_EMPTY_GROUP.code());
                results.add(result);
                continue;
            }

            boolean hadOffsets;
            try {
                hadOffsets = offsets.groupExists(groupId);
            } catch (Exception e) {
                result.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
                results.add(result);
                continue;
            }
            if (state == null && !hadOffsets) {
                result.setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
                results.add(result);
                continue;
            }

            try {
                offsets.deleteGroup(groupId);
            } catch (Exception e) {
                result.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
                results.add(result);
                continue;
            }
            // Evict in-memory coordinator state so generation resets on rejoin.
            registry.remove(groupId);
            result.setErrorCode(Errors.NONE.code());
            results.add(result);
        }
        response.setResults(results);
        return response;
    }

    // --------------------------- OffsetDelete ------------------------------

    /**
     * Delete committed offsets for the listed (topic, partition) pairs under a group. Per-partition
     * response codes mirror Kafka: {@code NONE} on success, a translated store error otherwise. If
     * the group has live members, the whole request is refused at the top level with {@code
     * NON_EMPTY_GROUP}; unknown groups return {@code GROUP_ID_NOT_FOUND}.
     */
    public OffsetDeleteResponseData offsetDelete(OffsetDeleteRequestData request) {
        OffsetDeleteResponseData response = new OffsetDeleteResponseData();
        response.setThrottleTimeMs(0);

        String groupId = request.groupId();
        KafkaGroupRegistry.GroupState state = registry.get(groupId);
        if (state != null && state.memberCount() > 0) {
            return response.setErrorCode(Errors.NON_EMPTY_GROUP.code());
        }

        boolean groupKnown;
        try {
            groupKnown = state != null || offsets.groupExists(groupId);
        } catch (Exception e) {
            return response.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
        }
        if (!groupKnown) {
            return response.setErrorCode(Errors.GROUP_ID_NOT_FOUND.code());
        }

        response.setErrorCode(Errors.NONE.code());
        OffsetDeleteResponseTopicCollection topicsResp = new OffsetDeleteResponseTopicCollection();
        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection topics = request.topics();
        if (topics != null) {
            for (OffsetDeleteRequestTopic topic : topics) {
                OffsetDeleteResponseTopic topicResp =
                        new OffsetDeleteResponseTopic().setName(topic.name());
                OffsetDeleteResponsePartitionCollection partsResp =
                        new OffsetDeleteResponsePartitionCollection();
                for (OffsetDeleteRequestPartition p : topic.partitions()) {
                    OffsetDeleteResponsePartition partResp =
                            new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(p.partitionIndex());
                    try {
                        offsets.delete(groupId, topic.name(), p.partitionIndex());
                        partResp.setErrorCode(Errors.NONE.code());
                    } catch (Exception e) {
                        partResp.setErrorCode(org.apache.fluss.kafka.KafkaErrors.toKafka(e).code());
                    }
                    partsResp.add(partResp);
                }
                topicResp.setPartitions(partsResp);
                topicsResp.add(topicResp);
            }
        }
        response.setTopics(topicsResp);
        return response;
    }

    // --------------------------- JoinGroup ---------------------------------

    public CompletableFuture<JoinGroupResponseData> joinGroup(JoinGroupRequestData request) {
        Map<String, byte[]> protocols = new HashMap<>();
        if (request.protocols() != null) {
            for (JoinGroupRequestProtocol p : request.protocols()) {
                protocols.put(p.name(), p.metadata() == null ? new byte[0] : p.metadata());
            }
        }

        KafkaGroupRegistry.GroupState state = registry.getOrCreate(request.groupId());
        int sessionTimeoutMs = Math.max(request.sessionTimeoutMs(), 1);
        long now = System.currentTimeMillis();
        return state.join(
                        request.memberId(),
                        request.protocolType(),
                        protocols,
                        request.groupInstanceId(),
                        sessionTimeoutMs,
                        now)
                .thenApply(
                        outcome -> {
                            JoinGroupResponseData response = new JoinGroupResponseData();
                            response.setThrottleTimeMs(0)
                                    .setErrorCode(Errors.NONE.code())
                                    .setGenerationId(outcome.generation())
                                    .setMemberId(outcome.memberId())
                                    .setLeader(
                                            outcome.leaderMemberId() == null
                                                    ? ""
                                                    : outcome.leaderMemberId())
                                    .setProtocolType(
                                            state.protocolType() == null
                                                    ? "consumer"
                                                    : state.protocolType())
                                    .setProtocolName(
                                            outcome.protocolName() == null
                                                    ? ""
                                                    : outcome.protocolName())
                                    .setSkipAssignment(false);
                            List<JoinGroupResponseMember> members = new java.util.ArrayList<>();
                            for (KafkaGroupRegistry.MemberMetadata m : outcome.members()) {
                                members.add(
                                        new JoinGroupResponseMember()
                                                .setMemberId(m.memberId())
                                                .setGroupInstanceId(m.groupInstanceId())
                                                .setMetadata(m.metadata()));
                            }
                            response.setMembers(members);
                            return response;
                        });
    }

    // --------------------------- SyncGroup ---------------------------------

    public CompletableFuture<SyncGroupResponseData> syncGroup(SyncGroupRequestData request) {
        KafkaGroupRegistry.GroupState state = registry.get(request.groupId());
        if (state == null) {
            SyncGroupResponseData response = new SyncGroupResponseData();
            return CompletableFuture.completedFuture(
                    response.setThrottleTimeMs(0)
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                            .setAssignment(new byte[0]));
        }

        Map<String, byte[]> assignments = null;
        if (request.assignments() != null && !request.assignments().isEmpty()) {
            assignments = new HashMap<>();
            for (SyncGroupRequestAssignment a : request.assignments()) {
                assignments.put(
                        a.memberId(), a.assignment() == null ? new byte[0] : a.assignment());
            }
        }

        return state.sync(request.memberId(), request.generationId(), assignments)
                .thenApply(
                        result -> {
                            SyncGroupResponseData response = new SyncGroupResponseData();
                            response.setThrottleTimeMs(0);
                            switch (result.code()) {
                                case OK:
                                    return response.setErrorCode(Errors.NONE.code())
                                            .setProtocolType(state.protocolType())
                                            .setProtocolName(state.protocolName())
                                            .setAssignment(result.assignment());
                                case UNKNOWN_MEMBER:
                                    return response.setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                                            .setAssignment(new byte[0]);
                                case ILLEGAL_GENERATION:
                                    return response.setErrorCode(Errors.ILLEGAL_GENERATION.code())
                                            .setAssignment(new byte[0]);
                                case REBALANCE_IN_PROGRESS:
                                default:
                                    return response.setErrorCode(
                                                    Errors.REBALANCE_IN_PROGRESS.code())
                                            .setAssignment(new byte[0]);
                            }
                        });
    }

    // --------------------------- Heartbeat ---------------------------------

    public HeartbeatResponseData heartbeat(HeartbeatRequestData request) {
        HeartbeatResponseData response = new HeartbeatResponseData();
        response.setThrottleTimeMs(0);

        KafkaGroupRegistry.GroupState state = registry.get(request.groupId());
        if (state == null) {
            return response.setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());
        }
        KafkaGroupRegistry.HeartbeatResult result =
                state.heartbeat(
                        request.memberId(), request.generationId(), System.currentTimeMillis());
        switch (result) {
            case OK:
                return response.setErrorCode(Errors.NONE.code());
            case UNKNOWN_MEMBER:
                return response.setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());
            case ILLEGAL_GENERATION:
                return response.setErrorCode(Errors.ILLEGAL_GENERATION.code());
            case REBALANCE_IN_PROGRESS:
            default:
                return response.setErrorCode(Errors.REBALANCE_IN_PROGRESS.code());
        }
    }

    // --------------------------- LeaveGroup --------------------------------

    public LeaveGroupResponseData leaveGroup(LeaveGroupRequestData request) {
        LeaveGroupResponseData response = new LeaveGroupResponseData();
        response.setThrottleTimeMs(0).setErrorCode(Errors.NONE.code());

        KafkaGroupRegistry.GroupState state = registry.get(request.groupId());
        if (state == null) {
            // Unknown group → still return NONE; the client treats this idempotently.
            return response;
        }

        // v0-v2 single member: use memberId directly.
        if (request.memberId() != null && !request.memberId().isEmpty()) {
            state.leave(request.memberId());
        }

        // v3+ batched members.
        if (request.members() != null && !request.members().isEmpty()) {
            for (MemberIdentity m : request.members()) {
                state.leave(m.memberId());
                response.members()
                        .add(
                                new MemberResponse()
                                        .setMemberId(m.memberId())
                                        .setGroupInstanceId(m.groupInstanceId())
                                        .setErrorCode(Errors.NONE.code()));
            }
        }
        return response;
    }
}
