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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory consumer-group state machine. Tracks membership, generation, leader, protocol, pending
 * assignments, and drives the Kafka rebalance protocol for multi-consumer groups.
 *
 * <p>Rebalance model:
 *
 * <ul>
 *   <li>A new member joining, or a session timeout reaping a member, bumps the generation and marks
 *       every surviving member as "not yet rejoined at the new gen".
 *   <li>Existing members discover this via {@code Heartbeat} responses returning {@code
 *       REBALANCE_IN_PROGRESS}, and re-send {@code JoinGroup} with their known {@code memberId}.
 *   <li>Once every current member is at the current generation, any pending {@code JoinGroup}
 *       futures complete — the leader's with the full member roster, followers' with empty rosters.
 *   <li>Followers' {@code SyncGroup} calls block on a per-member future until the leader submits
 *       assignments; on submission all pending futures complete with their share.
 * </ul>
 *
 * <p>Session timeouts are driven by {@link #reapExpired(long)} which external callers (e.g. a
 * scheduled reaper thread) invoke periodically. Members whose {@code lastSeenMs} has fallen behind
 * {@code sessionTimeoutMs} are dropped and trigger a generation bump.
 *
 * <p>Per-group mutations are serialised on the {@link GroupState} monitor.
 */
@Internal
public final class KafkaGroupRegistry {

    private final ConcurrentHashMap<String, GroupState> groups = new ConcurrentHashMap<>();

    public GroupState getOrCreate(String groupId) {
        return groups.computeIfAbsent(groupId, GroupState::new);
    }

    @Nullable
    public GroupState get(String groupId) {
        return groups.get(groupId);
    }

    /**
     * Forget all in-memory state for {@code groupId} (coordinator membership, generation, pending
     * assignments). Callers must have already verified the group has no live members; used by
     * {@code DELETE_GROUPS} so that a subsequent rejoin starts at generation 0.
     */
    public void remove(String groupId) {
        groups.remove(groupId);
    }

    /**
     * Reap expired members across every group. Returns the total number of members removed so
     * callers (tests, logging) can observe progress.
     */
    public int reapExpired(long nowMs) {
        int total = 0;
        for (GroupState g : groups.values()) {
            total += g.reapExpired(nowMs).size();
        }
        return total;
    }

    public Collection<GroupState> groups() {
        return groups.values();
    }

    /** Per-group coordinator state. Operations synchronised on {@code this}. */
    public static final class GroupState {
        private final String groupId;
        private final Map<String, Member> members = new HashMap<>();
        private int generation = 0;
        private @Nullable String leaderMemberId;
        private @Nullable String protocolType;
        private @Nullable String protocolName;

        /** Staged SyncGroup assignment keyed by memberId (written by leader, read by follower). */
        private final Map<String, byte[]> pendingAssignments = new HashMap<>();

        /** Per-member outstanding JoinGroup future; completed when the rebalance finalises. */
        private final Map<String, CompletableFuture<JoinOutcome>> pendingJoins = new HashMap<>();

        /** Per-member outstanding SyncGroup future; completed when the leader submits. */
        private final Map<String, CompletableFuture<SyncResult>> pendingSyncs = new HashMap<>();

        GroupState(String groupId) {
            this.groupId = groupId;
        }

        public String groupId() {
            return groupId;
        }

        public synchronized int generation() {
            return generation;
        }

        @Nullable
        public synchronized String leaderMemberId() {
            return leaderMemberId;
        }

        @Nullable
        public synchronized String protocolName() {
            return protocolName;
        }

        @Nullable
        public synchronized String protocolType() {
            return protocolType;
        }

        public synchronized boolean hasMember(String memberId) {
            return members.containsKey(memberId);
        }

        public synchronized int memberCount() {
            return members.size();
        }

        /**
         * Handle a JoinGroup. Returns a future that completes when the rebalance can be finalised
         * for this member — immediately for the single-member happy path, or after all surviving
         * members have re-joined at the new generation for multi-member rebalances.
         *
         * <p>A missing or empty {@code requestedMemberId} produces a freshly allocated id. A known
         * {@code requestedMemberId} is treated as a rejoin and simply updates metadata + heartbeat
         * timestamp at the current generation.
         */
        public synchronized CompletableFuture<JoinOutcome> join(
                String requestedMemberId,
                String protocolType,
                Map<String, byte[]> protocols,
                @Nullable String groupInstanceId,
                int sessionTimeoutMs,
                long nowMs) {
            boolean existing =
                    requestedMemberId != null
                            && !requestedMemberId.isEmpty()
                            && members.containsKey(requestedMemberId);

            String memberId;
            if (existing) {
                memberId = requestedMemberId;
                Member m = members.get(memberId);
                m.sessionTimeoutMs = sessionTimeoutMs;
                m.lastSeenMs = nowMs;
                m.joinedAtGeneration = generation;
                m.joinMetadata = chooseMetadata(protocols);
            } else {
                // New joiner (or unknown memberId): a fresh rebalance round.
                memberId =
                        (requestedMemberId == null || requestedMemberId.isEmpty())
                                ? newMemberId()
                                : requestedMemberId;
                generation++;
                // Force every surviving member to rejoin at the new generation.
                for (Member m : members.values()) {
                    m.joinedAtGeneration = -1;
                }
                // A fresh generation invalidates all prior sync state.
                failPendingSyncs(SyncResult.rebalanceInProgress());
                pendingAssignments.clear();
                // Clear any stale leader reference; first arrival in the new gen is elected below.
                leaderMemberId = null;
                byte[] metadata = chooseMetadata(protocols);
                members.put(
                        memberId,
                        new Member(
                                memberId,
                                groupInstanceId,
                                sessionTimeoutMs,
                                generation,
                                metadata,
                                nowMs));
            }

            // Election: first member arriving at the current generation fixes leader + protocol.
            if (leaderMemberId == null) {
                leaderMemberId = memberId;
                this.protocolType = protocolType;
                if (!protocols.isEmpty()) {
                    this.protocolName = protocols.keySet().iterator().next();
                }
            }

            CompletableFuture<JoinOutcome> future = new CompletableFuture<>();
            CompletableFuture<JoinOutcome> previous = pendingJoins.put(memberId, future);
            if (previous != null && !previous.isDone()) {
                previous.completeExceptionally(
                        new IllegalStateException("superseded by new JoinGroup"));
            }
            maybeCompleteJoins();
            return future;
        }

        private byte[] chooseMetadata(Map<String, byte[]> protocols) {
            if (protocolName != null && protocols.containsKey(protocolName)) {
                return protocols.get(protocolName);
            }
            if (!protocols.isEmpty()) {
                return protocols.values().iterator().next();
            }
            return new byte[0];
        }

        private void maybeCompleteJoins() {
            if (pendingJoins.isEmpty()) {
                return;
            }
            for (Member m : members.values()) {
                if (m.joinedAtGeneration != generation) {
                    return;
                }
            }
            List<MemberMetadata> roster = new ArrayList<>(members.size());
            for (Member m : members.values()) {
                roster.add(new MemberMetadata(m.memberId, m.groupInstanceId, m.joinMetadata));
            }
            for (Map.Entry<String, CompletableFuture<JoinOutcome>> e : pendingJoins.entrySet()) {
                String memberId = e.getKey();
                List<MemberMetadata> forMember =
                        memberId.equals(leaderMemberId) ? roster : Collections.emptyList();
                e.getValue()
                        .complete(
                                new JoinOutcome(
                                        memberId,
                                        generation,
                                        leaderMemberId,
                                        protocolName,
                                        forMember));
            }
            pendingJoins.clear();
        }

        /**
         * Handle a SyncGroup. The leader submits the full {@code assignments} map and receives its
         * own share; all pending follower futures are completed as a side effect. Followers whose
         * leader has not yet submitted receive a future that blocks until submission or until a
         * rebalance supersedes the current generation.
         */
        public synchronized CompletableFuture<SyncResult> sync(
                String memberId, int requestGeneration, @Nullable Map<String, byte[]> assignments) {
            if (!members.containsKey(memberId)) {
                return CompletableFuture.completedFuture(SyncResult.unknownMember());
            }
            if (requestGeneration != generation) {
                return CompletableFuture.completedFuture(SyncResult.illegalGeneration());
            }
            boolean leaderSubmit =
                    memberId.equals(leaderMemberId)
                            && assignments != null
                            && !assignments.isEmpty();
            if (leaderSubmit) {
                pendingAssignments.clear();
                pendingAssignments.putAll(assignments);
                for (Map.Entry<String, CompletableFuture<SyncResult>> e : pendingSyncs.entrySet()) {
                    byte[] bytes = assignments.get(e.getKey());
                    e.getValue().complete(SyncResult.ok(bytes == null ? new byte[0] : bytes));
                }
                pendingSyncs.clear();
                byte[] own = assignments.get(memberId);
                return CompletableFuture.completedFuture(
                        SyncResult.ok(own == null ? new byte[0] : own));
            }

            // Follower (or leader without assignments) — wait for the leader to submit.
            byte[] staged = pendingAssignments.get(memberId);
            if (staged != null) {
                return CompletableFuture.completedFuture(SyncResult.ok(staged));
            }
            CompletableFuture<SyncResult> future = new CompletableFuture<>();
            CompletableFuture<SyncResult> previous = pendingSyncs.put(memberId, future);
            if (previous != null && !previous.isDone()) {
                previous.completeExceptionally(
                        new IllegalStateException("superseded by new SyncGroup"));
            }
            return future;
        }

        public synchronized HeartbeatResult heartbeat(
                String memberId, int requestGeneration, long nowMs) {
            Member m = members.get(memberId);
            if (m == null) {
                return HeartbeatResult.UNKNOWN_MEMBER;
            }
            m.lastSeenMs = nowMs;
            if (requestGeneration < generation) {
                // An in-flight rebalance has bumped us past this member's last known generation;
                // signal it to rejoin (preserves memberId, unlike ILLEGAL_GENERATION).
                return HeartbeatResult.REBALANCE_IN_PROGRESS;
            }
            if (requestGeneration > generation) {
                return HeartbeatResult.ILLEGAL_GENERATION;
            }
            if (m.joinedAtGeneration != generation) {
                return HeartbeatResult.REBALANCE_IN_PROGRESS;
            }
            return HeartbeatResult.OK;
        }

        public synchronized void leave(String memberId) {
            Member removed = members.remove(memberId);
            pendingAssignments.remove(memberId);
            CompletableFuture<JoinOutcome> join = pendingJoins.remove(memberId);
            if (join != null && !join.isDone()) {
                join.completeExceptionally(new IllegalStateException("member left group"));
            }
            CompletableFuture<SyncResult> sync = pendingSyncs.remove(memberId);
            if (sync != null && !sync.isDone()) {
                sync.complete(SyncResult.unknownMember());
            }
            if (removed == null) {
                return;
            }
            // A departing member triggers a rebalance so survivors re-partition.
            generation++;
            if (members.isEmpty()) {
                leaderMemberId = null;
                protocolName = null;
                protocolType = null;
                pendingAssignments.clear();
                return;
            }
            if (memberId.equals(leaderMemberId)) {
                leaderMemberId = null;
            }
            for (Member m : members.values()) {
                m.joinedAtGeneration = -1;
            }
            failPendingSyncs(SyncResult.rebalanceInProgress());
            pendingAssignments.clear();
            maybeCompleteJoins();
        }

        /**
         * Remove members whose last heartbeat is older than their session timeout. Returns the list
         * of evicted memberIds. A non-empty result drives a generation bump and releases any
         * pending sync futures as {@code REBALANCE_IN_PROGRESS}.
         */
        public synchronized List<String> reapExpired(long nowMs) {
            if (members.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> removed = new ArrayList<>();
            for (Iterator<Map.Entry<String, Member>> it = members.entrySet().iterator();
                    it.hasNext(); ) {
                Map.Entry<String, Member> e = it.next();
                Member m = e.getValue();
                if (nowMs - m.lastSeenMs > m.sessionTimeoutMs) {
                    it.remove();
                    removed.add(m.memberId);
                }
            }
            if (removed.isEmpty()) {
                return removed;
            }
            generation++;
            for (String id : removed) {
                pendingAssignments.remove(id);
                CompletableFuture<JoinOutcome> join = pendingJoins.remove(id);
                if (join != null && !join.isDone()) {
                    join.completeExceptionally(new IllegalStateException("session timeout"));
                }
                CompletableFuture<SyncResult> sync = pendingSyncs.remove(id);
                if (sync != null && !sync.isDone()) {
                    sync.complete(SyncResult.unknownMember());
                }
            }
            if (members.isEmpty()) {
                leaderMemberId = null;
                protocolName = null;
                protocolType = null;
                pendingAssignments.clear();
                failPendingSyncs(SyncResult.rebalanceInProgress());
                return removed;
            }
            if (leaderMemberId != null && !members.containsKey(leaderMemberId)) {
                leaderMemberId = null;
            }
            for (Member m : members.values()) {
                m.joinedAtGeneration = -1;
            }
            failPendingSyncs(SyncResult.rebalanceInProgress());
            pendingAssignments.clear();
            maybeCompleteJoins();
            return removed;
        }

        private void failPendingSyncs(SyncResult result) {
            if (pendingSyncs.isEmpty()) {
                return;
            }
            for (CompletableFuture<SyncResult> f : pendingSyncs.values()) {
                if (!f.isDone()) {
                    f.complete(result);
                }
            }
            pendingSyncs.clear();
        }

        private static String newMemberId() {
            return "fluss-" + UUID.randomUUID();
        }
    }

    /** Result of a JoinGroup operation. */
    public static final class JoinOutcome {
        private final String memberId;
        private final int generation;
        private final @Nullable String leaderMemberId;
        private final @Nullable String protocolName;
        private final List<MemberMetadata> members;

        JoinOutcome(
                String memberId,
                int generation,
                @Nullable String leaderMemberId,
                @Nullable String protocolName,
                List<MemberMetadata> members) {
            this.memberId = memberId;
            this.generation = generation;
            this.leaderMemberId = leaderMemberId;
            this.protocolName = protocolName;
            this.members = members;
        }

        public String memberId() {
            return memberId;
        }

        public int generation() {
            return generation;
        }

        @Nullable
        public String leaderMemberId() {
            return leaderMemberId;
        }

        @Nullable
        public String protocolName() {
            return protocolName;
        }

        /**
         * Member roster visible to the leader. Followers receive an empty list (Kafka does not
         * disclose the roster to them).
         */
        public List<MemberMetadata> members() {
            return members;
        }
    }

    /** Outcome of a SyncGroup operation. */
    public static final class SyncResult {
        /** Outcome code mirroring the Kafka wire-protocol error set used by SyncGroup. */
        public enum Code {
            OK,
            UNKNOWN_MEMBER,
            ILLEGAL_GENERATION,
            REBALANCE_IN_PROGRESS
        }

        private final Code code;
        private final byte[] assignment;

        private SyncResult(Code code, byte[] assignment) {
            this.code = code;
            this.assignment = assignment;
        }

        public Code code() {
            return code;
        }

        public byte[] assignment() {
            return assignment;
        }

        static SyncResult ok(byte[] assignment) {
            return new SyncResult(Code.OK, assignment);
        }

        static SyncResult unknownMember() {
            return new SyncResult(Code.UNKNOWN_MEMBER, new byte[0]);
        }

        static SyncResult illegalGeneration() {
            return new SyncResult(Code.ILLEGAL_GENERATION, new byte[0]);
        }

        static SyncResult rebalanceInProgress() {
            return new SyncResult(Code.REBALANCE_IN_PROGRESS, new byte[0]);
        }
    }

    /** Outcome of a Heartbeat operation. */
    public enum HeartbeatResult {
        OK,
        UNKNOWN_MEMBER,
        ILLEGAL_GENERATION,
        REBALANCE_IN_PROGRESS
    }

    /** Snapshot of a member's identity + subscription bytes for the leader's roster. */
    public static final class MemberMetadata {
        private final String memberId;
        private final @Nullable String groupInstanceId;
        private final byte[] metadata;

        MemberMetadata(String memberId, @Nullable String groupInstanceId, byte[] metadata) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
            this.metadata = metadata;
        }

        public String memberId() {
            return memberId;
        }

        @Nullable
        public String groupInstanceId() {
            return groupInstanceId;
        }

        public byte[] metadata() {
            return metadata;
        }
    }

    /** Per-member state. */
    static final class Member {
        final String memberId;
        final @Nullable String groupInstanceId;
        int sessionTimeoutMs;
        int joinedAtGeneration;
        byte[] joinMetadata;
        long lastSeenMs;

        Member(
                String memberId,
                @Nullable String groupInstanceId,
                int sessionTimeoutMs,
                int joinedAtGeneration,
                byte[] joinMetadata,
                long lastSeenMs) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.joinedAtGeneration = joinedAtGeneration;
            this.joinMetadata = joinMetadata;
            this.lastSeenMs = lastSeenMs;
        }
    }
}
