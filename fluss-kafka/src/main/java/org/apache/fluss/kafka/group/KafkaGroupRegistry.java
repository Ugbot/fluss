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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal in-memory consumer-group state machine. Phase 2D-level: tracks membership, generation,
 * the leader's chosen protocol and its assignment payload. Single-consumer groups work end-to-end;
 * multi-consumer groups degrade to "first member is always leader, second member gets an empty
 * assignment" — see design 0004 §4.
 *
 * <p>Per-group mutations are serialized by a coarse lock on the {@link GroupState} instance.
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

    /** Per-group coordinator state. Operations synchronised on {@code this}. */
    public static final class GroupState {
        private final String groupId;
        private final Map<String, Member> members = new HashMap<>();
        private int generation = 0;
        private @Nullable String leaderMemberId;
        private @Nullable String protocolType;
        private @Nullable String protocolName;

        /** Pending SyncGroup assignment keyed by memberId (written by leader, read by follower). */
        private final Map<String, byte[]> pendingAssignments = new HashMap<>();

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

        /**
         * Register a new member. Returns the assigned memberId. The first member becomes the
         * leader; subsequent joiners inherit the existing leader and are added to the roster.
         *
         * @param requestedMemberId the memberId from the JoinGroup request. Empty string means
         *     "assign me one" per Kafka's two-phase Join protocol.
         * @param protocols assignor protocols offered by this member; we pick the first one
         *     (matching Kafka's behavior when only one is offered, and a reasonable-enough fallback
         *     otherwise for Phase 2D single-consumer groups).
         */
        public synchronized JoinOutcome join(
                String requestedMemberId,
                String protocolType,
                Map<String, byte[]> protocols,
                @Nullable String groupInstanceId) {
            String memberId =
                    (requestedMemberId == null || requestedMemberId.isEmpty())
                            ? newMemberId()
                            : requestedMemberId;

            if (!members.containsKey(memberId)) {
                // New member → bump generation.
                members.put(memberId, new Member(memberId, groupInstanceId));
                generation++;
                if (leaderMemberId == null) {
                    leaderMemberId = memberId;
                    this.protocolType = protocolType;
                    if (!protocols.isEmpty()) {
                        this.protocolName = protocols.keySet().iterator().next();
                    }
                }
            }
            return new JoinOutcome(memberId, generation, leaderMemberId, this.protocolName);
        }

        /**
         * Submit the assignment from the current leader's SyncGroup call and return the assignment
         * for {@code forMemberId}.
         *
         * @param fromMemberId id of the SyncGroup caller (must equal the leader for the assignment
         *     to be accepted; follower callers just read their already-staged payload).
         * @param assignments map of memberId → assignment bytes submitted by the leader.
         * @return the assignment bytes for {@code forMemberId}, or an empty array if none.
         */
        public synchronized byte[] sync(
                String fromMemberId,
                String forMemberId,
                @Nullable Map<String, byte[]> assignments) {
            if (assignments != null && fromMemberId.equals(leaderMemberId)) {
                pendingAssignments.clear();
                pendingAssignments.putAll(assignments);
            }
            byte[] payload = pendingAssignments.get(forMemberId);
            return payload == null ? new byte[0] : payload;
        }

        public synchronized void leave(String memberId) {
            members.remove(memberId);
            pendingAssignments.remove(memberId);
            if (memberId.equals(leaderMemberId)) {
                leaderMemberId = members.isEmpty() ? null : members.keySet().iterator().next();
                generation++;
            }
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

        JoinOutcome(
                String memberId,
                int generation,
                @Nullable String leaderMemberId,
                @Nullable String protocolName) {
            this.memberId = memberId;
            this.generation = generation;
            this.leaderMemberId = leaderMemberId;
            this.protocolName = protocolName;
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
    }

    /** Per-member state. */
    public static final class Member {
        private final String memberId;
        private final @Nullable String groupInstanceId;

        Member(String memberId, @Nullable String groupInstanceId) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
        }

        public String memberId() {
            return memberId;
        }

        @Nullable
        public String groupInstanceId() {
            return groupInstanceId;
        }
    }
}
