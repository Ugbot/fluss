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

import org.apache.fluss.kafka.group.KafkaGroupRegistry.GroupState;
import org.apache.fluss.kafka.group.KafkaGroupRegistry.HeartbeatResult;
import org.apache.fluss.kafka.group.KafkaGroupRegistry.JoinOutcome;
import org.apache.fluss.kafka.group.KafkaGroupRegistry.SyncResult;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the consumer-group state machine: single-consumer happy path, multi-consumer
 * rebalance, follower-blocking SyncGroup, heartbeat signalling, and session-timeout reaping.
 */
class KafkaGroupRegistryTest {

    private static final int SESSION_MS = 30_000;

    private static Map<String, byte[]> protocols() {
        Map<String, byte[]> p = new HashMap<>();
        p.put("range", new byte[] {1, 2, 3});
        return p;
    }

    private static JoinOutcome joinNow(GroupState state, String memberId, int sessionMs, long nowMs)
            throws Exception {
        return state.join(memberId, "consumer", protocols(), null, sessionMs, nowMs)
                .get(5, TimeUnit.SECONDS);
    }

    @Test
    void singleConsumerJoinsAsLeader() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");

        JoinOutcome out = joinNow(state, "", SESSION_MS, 1_000L);

        assertThat(out.memberId()).startsWith("fluss-");
        assertThat(out.generation()).isEqualTo(1);
        assertThat(out.leaderMemberId()).isEqualTo(out.memberId());
        assertThat(out.protocolName()).isEqualTo("range");
        assertThat(out.members()).hasSize(1);
        assertThat(out.members().get(0).memberId()).isEqualTo(out.memberId());
    }

    @Test
    void singleConsumerSyncReceivesOwnAssignment() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");
        JoinOutcome out = joinNow(state, "", SESSION_MS, 1_000L);

        Map<String, byte[]> assignments =
                Collections.singletonMap(out.memberId(), new byte[] {9, 9});
        SyncResult sync =
                state.sync(out.memberId(), out.generation(), assignments).get(5, TimeUnit.SECONDS);

        assertThat(sync.code()).isEqualTo(SyncResult.Code.OK);
        assertThat(sync.assignment()).containsExactly(9, 9);
    }

    @Test
    void heartbeatAfterJoinIsOk() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");
        JoinOutcome out = joinNow(state, "", SESSION_MS, 1_000L);

        HeartbeatResult hb = state.heartbeat(out.memberId(), out.generation(), 1_500L);
        assertThat(hb).isEqualTo(HeartbeatResult.OK);
    }

    @Test
    void secondJoinerTriggersRebalanceAndHoldsLeaderFuture() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");

        JoinOutcome a = joinNow(state, "", SESSION_MS, 1_000L);
        assertThat(a.generation()).isEqualTo(1);

        // B arrives. B becomes leader of gen 2 (first arrival at new gen). But B's join must block
        // until A rejoins at gen 2, so the roster is complete.
        CompletableFuture<JoinOutcome> bJoin =
                state.join("", "consumer", protocols(), null, SESSION_MS, 2_000L);
        assertThat(bJoin).isNotDone();
        assertThat(state.generation()).isEqualTo(2);
        assertThat(state.leaderMemberId()).isNotEqualTo(a.memberId());

        // A notices REBALANCE_IN_PROGRESS from its next heartbeat — not ILLEGAL_GENERATION,
        // because the Kafka client drops its memberId on ILLEGAL_GENERATION which would break the
        // rebalance loop.
        HeartbeatResult aHb = state.heartbeat(a.memberId(), 1, 2_500L);
        assertThat(aHb).isEqualTo(HeartbeatResult.REBALANCE_IN_PROGRESS);

        // A rejoins with its known memberId; after which B's future completes with a 2-member
        // roster and A's future completes with an empty follower roster.
        JoinOutcome aRejoined = joinNow(state, a.memberId(), SESSION_MS, 3_000L);
        JoinOutcome b = bJoin.get(5, TimeUnit.SECONDS);

        assertThat(b.generation()).isEqualTo(2);
        assertThat(aRejoined.generation()).isEqualTo(2);
        assertThat(b.leaderMemberId()).isEqualTo(b.memberId());
        assertThat(aRejoined.leaderMemberId()).isEqualTo(b.memberId());
        assertThat(b.members()).hasSize(2);
        assertThat(aRejoined.members()).isEmpty();
    }

    @Test
    void followerSyncBlocksUntilLeaderSubmits() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");

        JoinOutcome a = joinNow(state, "", SESSION_MS, 1_000L);
        CompletableFuture<JoinOutcome> bJoin =
                state.join("", "consumer", protocols(), null, SESSION_MS, 2_000L);
        JoinOutcome aRejoined = joinNow(state, a.memberId(), SESSION_MS, 3_000L);
        JoinOutcome b = bJoin.get(5, TimeUnit.SECONDS);

        String leaderId = b.leaderMemberId();
        String followerId = leaderId.equals(b.memberId()) ? aRejoined.memberId() : b.memberId();

        // Follower's SyncGroup is still waiting for the leader to submit assignments.
        CompletableFuture<SyncResult> followerSync = state.sync(followerId, b.generation(), null);
        assertThat(followerSync).isNotDone();

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(leaderId, new byte[] {0x10});
        assignments.put(followerId, new byte[] {0x20, 0x21});
        SyncResult leaderSync =
                state.sync(leaderId, b.generation(), assignments).get(5, TimeUnit.SECONDS);

        assertThat(leaderSync.code()).isEqualTo(SyncResult.Code.OK);
        assertThat(leaderSync.assignment()).containsExactly(0x10);

        SyncResult follower = followerSync.get(5, TimeUnit.SECONDS);
        assertThat(follower.code()).isEqualTo(SyncResult.Code.OK);
        assertThat(follower.assignment()).containsExactly(0x20, 0x21);
    }

    @Test
    void sessionTimeoutReapsDeadMemberAndBumpsGeneration() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");

        JoinOutcome a = joinNow(state, "", 500, 0L);
        JoinOutcome b;
        {
            CompletableFuture<JoinOutcome> bJoin =
                    state.join("", "consumer", protocols(), null, 500, 100L);
            joinNow(state, a.memberId(), 500, 150L);
            b = bJoin.get(5, TimeUnit.SECONDS);
        }
        assertThat(state.memberCount()).isEqualTo(2);
        int genBefore = state.generation();

        // Reap long after the session window — A didn't heartbeat since 150ms.
        state.heartbeat(b.memberId(), b.generation(), 10_000L);
        int reaped = registry.reapExpired(10_000L);
        assertThat(reaped).isEqualTo(1);
        assertThat(state.memberCount()).isEqualTo(1);
        assertThat(state.generation()).isGreaterThan(genBefore);
        assertThat(state.hasMember(b.memberId())).isTrue();
        assertThat(state.hasMember(a.memberId())).isFalse();

        // B's next heartbeat sees the stale generation and asks for a rejoin.
        HeartbeatResult bHb = state.heartbeat(b.memberId(), b.generation(), 10_500L);
        assertThat(bHb).isEqualTo(HeartbeatResult.REBALANCE_IN_PROGRESS);
    }

    @Test
    void leaveGroupDropsMemberAndRebalances() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");
        JoinOutcome a = joinNow(state, "", SESSION_MS, 1_000L);

        int gen = state.generation();
        state.leave(a.memberId());

        assertThat(state.memberCount()).isEqualTo(0);
        assertThat(state.generation()).isGreaterThan(gen);
        assertThat(state.leaderMemberId()).isNull();
    }

    @Test
    void syncWithWrongGenerationFailsImmediately() throws Exception {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");
        JoinOutcome a = joinNow(state, "", SESSION_MS, 1_000L);

        SyncResult res = state.sync(a.memberId(), a.generation() + 7, null).get();
        assertThat(res.code()).isEqualTo(SyncResult.Code.ILLEGAL_GENERATION);
    }

    @Test
    void heartbeatForUnknownMemberReportsUnknownMember() {
        KafkaGroupRegistry registry = new KafkaGroupRegistry();
        GroupState state = registry.getOrCreate("g1");

        HeartbeatResult hb = state.heartbeat("never-joined", 1, 1_000L);
        assertThat(hb).isEqualTo(HeartbeatResult.UNKNOWN_MEMBER);
    }
}
