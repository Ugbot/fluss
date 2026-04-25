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

package org.apache.fluss.catalog.entities;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase J.3 — round-trip tests for the encoded {@code group_ids} column on {@link
 * KafkaTxnStateEntity} and the new {@link KafkaTxnOffsetBufferEntity}. Uses randomised inputs per
 * the project's no-hardcoded-happy-paths rule.
 */
final class KafkaTxnEntitiesTest {

    @Test
    void txnStateRoundTripsGroupIds() {
        Random rand = new Random();
        Set<String> partitions = randStrSet(rand, "topic", "p", 1, 8);
        Set<String> groups = randStrSet(rand, "g", null, 0, 5);
        long now = System.currentTimeMillis();
        KafkaTxnStateEntity entity =
                new KafkaTxnStateEntity(
                        UUID.randomUUID().toString(),
                        rand.nextLong(),
                        (short) rand.nextInt(Short.MAX_VALUE),
                        KafkaTxnStateEntity.STATE_ONGOING,
                        partitions,
                        groups,
                        rand.nextInt(60_000) + 1000,
                        now - rand.nextInt(60_000),
                        now);

        // groups round-trip via encodeGroups / decodeGroups.
        String encoded = KafkaTxnStateEntity.encodeGroups(entity.groupIds());
        Set<String> decoded = KafkaTxnStateEntity.decodeGroups(encoded);
        assertThat(decoded).isEqualTo(entity.groupIds());
        // partitions round-trip likewise.
        assertThat(
                        KafkaTxnStateEntity.decodePartitions(
                                KafkaTxnStateEntity.encodePartitions(entity.topicPartitions())))
                .isEqualTo(entity.topicPartitions());
    }

    @Test
    void txnStateLegacyConstructorDefaultsGroupsEmpty() {
        long now = System.currentTimeMillis();
        KafkaTxnStateEntity legacy =
                new KafkaTxnStateEntity(
                        "legacy",
                        42L,
                        (short) 0,
                        KafkaTxnStateEntity.STATE_EMPTY,
                        null,
                        60_000,
                        null,
                        now);
        assertThat(legacy.groupIds()).isEmpty();
    }

    @Test
    void txnStateEqualsConsidersGroupIds() {
        long now = System.currentTimeMillis();
        Set<String> partitions = new TreeSet<>();
        partitions.add("a:0");
        Set<String> groupsA = new TreeSet<>();
        groupsA.add("groupA");
        Set<String> groupsB = new TreeSet<>();
        groupsB.add("groupB");
        KafkaTxnStateEntity left =
                new KafkaTxnStateEntity(
                        "id",
                        1L,
                        (short) 0,
                        KafkaTxnStateEntity.STATE_ONGOING,
                        partitions,
                        groupsA,
                        60_000,
                        now,
                        now);
        KafkaTxnStateEntity right =
                new KafkaTxnStateEntity(
                        "id",
                        1L,
                        (short) 0,
                        KafkaTxnStateEntity.STATE_ONGOING,
                        partitions,
                        groupsB,
                        60_000,
                        now,
                        now);
        assertThat(left).isNotEqualTo(right);
        assertThat(left.hashCode()).isNotEqualTo(right.hashCode());
    }

    @Test
    void offsetBufferEntityEqualityAndAccessors() {
        Random rand = new Random();
        long now = System.currentTimeMillis();
        String txnId = "txn-" + rand.nextInt();
        String groupId = "group-" + rand.nextInt();
        String topic = "topic-" + rand.nextInt();
        int partition = rand.nextInt(64);
        long offset = (long) rand.nextInt(Integer.MAX_VALUE);
        int leaderEpoch = rand.nextInt(1000);
        String metadata = rand.nextBoolean() ? null : "meta-" + rand.nextInt();
        KafkaTxnOffsetBufferEntity e =
                new KafkaTxnOffsetBufferEntity(
                        txnId, groupId, topic, partition, offset, leaderEpoch, metadata, now);
        assertThat(e.transactionalId()).isEqualTo(txnId);
        assertThat(e.groupId()).isEqualTo(groupId);
        assertThat(e.topic()).isEqualTo(topic);
        assertThat(e.partition()).isEqualTo(partition);
        assertThat(e.offset()).isEqualTo(offset);
        assertThat(e.leaderEpoch()).isEqualTo(leaderEpoch);
        assertThat(e.metadata()).isEqualTo(metadata);
        assertThat(e.committedAtMillis()).isEqualTo(now);

        KafkaTxnOffsetBufferEntity copy =
                new KafkaTxnOffsetBufferEntity(
                        txnId, groupId, topic, partition, offset, leaderEpoch, metadata, now);
        assertThat(e).isEqualTo(copy);
        assertThat(e.hashCode()).isEqualTo(copy.hashCode());

        // toString includes all fields — at minimum the (txn, group, topic, partition, offset).
        String s = e.toString();
        assertThat(s)
                .contains(txnId)
                .contains(groupId)
                .contains(topic)
                .contains(Integer.toString(partition))
                .contains(Long.toString(offset));
    }

    private static Set<String> randStrSet(
            Random rand, String prefixA, String prefixB, int min, int max) {
        int count = min + rand.nextInt(Math.max(1, max - min));
        Set<String> out = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String item =
                    prefixB == null
                            ? prefixA + rand.nextInt(1_000_000)
                            : prefixA + i + ":" + rand.nextInt(64);
            out.add(item);
        }
        return out;
    }
}
