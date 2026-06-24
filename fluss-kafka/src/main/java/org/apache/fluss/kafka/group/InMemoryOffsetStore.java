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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link OffsetStore}. Lost on broker restart; primarily useful for
 * tests and as a fast-read cache fronting a durable backend (see {@link ZkOffsetStore}).
 */
@Internal
public final class InMemoryOffsetStore implements OffsetStore {

    private final ConcurrentHashMap<String, Map<TopicPartition, CommittedOffset>> byGroup =
            new ConcurrentHashMap<>();

    @Override
    public synchronized void commit(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            @Nullable String metadata) {
        byGroup.computeIfAbsent(groupId, g -> new HashMap<>())
                .put(
                        new TopicPartition(topic, partition),
                        new CommittedOffset(offset, leaderEpoch, metadata));
    }

    @Override
    public synchronized Optional<CommittedOffset> fetch(
            String groupId, String topic, int partition) {
        Map<TopicPartition, CommittedOffset> offsets = byGroup.get(groupId);
        if (offsets == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(offsets.get(new TopicPartition(topic, partition)));
    }

    @Override
    public synchronized void delete(String groupId, String topic, int partition) {
        Map<TopicPartition, CommittedOffset> offsets = byGroup.get(groupId);
        if (offsets != null) {
            offsets.remove(new TopicPartition(topic, partition));
            if (offsets.isEmpty()) {
                byGroup.remove(groupId);
            }
        }
    }

    @Override
    public synchronized void deleteGroup(String groupId) {
        byGroup.remove(groupId);
    }

    @Override
    public synchronized Set<String> knownGroupIds() {
        return new HashSet<>(byGroup.keySet());
    }

    @Override
    public synchronized boolean groupExists(String groupId) {
        return byGroup.containsKey(groupId);
    }

    private static final class TopicPartition {
        private final String topic;
        private final int partition;

        TopicPartition(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TopicPartition)) {
                return false;
            }
            TopicPartition that = (TopicPartition) o;
            return partition == that.partition && Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition);
        }
    }
}
