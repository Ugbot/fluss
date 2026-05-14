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

import java.util.Optional;
import java.util.Set;

/**
 * Storage for Kafka consumer group committed offsets, keyed by {@code (groupId, topic, partition)}.
 * Implementations decide the durability characteristics.
 */
@Internal
public interface OffsetStore {

    void commit(
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            @Nullable String metadata)
            throws Exception;

    Optional<CommittedOffset> fetch(String groupId, String topic, int partition) throws Exception;

    void delete(String groupId, String topic, int partition) throws Exception;

    void deleteGroup(String groupId) throws Exception;

    Set<String> knownGroupIds() throws Exception;

    boolean groupExists(String groupId) throws Exception;

    /** A committed-offset record. */
    final class CommittedOffset {
        private final long offset;
        private final int leaderEpoch;
        private final @Nullable String metadata;

        public CommittedOffset(long offset, int leaderEpoch, @Nullable String metadata) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.metadata = metadata;
        }

        public long offset() {
            return offset;
        }

        public int leaderEpoch() {
            return leaderEpoch;
        }

        @Nullable
        public String metadata() {
            return metadata;
        }
    }
}
