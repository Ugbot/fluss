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

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One row in {@code _catalog.__kafka_txn_offset_buffer__} (design 0016 §10) — a Phase J.3 durable
 * replacement for the in-memory {@code TxnOffsetBuffer}. Rows are inserted on every {@code
 * TXN_OFFSET_COMMIT} and read+deleted by the coordinator on {@code END_TXN(commit)} (or simply
 * deleted on {@code END_TXN(abort)}).
 *
 * <p>The composite primary key is {@code (transactional_id, group_id, topic, partition)}; on a
 * second commit for the same key the row is upserted, matching Kafka's "later commits overwrite
 * earlier ones" semantic for offsets within a single transaction.
 */
@PublicEvolving
public final class KafkaTxnOffsetBufferEntity {

    private final String transactionalId;
    private final String groupId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final int leaderEpoch;
    private final @Nullable String metadata;
    private final long committedAtMillis;

    public KafkaTxnOffsetBufferEntity(
            String transactionalId,
            String groupId,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            @Nullable String metadata,
            long committedAtMillis) {
        this.transactionalId = transactionalId;
        this.groupId = groupId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
        this.metadata = metadata;
        this.committedAtMillis = committedAtMillis;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public String groupId() {
        return groupId;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
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

    public long committedAtMillis() {
        return committedAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTxnOffsetBufferEntity)) {
            return false;
        }
        KafkaTxnOffsetBufferEntity that = (KafkaTxnOffsetBufferEntity) o;
        return partition == that.partition
                && offset == that.offset
                && leaderEpoch == that.leaderEpoch
                && committedAtMillis == that.committedAtMillis
                && transactionalId.equals(that.transactionalId)
                && groupId.equals(that.groupId)
                && topic.equals(that.topic)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionalId,
                groupId,
                topic,
                partition,
                offset,
                leaderEpoch,
                metadata,
                committedAtMillis);
    }

    @Override
    public String toString() {
        return "KafkaTxnOffsetBufferEntity{"
                + "transactionalId='"
                + transactionalId
                + '\''
                + ", groupId='"
                + groupId
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", partition="
                + partition
                + ", offset="
                + offset
                + ", leaderEpoch="
                + leaderEpoch
                + ", metadata="
                + metadata
                + ", committedAtMillis="
                + committedAtMillis
                + '}';
    }
}
