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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * One row in {@code _catalog.__kafka_txn_state__} (design 0016 §5). Captures the durable Kafka
 * transactional-id state on the elected coordinator leader: producer id + epoch (the fencing key),
 * the lifecycle state, the participating partition set, the configured timeout, and the start /
 * last-update timestamps.
 *
 * <p>Phase J.1 lands this entity together with the producer-id allocator and the rewritten {@code
 * INIT_PRODUCER_ID} handler. The state machine transitions ({@code Empty → Ongoing → PrepareCommit
 * / PrepareAbort → CompleteCommit / CompleteAbort → Empty}) ship in J.2 / J.3; in J.1 the only
 * transitions touched are {@code (no row) → Empty} and {@code Empty → Empty} (fence-only epoch
 * bump).
 *
 * <p>The partition set is encoded as a comma-separated {@code "topic:partition"} string in the
 * underlying Fluss PK row: see design 0016 §5 "String-encoded partition set" for why a Fluss-native
 * {@code SET<STRING>} is not used. {@code null} or empty represents an empty set, which is the
 * normal case in {@code Empty} state.
 */
@PublicEvolving
public final class KafkaTxnStateEntity {

    /** Lifecycle state names persisted to the {@code state} column; see design 0016 §4. */
    public static final String STATE_EMPTY = "Empty";

    public static final String STATE_ONGOING = "Ongoing";
    public static final String STATE_PREPARE_COMMIT = "PrepareCommit";
    public static final String STATE_PREPARE_ABORT = "PrepareAbort";
    public static final String STATE_COMPLETE_COMMIT = "CompleteCommit";
    public static final String STATE_COMPLETE_ABORT = "CompleteAbort";

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final String state;
    private final Set<String> topicPartitions;
    private final int timeoutMs;
    private final @Nullable Long txnStartTimestampMillis;
    private final long lastUpdatedAtMillis;

    public KafkaTxnStateEntity(
            String transactionalId,
            long producerId,
            short producerEpoch,
            String state,
            @Nullable Set<String> topicPartitions,
            int timeoutMs,
            @Nullable Long txnStartTimestampMillis,
            long lastUpdatedAtMillis) {
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.state = state;
        this.topicPartitions =
                topicPartitions == null
                        ? Collections.emptySet()
                        : Collections.unmodifiableSet(new TreeSet<>(topicPartitions));
        this.timeoutMs = timeoutMs;
        this.txnStartTimestampMillis = txnStartTimestampMillis;
        this.lastUpdatedAtMillis = lastUpdatedAtMillis;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public String state() {
        return state;
    }

    /** Immutable, deterministic-iteration set of {@code "topic:partition"} entries. */
    public Set<String> topicPartitions() {
        return topicPartitions;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    @Nullable
    public Long txnStartTimestampMillis() {
        return txnStartTimestampMillis;
    }

    public long lastUpdatedAtMillis() {
        return lastUpdatedAtMillis;
    }

    /**
     * Encode the partition set into the comma-separated wire form used by the Fluss PK row. Sorted
     * so identical sets produce identical encodings (deterministic for tests + idempotent rewrite).
     */
    public static String encodePartitions(@Nullable Set<String> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return null;
        }
        TreeSet<String> sorted = new TreeSet<>(partitions);
        StringBuilder b = new StringBuilder();
        boolean first = true;
        for (String p : sorted) {
            if (!first) {
                b.append(',');
            }
            b.append(p);
            first = false;
        }
        return b.toString();
    }

    /** Inverse of {@link #encodePartitions(Set)}. {@code null} or empty maps to an empty set. */
    public static Set<String> decodePartitions(@Nullable String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return Collections.emptySet();
        }
        TreeSet<String> out = new TreeSet<>();
        for (String s : encoded.split(",")) {
            String trimmed = s.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTxnStateEntity)) {
            return false;
        }
        KafkaTxnStateEntity that = (KafkaTxnStateEntity) o;
        return producerId == that.producerId
                && producerEpoch == that.producerEpoch
                && timeoutMs == that.timeoutMs
                && lastUpdatedAtMillis == that.lastUpdatedAtMillis
                && transactionalId.equals(that.transactionalId)
                && state.equals(that.state)
                && topicPartitions.equals(that.topicPartitions)
                && Objects.equals(txnStartTimestampMillis, that.txnStartTimestampMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionalId,
                producerId,
                producerEpoch,
                state,
                topicPartitions,
                timeoutMs,
                txnStartTimestampMillis,
                lastUpdatedAtMillis);
    }

    @Override
    public String toString() {
        return "KafkaTxnStateEntity{"
                + "transactionalId='"
                + transactionalId
                + '\''
                + ", producerId="
                + producerId
                + ", producerEpoch="
                + producerEpoch
                + ", state='"
                + state
                + '\''
                + ", topicPartitions="
                + topicPartitions
                + ", timeoutMs="
                + timeoutMs
                + ", txnStartTimestampMillis="
                + txnStartTimestampMillis
                + ", lastUpdatedAtMillis="
                + lastUpdatedAtMillis
                + '}';
    }
}
