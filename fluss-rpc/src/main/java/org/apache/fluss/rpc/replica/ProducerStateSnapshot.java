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

package org.apache.fluss.rpc.replica;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Immutable snapshot of one active idempotent-producer entry for a single bucket. Fields match
 * Kafka's {@code DescribeProducersResponseData.ProducerState} shape so bolt-ons (Kafka's {@code
 * DESCRIBE_PRODUCERS}) can translate without further adaptation.
 *
 * <p>Fluss's internal term is "writer" rather than "producer"; the name here follows the Kafka
 * vocabulary since this DTO is the protocol boundary. {@link #producerId()} corresponds to Fluss's
 * {@code writerId}. {@link #producerEpoch()} is always {@code 0} for now — Fluss has idempotent
 * writer ids but no per-writer epoch concept (no transactional producer yet).
 *
 * <p>{@link #coordinatorEpoch()} and {@link #currentTxnStartOffset()} default to sentinel values
 * ({@code -1}) because Fluss doesn't have a transaction coordinator today; bolt-ons should pass
 * these through unchanged.
 */
@PublicEvolving
public final class ProducerStateSnapshot {

    private final long producerId;
    private final int producerEpoch;
    private final int lastSequence;
    private final long lastTimestamp;
    private final int coordinatorEpoch;
    private final long currentTxnStartOffset;

    public ProducerStateSnapshot(
            long producerId,
            int producerEpoch,
            int lastSequence,
            long lastTimestamp,
            int coordinatorEpoch,
            long currentTxnStartOffset) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.lastSequence = lastSequence;
        this.lastTimestamp = lastTimestamp;
        this.coordinatorEpoch = coordinatorEpoch;
        this.currentTxnStartOffset = currentTxnStartOffset;
    }

    /** Producer id (Fluss writer id). Always {@code >= 0} for idempotent producers. */
    public long producerId() {
        return producerId;
    }

    /** Producer epoch. Fluss has no epoch concept today; always {@code 0}. */
    public int producerEpoch() {
        return producerEpoch;
    }

    /** Last successful batch sequence seen from this producer; {@code -1} if unknown. */
    public int lastSequence() {
        return lastSequence;
    }

    /** Wall-clock timestamp (millis since epoch) of this producer's last batch. */
    public long lastTimestamp() {
        return lastTimestamp;
    }

    /**
     * Transaction-coordinator epoch for the in-flight transaction. {@code -1} when there is no
     * active transaction (always true in Fluss today, which has no transactional producer).
     */
    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    /**
     * Offset of the first record of the in-flight transaction, or {@code -1} if no transaction is
     * active.
     */
    public long currentTxnStartOffset() {
        return currentTxnStartOffset;
    }
}
