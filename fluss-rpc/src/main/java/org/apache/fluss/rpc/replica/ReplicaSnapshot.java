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
 * Immutable snapshot of replica-level metadata a protocol bolt-on needs for read-only operations
 * (e.g. Kafka's {@code LIST_OFFSETS} and {@code METADATA}).
 *
 * <p>Exposed by {@code ReplicaManager#getReplicaSnapshot(TableBucket)} so bolt-ons never have to
 * import {@code org.apache.fluss.server.replica.Replica} — the Fluss replica internals can evolve
 * freely as long as the fields here stay filled.
 */
@PublicEvolving
public final class ReplicaSnapshot {

    private final long logStartOffset;
    private final long localLogEndOffset;
    private final long logHighWatermark;
    private final int leaderEpoch;

    public ReplicaSnapshot(
            long logStartOffset, long localLogEndOffset, long logHighWatermark, int leaderEpoch) {
        this.logStartOffset = logStartOffset;
        this.localLogEndOffset = localLogEndOffset;
        this.logHighWatermark = logHighWatermark;
        this.leaderEpoch = leaderEpoch;
    }

    /** Earliest offset still retained locally (== Kafka's log-start-offset). */
    public long logStartOffset() {
        return logStartOffset;
    }

    /** One past the last local offset (== Kafka's LEO). */
    public long localLogEndOffset() {
        return localLogEndOffset;
    }

    /** Committed high watermark (consumers can read up to here). */
    public long logHighWatermark() {
        return logHighWatermark;
    }

    /** Current leader epoch — monotonic across failovers. */
    public int leaderEpoch() {
        return leaderEpoch;
    }
}
