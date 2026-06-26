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

package org.apache.fluss.lake.tiering.coordinator;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.lake.committer.TieringStats;

import javax.annotation.Nullable;

/**
 * Immutable information about a finished tiering round of a single table, used to populate the
 * {@code finishedTables} section of a {@link
 * org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest}.
 *
 * <p>Extracted from the {@code TieringSourceEnumerator.TieringFinishInfo} inner class (which was
 * private and Flink-package-local) so the engine-agnostic heartbeat builders can be shared.
 */
@Internal
public final class TieringFinishInfo {

    /** The epoch of the tiering operation for this table. */
    private final long tieringEpoch;

    /**
     * Whether this table was force finished due to reaching the maximum tiering duration. When a
     * table's tiering operation exceeds the max duration (data lake freshness), it will be force
     * finished to prevent it from blocking other tables' tiering operations. The coordinator then
     * transitions the table {@code Tiered -> Pending} (immediate re-tier) instead of {@code Tiered
     * -> Scheduled}.
     */
    private final boolean forceFinished;

    /**
     * Stats collected during this tiering round; never {@code null} ({@link TieringStats#UNKNOWN}).
     */
    private final TieringStats stats;

    public static TieringFinishInfo from(long tieringEpoch) {
        return new TieringFinishInfo(tieringEpoch, false, null);
    }

    public static TieringFinishInfo from(
            long tieringEpoch, boolean forceFinished, @Nullable TieringStats stats) {
        return new TieringFinishInfo(tieringEpoch, forceFinished, stats);
    }

    private TieringFinishInfo(
            long tieringEpoch, boolean forceFinished, @Nullable TieringStats stats) {
        this.tieringEpoch = tieringEpoch;
        this.forceFinished = forceFinished;
        this.stats = stats != null ? stats : TieringStats.UNKNOWN;
    }

    public long getTieringEpoch() {
        return tieringEpoch;
    }

    public boolean isForceFinished() {
        return forceFinished;
    }

    public TieringStats getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return "TieringFinishInfo{"
                + "tieringEpoch="
                + tieringEpoch
                + ", forceFinished="
                + forceFinished
                + ", stats="
                + stats
                + '}';
    }
}
