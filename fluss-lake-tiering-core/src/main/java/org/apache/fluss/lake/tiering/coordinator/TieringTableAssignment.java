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
import org.apache.fluss.metadata.TablePath;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * An immutable description of a single table assigned to the tiering service by the Fluss
 * coordinator, returned in the {@code lakeTieringHeartbeat} response when the service requested a
 * table.
 *
 * <p>This replaces the engine-specific {@code org.apache.flink.api.java.tuple.Tuple3<Long, Long,
 * TablePath>} that {@code TieringSourceEnumerator} used to carry {@code (tableId, tieringEpoch,
 * tablePath)} between {@code requestTieringTableSplitsViaHeartBeat} and {@code
 * generateTieringSplits}.
 *
 * <p>The {@code tieringEpoch} fences the assignment: it must be propagated unchanged through all
 * subsequent {@code finished} / {@code failed} / in-flight heartbeat requests for this table so the
 * coordinator can detect stale tiering attempts (see {@code FencedTieringEpochException} on the
 * server side).
 */
@Internal
public final class TieringTableAssignment {

    private final long tableId;
    private final long tieringEpoch;
    private final TablePath tablePath;

    public TieringTableAssignment(long tableId, long tieringEpoch, TablePath tablePath) {
        this.tableId = tableId;
        this.tieringEpoch = tieringEpoch;
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null");
    }

    /** The id of the table to tier. */
    public long getTableId() {
        return tableId;
    }

    /** The tiering epoch fencing this assignment; must be echoed on all follow-up requests. */
    public long getTieringEpoch() {
        return tieringEpoch;
    }

    /** The path (database + table name) of the table to tier. */
    public TablePath getTablePath() {
        return tablePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieringTableAssignment that = (TieringTableAssignment) o;
        return tableId == that.tableId
                && tieringEpoch == that.tieringEpoch
                && tablePath.equals(that.tablePath);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(tableId);
        result = 31 * result + Long.hashCode(tieringEpoch);
        result = 31 * result + tablePath.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TieringTableAssignment{"
                + "tableId="
                + tableId
                + ", tieringEpoch="
                + tieringEpoch
                + ", tablePath="
                + tablePath
                + '}';
    }
}
