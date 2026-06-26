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
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.PbHeartbeatReqForTable;
import org.apache.fluss.rpc.messages.PbLakeTieringStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Static builders for the {@link LakeTieringHeartbeatRequest} variants exchanged with the Fluss
 * coordinator's {@code lakeTieringHeartbeat} RPC.
 *
 * <p>These builders are extracted verbatim (Flink-free) from {@code
 * TieringSourceEnumerator.HeartBeatHelper}. They cover:
 *
 * <ul>
 *   <li>{@link #basicHeartBeat()} — an empty heartbeat used for the registration handshake and as
 *       the base of every other variant.
 *   <li>{@link #heartBeatWithRequestNewTieringTable(LakeTieringHeartbeatRequest)} — flags the
 *       request so the coordinator may hand back a new table to tier.
 *   <li>{@link #tieringTableHeartBeat} — the full per-tick heartbeat carrying in-flight, finished
 *       and failed table sections.
 *   <li>{@link #failedTableHeartBeat} — appends failed tables (also used standalone on shutdown).
 * </ul>
 *
 * <p>Every per-table section carries {@code (tableId, coordinatorEpoch, tieringEpoch)} so the
 * coordinator can fence stale requests.
 */
@Internal
public final class HeartbeatRequests {

    private HeartbeatRequests() {}

    /** A bare heartbeat request, used for the registration handshake and as a base builder. */
    public static LakeTieringHeartbeatRequest basicHeartBeat() {
        return new LakeTieringHeartbeatRequest();
    }

    /**
     * Marks the given heartbeat so the coordinator may assign a new tiering table in its response.
     */
    public static LakeTieringHeartbeatRequest heartBeatWithRequestNewTieringTable(
            LakeTieringHeartbeatRequest heartbeatRequest) {
        heartbeatRequest.setRequestTable(true);
        return heartbeatRequest;
    }

    /**
     * Populates the given heartbeat with the in-flight, finished and failed table sections.
     *
     * @param heartbeatRequest the base request to mutate and return
     * @param tieringTableEpochs in-flight tables (tableId -&gt; tieringEpoch) being heart-beat to
     *     keep them alive on the coordinator
     * @param finishedTables tables finished this round (tableId -&gt; finish info)
     * @param failedTableEpochs tables failed this round (tableId -&gt; tieringEpoch)
     * @param coordinatorEpoch the current Fluss coordinator epoch, echoed on every table entry
     */
    public static LakeTieringHeartbeatRequest tieringTableHeartBeat(
            LakeTieringHeartbeatRequest heartbeatRequest,
            Map<Long, Long> tieringTableEpochs,
            Map<Long, TieringFinishInfo> finishedTables,
            Map<Long, Long> failedTableEpochs,
            int coordinatorEpoch) {
        if (!tieringTableEpochs.isEmpty()) {
            heartbeatRequest.addAllTieringTables(
                    toPbHeartbeatReqForTable(tieringTableEpochs, coordinatorEpoch));
        }
        if (!finishedTables.isEmpty()) {
            Set<Long> forceFinishedTables = new HashSet<>();
            List<PbHeartbeatReqForTable> finishedTableReqs = new ArrayList<>();
            finishedTables.forEach(
                    (tableId, tieringFinishInfo) -> {
                        if (tieringFinishInfo.isForceFinished()) {
                            forceFinishedTables.add(tableId);
                        }
                        PbHeartbeatReqForTable pbHeartbeatReqForTable =
                                new PbHeartbeatReqForTable()
                                        .setTableId(tableId)
                                        .setCoordinatorEpoch(coordinatorEpoch)
                                        .setTieringEpoch(tieringFinishInfo.getTieringEpoch());
                        TieringStats stats = tieringFinishInfo.getStats();
                        if (stats.isAvailableStats()) {
                            PbLakeTieringStats pbLakeTieringStats = new PbLakeTieringStats();
                            if (stats.getFileSize() != null) {
                                pbLakeTieringStats.setFileSize(stats.getFileSize());
                            }
                            if (stats.getRecordCount() != null) {
                                pbLakeTieringStats.setRecordCount(stats.getRecordCount());
                            }
                            pbHeartbeatReqForTable.setLakeTieringStats(pbLakeTieringStats);
                        }
                        finishedTableReqs.add(pbHeartbeatReqForTable);
                    });
            heartbeatRequest.addAllFinishedTables(finishedTableReqs);
            for (long forceFinishedTableId : forceFinishedTables) {
                heartbeatRequest.addForceFinishedTable(forceFinishedTableId);
            }
        }
        // add failed tiering table to heart beat request
        return failedTableHeartBeat(heartbeatRequest, failedTableEpochs, coordinatorEpoch);
    }

    /**
     * Appends the failed table section to the given heartbeat. Also usable standalone, e.g. on
     * tiering-service shutdown to promptly re-queue in-flight tables on the coordinator.
     */
    public static LakeTieringHeartbeatRequest failedTableHeartBeat(
            LakeTieringHeartbeatRequest lakeTieringHeartbeatRequest,
            Map<Long, Long> failedTieringTableEpochs,
            int coordinatorEpoch) {
        if (!failedTieringTableEpochs.isEmpty()) {
            lakeTieringHeartbeatRequest.addAllFailedTables(
                    toPbHeartbeatReqForTable(failedTieringTableEpochs, coordinatorEpoch));
        }
        return lakeTieringHeartbeatRequest;
    }

    private static Set<PbHeartbeatReqForTable> toPbHeartbeatReqForTable(
            Map<Long, Long> tableEpochs, int coordinatorEpoch) {
        return tableEpochs.entrySet().stream()
                .map(
                        tieringTableEpoch ->
                                new PbHeartbeatReqForTable()
                                        .setTableId(tieringTableEpoch.getKey())
                                        .setCoordinatorEpoch(coordinatorEpoch)
                                        .setTieringEpoch(tieringTableEpoch.getValue()))
                .collect(Collectors.toSet());
    }
}
