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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Engine-agnostic client wrapping the Fluss coordinator's {@code lakeTieringHeartbeat} RPC for the
 * standalone tiering service.
 *
 * <p>This is the extraction of the heartbeat/epoch plumbing that {@code TieringSourceEnumerator}
 * embedded inline (its {@code coordinatorGateway} field, {@code flussCoordinatorEpoch} field, and
 * {@code HeartBeatHelper}). All Flink dependencies are removed: it deals only in {@link
 * CoordinatorGateway}, the RPC messages, and the small value types in this package.
 *
 * <p>It tracks the {@link CoordinatorEpoch} obtained from the registration handshake ({@link
 * #register()}) and refreshed on every heartbeat response, and exposes:
 *
 * <ul>
 *   <li>{@link #register()} — the registration handshake (basic heartbeat) returning the
 *       coordinator epoch.
 *   <li>{@link #heartbeat(Map, Map, Map)} — a plain liveness/notification heartbeat (no table
 *       request).
 *   <li>{@link #requestTable(Map, Map, Map)} — a heartbeat that requests a new table to tier,
 *       returning {@link Optional#empty()} when none is available.
 *   <li>{@link #reportFinished(long, long, TieringStats, boolean)} / {@link #reportFailed(long,
 *       long)} — convenience single-table notifications.
 * </ul>
 *
 * <p>The client may either own its {@link RpcClient} (constructed from a {@link Configuration} via
 * {@link #create(Configuration)} — mirroring {@code FlussTableLakeSnapshotCommitter#open()}) or
 * wrap a {@link CoordinatorGateway} the caller already owns (the daemon shares one {@code
 * RpcClient} / gateway across the heartbeat client and the committer). When constructed with an
 * external gateway, {@link #close()} does not close it.
 */
@Internal
public class TieringCoordinatorClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TieringCoordinatorClient.class);

    /**
     * Matches the heartbeat wait timeout used by {@code TieringSourceEnumerator.HeartBeatHelper}.
     */
    private static final long HEARTBEAT_TIMEOUT_MINUTES = 3;

    private final CoordinatorGateway coordinatorGateway;
    private final CoordinatorEpoch coordinatorEpoch;

    /** Non-null only when this client created and therefore owns the underlying RPC client. */
    @Nullable private final RpcClient ownedRpcClient;

    /**
     * Wraps an externally-owned {@link CoordinatorGateway}. The gateway (and its backing {@code
     * RpcClient}) is NOT closed by {@link #close()} — the owner is responsible. Use this in the
     * daemon, which shares one {@code RpcClient} across the heartbeat client and committer.
     */
    public TieringCoordinatorClient(CoordinatorGateway coordinatorGateway) {
        this(coordinatorGateway, null);
    }

    private TieringCoordinatorClient(
            CoordinatorGateway coordinatorGateway, @Nullable RpcClient ownedRpcClient) {
        this.coordinatorGateway =
                checkNotNull(coordinatorGateway, "coordinatorGateway must not be null");
        this.coordinatorEpoch = new CoordinatorEpoch();
        this.ownedRpcClient = ownedRpcClient;
    }

    /**
     * Creates a self-contained client owning its own {@link RpcClient} and {@link
     * CoordinatorGateway}, built from the given configuration. This mirrors {@code
     * FlussTableLakeSnapshotCommitter#open()} and is convenient for tests or processes that do not
     * already hold a gateway. The returned client owns the {@code RpcClient}, so {@link #close()}
     * will close it.
     */
    public static TieringCoordinatorClient create(Configuration flussConf) {
        checkNotNull(flussConf, "flussConf must not be null");
        String clientId = flussConf.getString(ConfigOptions.CLIENT_ID);
        MetricRegistry metricRegistry = MetricRegistry.create(flussConf, null);
        RpcClient rpcClient =
                RpcClient.create(flussConf, new ClientMetricGroup(metricRegistry, clientId));
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        CoordinatorGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        return new TieringCoordinatorClient(gateway, rpcClient);
    }

    /** Returns the holder tracking the Fluss coordinator epoch. */
    public CoordinatorEpoch coordinatorEpoch() {
        return coordinatorEpoch;
    }

    /**
     * Performs the registration handshake with a basic heartbeat and records the coordinator epoch.
     *
     * @return the established coordinator epoch
     */
    public int register() {
        LakeTieringHeartbeatResponse response =
                waitHeartbeatResponse(
                        coordinatorGateway.lakeTieringHeartbeat(
                                HeartbeatRequests.basicHeartBeat()));
        int epoch = response.getCoordinatorEpoch();
        coordinatorEpoch.set(epoch);
        LOG.info("Registered tiering service to Fluss coordinator (epoch={}).", epoch);
        return epoch;
    }

    /**
     * Sends a heartbeat without requesting a new table, carrying the given in-flight / finished /
     * failed table sections. Used for liveness renewal of in-flight tables and to notify the
     * coordinator of completed / failed tables.
     *
     * @return the heartbeat response (e.g. so callers can refresh the coordinator epoch)
     */
    public LakeTieringHeartbeatResponse heartbeat(
            Map<Long, Long> inFlightTableEpochs,
            Map<Long, TieringFinishInfo> finishedTables,
            Map<Long, Long> failedTableEpochs) {
        LakeTieringHeartbeatRequest request =
                HeartbeatRequests.tieringTableHeartBeat(
                        HeartbeatRequests.basicHeartBeat(),
                        inFlightTableEpochs,
                        finishedTables,
                        failedTableEpochs,
                        coordinatorEpoch.get());
        LakeTieringHeartbeatResponse response =
                waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(request));
        coordinatorEpoch.set(response.getCoordinatorEpoch());
        return response;
    }

    /**
     * Sends a heartbeat that also requests a new table to tier, carrying the given in-flight /
     * finished / failed table sections. Returns the assigned table, or {@link Optional#empty()}
     * when the coordinator has no table to hand out this round.
     */
    public Optional<TieringTableAssignment> requestTable(
            Map<Long, Long> inFlightTableEpochs,
            Map<Long, TieringFinishInfo> finishedTables,
            Map<Long, Long> failedTableEpochs) {
        LakeTieringHeartbeatRequest request =
                HeartbeatRequests.heartBeatWithRequestNewTieringTable(
                        HeartbeatRequests.tieringTableHeartBeat(
                                HeartbeatRequests.basicHeartBeat(),
                                inFlightTableEpochs,
                                finishedTables,
                                failedTableEpochs,
                                coordinatorEpoch.get()));
        LakeTieringHeartbeatResponse response =
                waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(request));
        coordinatorEpoch.set(response.getCoordinatorEpoch());
        if (response.hasTieringTable()) {
            PbLakeTieringTableInfo tieringTable = response.getTieringTable();
            TieringTableAssignment assignment =
                    new TieringTableAssignment(
                            tieringTable.getTableId(),
                            tieringTable.getTieringEpoch(),
                            TablePath.of(
                                    tieringTable.getTablePath().getDatabaseName(),
                                    tieringTable.getTablePath().getTableName()));
            LOG.info("Coordinator assigned tiering table {}.", assignment);
            return Optional.of(assignment);
        }
        LOG.debug("No available tiering table found, will poll later.");
        return Optional.empty();
    }

    /**
     * Reports a single table as finished tiering. Convenience over {@link #heartbeat(Map, Map,
     * Map)}.
     *
     * @param tableId the finished table
     * @param tieringEpoch the assigned tiering epoch (for fencing)
     * @param stats stats collected this round (may be {@link TieringStats#UNKNOWN})
     * @param forceFinished whether the table was force-finished due to reaching max duration
     */
    public LakeTieringHeartbeatResponse reportFinished(
            long tableId, long tieringEpoch, TieringStats stats, boolean forceFinished) {
        Map<Long, TieringFinishInfo> finished = new HashMap<>();
        finished.put(tableId, TieringFinishInfo.from(tieringEpoch, forceFinished, stats));
        return heartbeat(
                Collections.<Long, Long>emptyMap(), finished, Collections.<Long, Long>emptyMap());
    }

    /**
     * Reports a single table as failed tiering. Convenience over {@link #heartbeat(Map, Map, Map)};
     * the coordinator re-queues the table.
     *
     * @param tableId the failed table
     * @param tieringEpoch the assigned tiering epoch (for fencing)
     */
    public LakeTieringHeartbeatResponse reportFailed(long tableId, long tieringEpoch) {
        Map<Long, Long> failed = new HashMap<>();
        failed.put(tableId, tieringEpoch);
        return heartbeat(
                Collections.<Long, Long>emptyMap(),
                Collections.<Long, TieringFinishInfo>emptyMap(),
                failed);
    }

    private static LakeTieringHeartbeatResponse waitHeartbeatResponse(
            CompletableFuture<LakeTieringHeartbeatResponse> responseFuture) {
        try {
            return responseFuture.get(HEARTBEAT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (Exception e) {
            LOG.error("Failed to wait for heartbeat response.", e);
            throw new FlussRuntimeException("Failed to wait for heartbeat response.", e);
        }
    }

    @Override
    public void close() {
        if (ownedRpcClient != null) {
            try {
                ownedRpcClient.close();
            } catch (Exception e) {
                LOG.error("Failed to close tiering coordinator RPC client.", e);
            }
        }
    }
}
