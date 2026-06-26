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

package org.apache.fluss.tiering.service;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.tiering.committer.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.lake.tiering.committer.TableTieringCommitter;
import org.apache.fluss.lake.tiering.coordinator.TieringCoordinatorClient;
import org.apache.fluss.lake.tiering.coordinator.TieringFinishInfo;
import org.apache.fluss.lake.tiering.coordinator.TieringTableAssignment;
import org.apache.fluss.lake.tiering.reader.BucketWriteResult;
import org.apache.fluss.lake.tiering.reader.TieringReaderMetrics;
import org.apache.fluss.lake.tiering.reader.TieringTableReader;
import org.apache.fluss.lake.tiering.split.TieringSplit;
import org.apache.fluss.lake.tiering.split.TieringSplitGenerator;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The standalone, engine-free tiering daemon.
 *
 * <p>This replaces the Flink {@code TieringSourceEnumerator} (the heartbeat / epoch / scheduling
 * brain) and {@code TieringSourceReader} (the connection owner) with a single process driven by a
 * {@link ScheduledExecutorService}. It owns one {@link Connection}, one {@link RpcClient} / {@link
 * CoordinatorGateway}, one {@link Admin}, a {@link TieringSplitGenerator}, the lake {@link
 * LakeTieringFactory} (loaded via the SPI from the configured data lake format and {@link
 * PluginManager}), a {@link FlussTableLakeSnapshotCommitter} / {@link TableTieringCommitter}, and a
 * {@link TieringWorkerPool}.
 *
 * <p>Lifecycle (mirrors {@code TieringSourceEnumerator}):
 *
 * <ol>
 *   <li>{@link #start()} performs the registration handshake (a basic heartbeat) to obtain the
 *       coordinator epoch, then schedules the periodic poll tick at {@code
 *       tiering.poll.table.interval}.
 *   <li>Each poll tick: drain the pending {@code finished} / {@code failed} tables into a
 *       heartbeat, request a new table iff a worker is free, and refresh the coordinator epoch from
 *       the response. When the coordinator assigns a table, submit a tiering unit to the worker
 *       pool.
 *   <li>The tiering unit generates the table's splits, tiers them via {@link TieringTableReader},
 *       commits via {@link TableTieringCommitter}, and records the table as finished (or failed on
 *       error). A per-table force-finish timer fires after {@code dataLakeFreshness} and calls
 *       {@link TieringTableReader#handleTableReachTieringMaxDuration(long)} so the reader
 *       force-completes whatever is tiered so far.
 *   <li>{@link #close()} moves all in-flight tables to {@code failed} and sends one final heartbeat
 *       so the coordinator re-queues them promptly, then closes the worker pool, admin, connection
 *       and RPC client.
 * </ol>
 *
 * <p><b>Epoch fencing &amp; heartbeat liveness.</b> Every per-table notification carries the
 * assigned {@code tieringEpoch}; in-flight tables are heart-beaten on every tick so the coordinator
 * keeps them out of {@code TIERING_SERVICE_TIMEOUT_MS}. The poll interval is therefore required to
 * stay well under that 2 minute timeout (enforced by the default of {@link
 * TieringServiceOptions#POLL_INTERVAL}).
 */
public class TieringService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TieringService.class);

    private final Configuration flussConfig;
    private final Configuration dataLakeConfig;
    private final Configuration lakeTieringConfig;
    private final String dataLakeFormat;
    @Nullable private final PluginManager pluginManager;

    private final Duration pollInterval;
    private final Duration pollTimeout;
    private final int maxConcurrentTables;

    // ------------------------------------------------------------------------
    // shared state, populated in start()
    // ------------------------------------------------------------------------

    private Connection connection;
    private Admin admin;
    private RpcClient rpcClient;
    private MetricRegistry metricRegistry;
    private TieringCoordinatorClient coordinatorClient;
    private FlussTableLakeSnapshotCommitter flussCommitter;
    private TieringSplitGenerator splitGenerator;
    private LakeTieringFactory<?, ?> lakeTieringFactory;

    @SuppressWarnings("rawtypes")
    private TableTieringCommitter tableTieringCommitter;

    private ScheduledExecutorService poller;
    private TieringWorkerPool workerPool;

    /**
     * Tables currently being tiered, mapped to their assigned tiering epoch. Heart-beaten on every
     * tick for liveness. Shared between the poller thread and worker threads, hence concurrent.
     */
    private final Map<Long, Long> inFlightTableEpochs = new ConcurrentHashMap<>();

    /** Tables that finished tiering since the previous heartbeat (drained each tick). */
    private final Map<Long, TieringFinishInfo> finishedTables = new ConcurrentHashMap<>();

    /** Tables that failed tiering since the previous heartbeat (drained each tick). */
    private final Map<Long, Long> failedTableEpochs = new ConcurrentHashMap<>();

    /** Force-finish timers keyed by table id, so they can be cancelled when a table finishes. */
    private final Map<Long, ScheduledFuture<?>> forceFinishTimers = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    public TieringService(
            Configuration flussConfig,
            Configuration dataLakeConfig,
            Configuration lakeTieringConfig,
            String dataLakeFormat,
            @Nullable PluginManager pluginManager) {
        this.flussConfig = checkNotNull(flussConfig, "flussConfig must not be null.");
        this.dataLakeConfig = checkNotNull(dataLakeConfig, "dataLakeConfig must not be null.");
        this.lakeTieringConfig =
                checkNotNull(lakeTieringConfig, "lakeTieringConfig must not be null.");
        this.dataLakeFormat = checkNotNull(dataLakeFormat, "dataLakeFormat must not be null.");
        this.pluginManager = pluginManager;
        this.pollInterval = flussConfig.get(TieringServiceOptions.POLL_INTERVAL);
        this.pollTimeout = flussConfig.get(TieringServiceOptions.POLL_TIMEOUT);
        this.maxConcurrentTables = flussConfig.get(TieringServiceOptions.MAX_CONCURRENT_TABLES);
    }

    /**
     * Boots the daemon: builds the shared Fluss / lake resources, performs the registration
     * handshake and schedules the periodic poll tick. Idempotent only in the sense that it refuses
     * to start twice.
     */
    public void start() throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("TieringService is already started.");
        }
        LOG.info(
                "Starting standalone tiering service for data lake format '{}' "
                        + "(pollInterval={}, pollTimeout={}, maxConcurrentTables={}).",
                dataLakeFormat,
                pollInterval,
                pollTimeout,
                maxConcurrentTables);

        // shared Fluss client resources
        this.connection = ConnectionFactory.createConnection(flussConfig);
        this.admin = connection.getAdmin();

        // a single shared RpcClient + CoordinatorGateway for the heartbeat client; the committer
        // owns its own RpcClient via open() (it mirrors FlussTableLakeSnapshotCommitter#open()).
        String clientId = flussConfig.getString(ConfigOptions.CLIENT_ID);
        this.metricRegistry = MetricRegistry.create(flussConfig, null);
        this.rpcClient =
                RpcClient.create(flussConfig, new ClientMetricGroup(metricRegistry, clientId));
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConfig, rpcClient);
        CoordinatorGateway coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        this.coordinatorClient = new TieringCoordinatorClient(coordinatorGateway);

        // the Fluss-side committer (its own RpcClient/gateway, two-phase commit RPCs)
        this.flussCommitter = new FlussTableLakeSnapshotCommitter(flussConfig);
        this.flussCommitter.open();

        // the lake factory, loaded via the SPI from the configured data lake format + PluginManager
        this.lakeTieringFactory = loadLakeTieringFactory();

        this.splitGenerator = new TieringSplitGenerator(admin);
        this.tableTieringCommitter =
                createTableTieringCommitter(lakeTieringFactory, flussCommitter);

        this.workerPool = new TieringWorkerPool(maxConcurrentTables);

        // registration handshake: obtain the coordinator epoch before the first table request
        coordinatorClient.register();

        // schedule the periodic poll tick
        this.poller =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "fluss-tiering-poller");
                            t.setDaemon(true);
                            return t;
                        });
        long periodMs = pollInterval.toMillis();
        poller.scheduleWithFixedDelay(this::pollTick, 0L, periodMs, TimeUnit.MILLISECONDS);
        LOG.info("Tiering service started; polling every {} ms.", periodMs);
    }

    /**
     * One poll tick: drain the finished / failed sections, optionally request a new table iff a
     * worker is free, and (on assignment) submit a tiering unit to the worker pool. Runs on the
     * single-threaded poller. Never throws — a failure here must not kill the scheduled task.
     */
    private void pollTick() {
        if (!running.get()) {
            return;
        }
        try {
            // snapshot + drain the finished / failed sections so the coordinator is notified
            // exactly once per completion.
            Map<Long, TieringFinishInfo> finishedSnapshot = drain(finishedTables);
            Map<Long, Long> failedSnapshot = drain(failedTableEpochs);
            // in-flight tables are heart-beaten (not drained) for liveness renewal.
            Map<Long, Long> inFlightSnapshot = new HashMap<>(inFlightTableEpochs);

            boolean requestNewTable = workerPool.hasFreeWorker();
            Optional<TieringTableAssignment> assignment;
            if (requestNewTable) {
                assignment =
                        coordinatorClient.requestTable(
                                inFlightSnapshot, finishedSnapshot, failedSnapshot);
            } else {
                // no free worker: just heartbeat (liveness + notifications), do not request a table
                coordinatorClient.heartbeat(inFlightSnapshot, finishedSnapshot, failedSnapshot);
                assignment = Optional.empty();
            }

            if (assignment.isPresent()) {
                submitTieringUnit(assignment.get());
            }
        } catch (Throwable t) {
            // a failed heartbeat / request must not stop the poller; log and retry next tick.
            LOG.warn("Tiering poll tick failed; will retry on the next tick.", t);
        }
    }

    /**
     * Submits the assigned table to the worker pool. The worker generates the table's splits, tiers
     * them, commits them and records the table as finished (or failed on any error). The table is
     * tracked as in-flight (for heartbeat liveness) for the entire duration.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void submitTieringUnit(TieringTableAssignment assignment) {
        long tableId = assignment.getTableId();
        long tieringEpoch = assignment.getTieringEpoch();
        TablePath tablePath = assignment.getTablePath();

        // mark in-flight before the worker starts so the very next tick already renews liveness.
        inFlightTableEpochs.put(tableId, tieringEpoch);

        workerPool.submit(
                () -> {
                    TieringReaderMetrics readerMetrics = new TieringReaderMetrics();
                    TieringTableReader reader = null;
                    ScheduledFuture<?> forceFinishTimer = null;
                    try {
                        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                        List<TieringSplit> splits = splitGenerator.generateTableSplits(tableInfo);

                        if (splits.isEmpty()) {
                            // matches the enumerator's empty-splits short-circuit: nothing to tier,
                            // report finished immediately (non force-finished).
                            LOG.info(
                                    "No splits to tier for table {} (id {}); reporting finished.",
                                    tablePath,
                                    tableId);
                            markFinished(tableId, tieringEpoch, null, false);
                            return;
                        }

                        // populate the per-round split metadata (numberOfSplits / splitIndex /
                        // round timestamp) exactly as the enumerator does before handing splits
                        // to the reader.
                        List<TieringSplit> splitsWithMetadata =
                                populateTieringRoundMetadata(splits);

                        reader =
                                new TieringTableReader(
                                        connection,
                                        lakeTieringFactory,
                                        Thread.currentThread().getContextClassLoader(),
                                        pollTimeout,
                                        readerMetrics);

                        // schedule the force-finish timer: when the freshness window elapses, ask
                        // the reader to force-complete whatever is tiered so far.
                        final TieringTableReader readerRef = reader;
                        long freshnessMs =
                                tableInfo.getTableConfig().getDataLakeFreshness().toMillis();
                        forceFinishTimer =
                                poller.schedule(
                                        () -> readerRef.handleTableReachTieringMaxDuration(tableId),
                                        freshnessMs,
                                        TimeUnit.MILLISECONDS);
                        forceFinishTimers.put(tableId, forceFinishTimer);

                        List<BucketWriteResult> results =
                                reader.tierTable(tablePath, tableId, splitsWithMetadata);

                        // whether the round was force-finished: the timer fired (i.e. it is no
                        // longer pending and did not get cancelled).
                        boolean forceFinished = forceFinishTimer.isDone();

                        TableTieringCommitter.CommitResult commitResult =
                                tableTieringCommitter.commit(tableId, tablePath, results);
                        TieringStats stats = (TieringStats) commitResult.stats();
                        markFinished(tableId, tieringEpoch, stats, forceFinished);
                    } catch (Throwable t) {
                        LOG.warn(
                                "Failed to tier table {} (id {}); reporting failed.",
                                tablePath,
                                tableId,
                                ExceptionUtils.stripExecutionException(t));
                        markFailed(tableId, tieringEpoch);
                    } finally {
                        if (forceFinishTimer != null) {
                            forceFinishTimer.cancel(false);
                        }
                        forceFinishTimers.remove(tableId);
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to close tiering reader for table {}.", tableId, e);
                            }
                        }
                    }
                });
    }

    /**
     * Populates the per-round metadata (numberOfSplits / splitIndex / round timestamp) on the
     * generated splits, mirroring {@code TieringSourceEnumerator.populateTieringRoundMetadata}.
     */
    private static List<TieringSplit> populateTieringRoundMetadata(List<TieringSplit> splits) {
        int numberOfSplits = splits.size();
        long tieringRoundTimestamp = System.currentTimeMillis();
        List<TieringSplit> splitsWithMetadata = new ArrayList<>(numberOfSplits);
        for (int splitIndex = 0; splitIndex < numberOfSplits; splitIndex++) {
            splitsWithMetadata.add(
                    splits.get(splitIndex).copy(numberOfSplits, splitIndex, tieringRoundTimestamp));
        }
        return splitsWithMetadata;
    }

    private void markFinished(
            long tableId, long tieringEpoch, @Nullable TieringStats stats, boolean forceFinished) {
        // record the result first, then stop tracking as in-flight, so a concurrent tick can never
        // observe the table as neither in-flight nor finished.
        finishedTables.put(tableId, TieringFinishInfo.from(tieringEpoch, forceFinished, stats));
        inFlightTableEpochs.remove(tableId);
    }

    private void markFailed(long tableId, long tieringEpoch) {
        failedTableEpochs.put(tableId, tieringEpoch);
        inFlightTableEpochs.remove(tableId);
    }

    /** Atomically removes and returns a copy of every entry currently in {@code source}. */
    private static <V> Map<Long, V> drain(Map<Long, V> source) {
        Map<Long, V> snapshot = new HashMap<>();
        for (Map.Entry<Long, V> entry : source.entrySet()) {
            V value = source.remove(entry.getKey());
            if (value != null) {
                snapshot.put(entry.getKey(), value);
            }
        }
        return snapshot;
    }

    private LakeTieringFactory<?, ?> loadLakeTieringFactory() {
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat, pluginManager);
        LakeStorage lakeStorage = checkNotNull(lakeStoragePlugin).createLakeStorage(dataLakeConfig);
        LakeTieringFactory<?, ?> factory = lakeStorage.createLakeTieringFactory();
        LOG.info(
                "Loaded LakeTieringFactory {} for data lake format '{}'.",
                factory.getClass().getName(),
                dataLakeFormat);
        return factory;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private TableTieringCommitter createTableTieringCommitter(
            LakeTieringFactory<?, ?> factory, FlussTableLakeSnapshotCommitter committer) {
        return new TableTieringCommitter(flussConfig, lakeTieringConfig, factory, admin, committer);
    }

    /**
     * Stops the daemon and releases all resources. Best-effort re-queues in-flight tables by
     * reporting them as failed in a final heartbeat (mirroring the enumerator's {@code close()}),
     * so the coordinator does not have to wait for the tiering-service timeout to re-schedule them.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info("Closing tiering service.");

        // stop scheduling new ticks / timers first
        if (poller != null) {
            poller.shutdownNow();
        }

        // stop accepting / draining workers; in-flight workers are interrupted by the pool's
        // forced shutdown after the grace period.
        if (workerPool != null) {
            workerPool.close();
        }

        // re-queue everything still in flight (and anything not yet reported) by failing it.
        try {
            if (coordinatorClient != null) {
                Map<Long, Long> toFail = new HashMap<>(failedTableEpochs);
                toFail.putAll(inFlightTableEpochs);
                Map<Long, TieringFinishInfo> finishedSnapshot = new HashMap<>(finishedTables);
                if (!toFail.isEmpty() || !finishedSnapshot.isEmpty()) {
                    coordinatorClient.heartbeat(new HashMap<>(), finishedSnapshot, toFail);
                }
            }
        } catch (Throwable t) {
            LOG.warn("Failed to send final heartbeat re-queuing in-flight tables.", t);
        } finally {
            inFlightTableEpochs.clear();
            finishedTables.clear();
            failedTableEpochs.clear();
            forceFinishTimers.clear();
        }

        closeQuietly(coordinatorClient, "coordinator client");
        closeQuietly(flussCommitter, "fluss committer");
        closeQuietly(admin, "admin");
        closeQuietly(connection, "connection");
        if (rpcClient != null) {
            try {
                rpcClient.close();
            } catch (Exception e) {
                LOG.warn("Failed to close RPC client.", e);
            }
        }
        if (metricRegistry != null) {
            try {
                metricRegistry.close();
            } catch (Exception e) {
                LOG.warn("Failed to close metric registry.", e);
            }
        }
        LOG.info("Tiering service closed.");
    }

    /** Returns whether the daemon is currently running. */
    public boolean isRunning() {
        return running.get();
    }

    private static void closeQuietly(@Nullable AutoCloseable closeable, String what) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Failed to close {}.", what, e);
        }
    }
}
