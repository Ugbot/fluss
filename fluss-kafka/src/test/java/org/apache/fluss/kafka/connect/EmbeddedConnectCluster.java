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

package org.apache.fluss.kafka.connect;

import org.apache.fluss.utils.FileUtils;

import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.util.FutureCallback;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * In-JVM Kafka Connect worker wrapper used by the {@code fluss-kafka} test suite (see {@code
 * dev-docs/design/0017-kafka-connect-and-streams-test-harness.md}).
 *
 * <p>Defaults to {@link ConnectStandalone} to keep start-up in the 3-5 s range; a constructor flag
 * opts in to {@link ConnectDistributed} for the MM2 IT which requires cluster-mode coordination.
 *
 * <p>Each instance owns a per-test state directory under {@code target/connect-<uuid>/}; the
 * directory is deleted on {@link #close()} so parallel Surefire forks cannot collide on standalone
 * offset files or distributed-mode scratch space. The bootstrap URL is passed at construction time
 * — normally the KAFKA listener of a {@code FlussClusterExtension}.
 */
public final class EmbeddedConnectCluster implements AutoCloseable {

    /** Awaitable deadline for async herder callbacks. Connect workers start cold in 3-5 s. */
    private static final long DEFAULT_TIMEOUT_SECONDS = 30L;

    private final String bootstrapServers;
    private final Map<String, String> extraWorkerProps;
    private final boolean distributed;
    private final Path stateDir;
    private final Path offsetsFile;
    private final String workerId;

    private Connect<?> connect;
    private Herder herder;

    /** Starts a standalone worker against the supplied bootstrap URL. */
    public EmbeddedConnectCluster(String bootstrapServers) {
        this(bootstrapServers, new HashMap<>(), false);
    }

    /** Starts a standalone worker with caller-supplied overrides on top of the harness defaults. */
    public EmbeddedConnectCluster(String bootstrapServers, Map<String, String> extraWorkerProps) {
        this(bootstrapServers, extraWorkerProps, false);
    }

    /** Full constructor; {@code distributed=true} selects the cluster-mode worker. */
    public EmbeddedConnectCluster(
            String bootstrapServers, Map<String, String> extraWorkerProps, boolean distributed) {
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers must be non-empty");
        }
        this.bootstrapServers = bootstrapServers;
        this.extraWorkerProps = new HashMap<>(extraWorkerProps);
        this.distributed = distributed;
        String uuid = UUID.randomUUID().toString();
        this.stateDir = Path.of("target", "connect-" + uuid);
        this.offsetsFile = stateDir.resolve("connect.offsets");
        this.workerId = "fluss-it-" + uuid;
    }

    /** Starts the embedded Connect worker and blocks until its herder reports ready. */
    public void start() {
        if (connect != null) {
            throw new IllegalStateException("EmbeddedConnectCluster already started");
        }
        try {
            java.nio.file.Files.createDirectories(stateDir);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to create Connect state dir: " + stateDir, e);
        }

        Map<String, String> workerProps = buildWorkerProps();
        if (distributed) {
            this.connect = new ConnectDistributed().startConnect(workerProps);
        } else {
            this.connect = new ConnectStandalone().startConnect(workerProps);
        }
        this.herder = connect.herder();

        // AbstractConnectCli.startConnect(Map) returns a Connect whose Herder has been started but
        // not yet had ready() invoked — the CLI flips that bit only when run() reaches the
        // processExtraArgs hook. Embedded callers (us) must flip it manually so isReady() / the
        // distributed RebalanceListener accept connector configs.
        if (herder instanceof StandaloneHerder) {
            ((StandaloneHerder) herder).ready();
        } else if (herder instanceof DistributedHerder) {
            // DistributedHerder.ready() is package-private but the herder calls it from its own
            // ConfigBackingStore listener after the worker joins the group; we don't need to drive
            // it manually. isReady() will flip true once the rebalance completes.
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEFAULT_TIMEOUT_SECONDS);
        while (!herder.isReady()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "Connect herder did not become ready within "
                                + DEFAULT_TIMEOUT_SECONDS
                                + "s (distributed="
                                + distributed
                                + ")");
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for herder readiness", ie);
            }
        }
    }

    /**
     * Deploys a connector and waits for its config to be accepted by the herder. The returned
     * {@link ConnectorInfo} is the snapshot the herder stored; task state must be queried via
     * {@link #connectorStatus(String)}.
     */
    public ConnectorInfo startConnector(String name, Map<String, String> connectorConfig) {
        ensureStarted();
        Map<String, String> config = new HashMap<>(connectorConfig);
        config.put("name", name);
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(name, config, false, cb);
        try {
            return cb.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS).result();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while starting connector " + name, ie);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to start connector " + name, e);
        }
    }

    /** Deletes a connector's config; blocks until the herder confirms removal. */
    public void stopConnector(String name) {
        ensureStarted();
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.deleteConnectorConfig(name, cb);
        try {
            cb.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while stopping connector " + name, ie);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to stop connector " + name, e);
        }
    }

    /** Synchronous {@code connectorInfo}; returns the immutable config snapshot. */
    public ConnectorInfo connectorInfo(String name) {
        ensureStarted();
        return herder.connectorInfo(name);
    }

    /** Synchronous {@code connectorStatus}; returns the dynamic per-task state snapshot. */
    public ConnectorStateInfo connectorStatus(String name) {
        ensureStarted();
        return herder.connectorStatus(name);
    }

    /** REST base URI; only meaningful after {@link #start()} has returned. */
    public URI workerUri() {
        ensureStarted();
        return connect.rest().advertisedUrl();
    }

    /** Stable worker id used as the Connect group id / client id prefix. */
    public String workerId() {
        return workerId;
    }

    /** Absolute path to the per-test state directory. */
    public Path stateDir() {
        return stateDir;
    }

    /** True if this harness was constructed in distributed (cluster) mode. */
    public boolean isDistributed() {
        return distributed;
    }

    @Override
    public void close() {
        try {
            if (connect != null) {
                connect.stop();
                connect.awaitStop();
            }
        } finally {
            connect = null;
            herder = null;
            FileUtils.deleteDirectoryQuietly(stateDir.toFile());
        }
    }

    // ------------------------------------------------------------------
    //  Worker config assembly
    // ------------------------------------------------------------------

    private Map<String, String> buildWorkerProps() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");
        props.put("offset.flush.interval.ms", "500");
        props.put("plugin.path", "");
        // Pin the Jetty listener to a random free port; advertisedUrl() will resolve it.
        props.put("listeners", "HTTP://127.0.0.1:0");
        props.put("rest.advertised.host.name", "127.0.0.1");
        // Consumer defaults wired from the top-level worker properties propagate to connectors.
        props.put("consumer.auto.offset.reset", "earliest");

        if (distributed) {
            // Distributed mode requires a group id plus three compacted storage topics. Use a
            // per-test suffix so parallel forks cannot collide.
            String suffix = workerId;
            props.put(DistributedConfig.GROUP_ID_CONFIG, "connect-cluster-" + suffix);
            props.put("config.storage.topic", "connect-configs-" + suffix);
            props.put("offset.storage.topic", "connect-offsets-" + suffix);
            props.put("status.storage.topic", "connect-status-" + suffix);
            props.put("config.storage.replication.factor", "1");
            props.put("offset.storage.replication.factor", "1");
            props.put("status.storage.replication.factor", "1");
            props.put("offset.storage.partitions", "3");
            props.put("status.storage.partitions", "3");
        } else {
            props.put(
                    StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                    offsetsFile.toAbsolutePath().toString());
        }

        // Caller-supplied overrides win (except the state-dir-sensitive ones, which they should
        // not be setting anyway).
        props.putAll(extraWorkerProps);
        return props;
    }

    private void ensureStarted() {
        if (connect == null || herder == null) {
            throw new IllegalStateException("EmbeddedConnectCluster not started");
        }
    }

    // Package-private accessor for the underlying state-dir File, mirroring FlussClusterExtension
    // style where tests occasionally assert on materialised files.
    File stateDirFile() {
        return stateDir.toFile();
    }
}
