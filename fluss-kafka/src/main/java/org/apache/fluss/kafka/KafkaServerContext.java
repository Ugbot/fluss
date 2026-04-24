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

package org.apache.fluss.kafka;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.ClusterMetadataProvider;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.ZooKeeperClient;

import javax.annotation.Nullable;

import java.util.OptionalInt;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Read-only bridge exposing the state a {@link KafkaRequestHandler} needs from the surrounding
 * server. Built once per tablet server at plugin setup and shared across requests.
 */
@Internal
public final class KafkaServerContext {

    private final @Nullable ClusterMetadataProvider metadataCache;
    private final @Nullable MetadataManager metadataManager;
    private final @Nullable CoordinatorGateway coordinatorGateway;
    private final @Nullable ReplicaManager replicaManager;
    private final @Nullable ZooKeeperClient zooKeeperClient;
    private final @Nullable Authorizer authorizer;
    private final String clusterId;
    private final String kafkaDatabase;
    private final Configuration serverConf;

    /**
     * When this context is attached to a real tablet server, the numeric id of that server. {@link
     * Integer#MIN_VALUE} signals "not available" (e.g. testing-gateway-backed handlers).
     */
    private final int ownServerId;

    public KafkaServerContext(
            @Nullable ClusterMetadataProvider metadataCache,
            @Nullable MetadataManager metadataManager,
            @Nullable CoordinatorGateway coordinatorGateway,
            @Nullable ReplicaManager replicaManager,
            @Nullable ZooKeeperClient zooKeeperClient,
            String clusterId,
            String kafkaDatabase) {
        this(
                metadataCache,
                metadataManager,
                coordinatorGateway,
                replicaManager,
                zooKeeperClient,
                null,
                clusterId,
                kafkaDatabase,
                Integer.MIN_VALUE,
                new Configuration());
    }

    public KafkaServerContext(
            @Nullable ClusterMetadataProvider metadataCache,
            @Nullable MetadataManager metadataManager,
            @Nullable CoordinatorGateway coordinatorGateway,
            @Nullable ReplicaManager replicaManager,
            @Nullable ZooKeeperClient zooKeeperClient,
            String clusterId,
            String kafkaDatabase,
            int ownServerId,
            Configuration serverConf) {
        this(
                metadataCache,
                metadataManager,
                coordinatorGateway,
                replicaManager,
                zooKeeperClient,
                null,
                clusterId,
                kafkaDatabase,
                ownServerId,
                serverConf);
    }

    public KafkaServerContext(
            @Nullable ClusterMetadataProvider metadataCache,
            @Nullable MetadataManager metadataManager,
            @Nullable CoordinatorGateway coordinatorGateway,
            @Nullable ReplicaManager replicaManager,
            @Nullable ZooKeeperClient zooKeeperClient,
            @Nullable Authorizer authorizer,
            String clusterId,
            String kafkaDatabase,
            int ownServerId,
            Configuration serverConf) {
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.coordinatorGateway = coordinatorGateway;
        this.replicaManager = replicaManager;
        this.zooKeeperClient = zooKeeperClient;
        this.authorizer = authorizer;
        this.clusterId = checkNotNull(clusterId, "clusterId");
        this.kafkaDatabase = checkNotNull(kafkaDatabase, "kafkaDatabase");
        this.ownServerId = ownServerId;
        this.serverConf = checkNotNull(serverConf, "serverConf");
    }

    public ClusterMetadataProvider metadataCache() {
        return checkNotNull(
                metadataCache,
                "Metadata cache is not available on this server; "
                        + "Kafka metadata APIs require a full TabletServer.");
    }

    public MetadataManager metadataManager() {
        return checkNotNull(
                metadataManager,
                "MetadataManager is not available on this server; "
                        + "Kafka metadata APIs require a full TabletServer.");
    }

    public String clusterId() {
        return clusterId;
    }

    public String kafkaDatabase() {
        return kafkaDatabase;
    }

    public CoordinatorGateway coordinatorGateway() {
        return checkNotNull(
                coordinatorGateway,
                "CoordinatorGateway is not available on this server; "
                        + "Kafka admin APIs require a full TabletServer.");
    }

    public ReplicaManager replicaManager() {
        return checkNotNull(
                replicaManager,
                "ReplicaManager is not available on this server; "
                        + "Kafka Produce/Fetch require a full TabletServer.");
    }

    public ZooKeeperClient zooKeeperClient() {
        return checkNotNull(
                zooKeeperClient,
                "ZooKeeperClient is not available on this server; "
                        + "Kafka durable offsets require a full TabletServer.");
    }

    public boolean hasZooKeeperClient() {
        return zooKeeperClient != null;
    }

    public boolean hasServerState() {
        return metadataCache != null && metadataManager != null;
    }

    public boolean hasCoordinatorGateway() {
        return coordinatorGateway != null;
    }

    public boolean hasReplicaManager() {
        return replicaManager != null;
    }

    /**
     * Returns this tablet server's own id if the Kafka handler is attached to a real tablet server,
     * empty otherwise (e.g. when the plugin is wired against a testing gateway).
     */
    public OptionalInt ownServerId() {
        if (ownServerId == Integer.MIN_VALUE) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(ownServerId);
    }

    /** Returns the full server-side configuration the plugin was started with. */
    public Configuration serverConf() {
        return serverConf;
    }

    /**
     * The ACL enforcement point for this Kafka listener, or {@code null} when the server started
     * with {@code authorizer.enabled=false} or the plugin is bound to a test-only gateway. Handlers
     * must no-op authz when this is {@code null} so that unauthenticated development setups (and
     * the test gateway) continue to work.
     */
    @Nullable
    public Authorizer authorizer() {
        return authorizer;
    }
}
