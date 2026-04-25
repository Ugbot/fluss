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

package org.apache.fluss.catalog;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CoordinatorLeaderBootstrap} that wires up the Fluss catalog service on the elected
 * coordinator leader. Starts whenever any consumer of the catalog is enabled — currently the Kafka
 * Schema Registry ({@link ConfigOptions#KAFKA_SCHEMA_REGISTRY_ENABLED}) and the Iceberg REST
 * Catalog ({@link ConfigOptions#ICEBERG_REST_ENABLED}). Future projections (Flink multi-format
 * catalog, …) will extend this gate.
 *
 * <p>Runs at priority 10 so it registers the service before the higher-priority projection
 * bootstraps (Kafka SR, Iceberg REST) look for it via {@link CatalogServices#current()}.
 */
@Internal
public final class FlussCatalogBootstrap implements CoordinatorLeaderBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(FlussCatalogBootstrap.class);

    /** Fluss listener name we bootstrap the catalog client against. */
    public static final String FLUSS_LISTENER = "FLUSS";

    @Override
    public String name() {
        return "Fluss Catalog";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zk,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache,
            List<Endpoint> coordinatorBindEndpoints)
            throws Exception {
        if (!catalogRequired(conf)) {
            return null;
        }
        List<String> bootstrap = buildBootstrap(metadataCache, coordinatorBindEndpoints);
        if (bootstrap.isEmpty()) {
            LOG.warn(
                    "No FLUSS listener available for catalog bootstrap; catalog service cannot start. "
                            + "Will retry on next leader election cycle.");
            return null;
        }
        Configuration clientConf = new Configuration(conf);
        clientConf.set(ConfigOptions.BOOTSTRAP_SERVERS, bootstrap);
        // Open the Connection lazily — this bootstrap runs from inside
        // initCoordinatorLeader before the coordinator has announced leadership, so eager
        // client init would hit NotCoordinatorLeaderException. First real SR request triggers
        // a Connection at a point where the coordinator is serving metadata normally.
        FlussCatalogService service =
                new FlussCatalogService(() -> ConnectionFactory.createConnection(clientConf));
        CatalogServices.set(service);
        LOG.info(
                "Fluss catalog service registered (lazy bootstrap={}); connection opens on first use.",
                bootstrap);
        return () -> {
            CatalogServices.clear();
            service.close();
        };
    }

    /** The catalog starts when any of its consumers is enabled. */
    private static boolean catalogRequired(Configuration conf) {
        boolean srEnabled =
                conf.getBoolean(ConfigOptions.KAFKA_ENABLED)
                        && conf.getBoolean(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED);
        boolean icebergRestEnabled = conf.getBoolean(ConfigOptions.ICEBERG_REST_ENABLED);
        // The Kafka transaction coordinator (Phase J) needs the catalog for __kafka_txn_state__
        // and __kafka_producer_ids__; it's hosted whenever the Kafka bolt-on is enabled.
        boolean kafkaTxnEnabled = conf.getBoolean(ConfigOptions.KAFKA_ENABLED);
        return srEnabled || icebergRestEnabled || kafkaTxnEnabled;
    }

    private static List<String> buildBootstrap(
            ServerMetadataCache cache, List<Endpoint> coordinatorBindEndpoints) {
        List<String> out = new ArrayList<>();
        // Prefer tablet-server FLUSS endpoints — they host the PK tables directly.
        for (ServerNode node : cache.getAllAliveTabletServers(FLUSS_LISTENER).values()) {
            out.add(node.host() + ":" + node.port());
        }
        if (out.isEmpty()) {
            // Fall back to this coordinator's own FLUSS endpoint. The Fluss client will
            // bootstrap cluster metadata from here and discover tablet servers before the first
            // data RPC. Matters at leader bootstrap when tablet-server registrations may not
            // yet be visible to the cache.
            for (Endpoint e : coordinatorBindEndpoints) {
                if (FLUSS_LISTENER.equals(e.getListenerName())) {
                    String host = e.getHost();
                    if (host == null || host.isEmpty() || "0.0.0.0".equals(host)) {
                        host = "localhost";
                    }
                    out.add(host + ":" + e.getPort());
                }
            }
        }
        return out;
    }
}
