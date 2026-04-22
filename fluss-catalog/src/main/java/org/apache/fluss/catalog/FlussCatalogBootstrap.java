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
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
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
 * coordinator leader. Gated by {@link ConfigOptions#CATALOG_SERVICE_ENABLED}; no-op when the
 * feature flag is off.
 *
 * <p>On start: opens a Fluss client {@link Connection} against the FLUSS-listener endpoints of
 * alive tablet servers, constructs a {@link FlussCatalogService}, and registers it as the ambient
 * instance consumed by in-process projections (Kafka SR service looks it up here).
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
    public AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zk,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache)
            throws Exception {
        if (!conf.getBoolean(ConfigOptions.CATALOG_SERVICE_ENABLED)) {
            return null;
        }
        List<String> bootstrap = buildBootstrap(metadataCache);
        if (bootstrap.isEmpty()) {
            LOG.warn(
                    "No alive tablet server on FLUSS listener; catalog service cannot start. "
                            + "Will retry on next leader election cycle.");
            return null;
        }
        Configuration clientConf = new Configuration(conf);
        clientConf.set(ConfigOptions.BOOTSTRAP_SERVERS, bootstrap);
        Connection connection = ConnectionFactory.createConnection(clientConf);
        FlussCatalogService service = new FlussCatalogService(connection);
        CatalogServices.set(service);
        LOG.info("Fluss catalog service started (bootstrap={})", bootstrap);
        return () -> {
            CatalogServices.clear();
            try {
                service.close();
            } finally {
                connection.close();
            }
        };
    }

    private static List<String> buildBootstrap(ServerMetadataCache cache) {
        List<String> out = new ArrayList<>();
        for (ServerNode node : cache.getAllAliveTabletServers(FLUSS_LISTENER).values()) {
            out.add(node.host() + ":" + node.port());
        }
        return out;
    }
}
