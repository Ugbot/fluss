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

package org.apache.fluss.iceberg.rest;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.List;

/**
 * {@link CoordinatorLeaderBootstrap} that stands up the Iceberg REST Catalog HTTP listener on the
 * elected coordinator leader. Requires the {@link CatalogService} to be running (started by {@code
 * FlussCatalogBootstrap} at priority 10); fails fast if absent.
 *
 * <p>Runs at priority 100 — same tier as the Kafka Schema Registry bootstrap — because both are
 * projections over the shared catalog and neither has any ordering dependency on the other.
 */
@Internal
public final class IcebergRestBootstrap implements CoordinatorLeaderBootstrap {

    @Override
    public String name() {
        return "Iceberg REST Catalog";
    }

    @Override
    public int priority() {
        // Run after FlussCatalogBootstrap (priority 10) so the catalog is already registered.
        return 100;
    }

    @Override
    public AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache,
            List<Endpoint> coordinatorBindEndpoints)
            throws Exception {
        if (!conf.getBoolean(ConfigOptions.ICEBERG_REST_ENABLED)) {
            return null;
        }
        CatalogService catalog =
                CatalogServices.current()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Iceberg REST Catalog is enabled but the Fluss "
                                                        + "catalog service is not running. Ensure "
                                                        + "FlussCatalogBootstrap is on the "
                                                        + "classpath and not self-gated off."));

        String host = conf.get(ConfigOptions.ICEBERG_REST_HOST);
        int port = conf.get(ConfigOptions.ICEBERG_REST_PORT);
        IcebergRestHttpServer server = new IcebergRestHttpServer(host, port, catalog);
        server.start();
        return server;
    }
}
