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

package org.apache.fluss.kafka.sr;

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
 * {@link CoordinatorLeaderBootstrap} implementation that stands up the Kafka Schema Registry HTTP
 * listener on the elected coordinator leader. Requires the {@link CatalogService} to be running
 * (started by {@code FlussCatalogBootstrap} at a lower priority); fails fast if absent.
 */
@Internal
public final class SchemaRegistryBootstrap implements CoordinatorLeaderBootstrap {

    @Override
    public String name() {
        return "Kafka Schema Registry";
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
        if (!conf.getBoolean(ConfigOptions.KAFKA_ENABLED)
                || !conf.getBoolean(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED)) {
            return null;
        }
        CatalogService catalog =
                CatalogServices.current()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Kafka Schema Registry is enabled but the Fluss "
                                                        + "catalog service is not running. Ensure "
                                                        + "FlussCatalogBootstrap is on the "
                                                        + "classpath and not self-gated off."));

        String host = conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST);
        int port = conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT);
        String kafkaDatabase = conf.get(ConfigOptions.KAFKA_DATABASE);
        boolean rbacEnforced = conf.getBoolean(ConfigOptions.KAFKA_SCHEMA_REGISTRY_RBAC_ENFORCED);
        SchemaRegistryService service =
                new SchemaRegistryService(metadataManager, catalog, kafkaDatabase, rbacEnforced);
        SchemaRegistryHttpServer server = new SchemaRegistryHttpServer(host, port, service);
        server.start();
        return server;
    }
}
