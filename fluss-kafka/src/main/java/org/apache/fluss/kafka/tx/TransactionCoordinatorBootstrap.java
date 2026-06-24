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

package org.apache.fluss.kafka.tx;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * {@link CoordinatorLeaderBootstrap} that stands up the Kafka {@link TransactionCoordinator} on the
 * elected coordinator leader (design 0016 §3). Requires {@link CatalogService} to be running
 * (registered by {@code FlussCatalogBootstrap} at lower priority); fails fast if absent.
 *
 * <p>Self-gate: bootstrap is a no-op when {@link ConfigOptions#KAFKA_ENABLED} is off — Phase J
 * coordinators have no purpose outside the Kafka bolt-on.
 */
@Internal
public final class TransactionCoordinatorBootstrap implements CoordinatorLeaderBootstrap {

    private static final Logger LOG =
            LoggerFactory.getLogger(TransactionCoordinatorBootstrap.class);

    @Override
    public String name() {
        return "Kafka Transaction Coordinator";
    }

    @Override
    public int priority() {
        // Run after FlussCatalogBootstrap (priority 10) so the catalog is already registered, and
        // alongside the Schema Registry (priority 100) — neither depends on the other.
        return 110;
    }

    @Override
    public AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache,
            List<Endpoint> coordinatorBindEndpoints)
            throws Exception {
        if (!conf.getBoolean(ConfigOptions.KAFKA_ENABLED)) {
            return null;
        }
        CatalogService catalog =
                CatalogServices.current()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Kafka transaction coordinator requires the Fluss "
                                                        + "catalog service to be running. Ensure "
                                                        + "FlussCatalogBootstrap is on the classpath "
                                                        + "and not self-gated off."));
        TransactionCoordinator coord = TransactionCoordinator.startFromCatalog(catalog);
        TransactionCoordinators.set(coord);
        LOG.info(
                "Kafka transaction coordinator registered (active transactional ids: {}).",
                coord.activeTransactionalIdCount());
        return () -> {
            TransactionCoordinators.clear();
            LOG.info("Kafka transaction coordinator unregistered (leadership loss / shutdown).");
        };
    }
}
