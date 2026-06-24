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

package org.apache.fluss.server.coordinator.spi;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.List;

/**
 * Service-loader SPI for bolt-on services that need to run on the elected coordinator leader — the
 * Kafka Schema Registry HTTP listener today, the Fluss catalog service and the Iceberg REST
 * endpoint in planned phases.
 *
 * <p>One implementation per service; each is listed in {@code
 * META-INF/services/org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap} inside its
 * own module. {@link org.apache.fluss.server.coordinator.CoordinatorServer} discovers them via
 * {@link java.util.ServiceLoader} on leadership acquisition, calls {@link #start} on each, and
 * closes the returned {@link AutoCloseable}s on leadership loss or shutdown.
 *
 * <p>Implementations are expected to:
 *
 * <ul>
 *   <li>Self-gate — return {@code null} from {@link #start} when their feature flag is off.
 *   <li>Never throw out of {@link #start} except for configuration-error reasons. Transient
 *       failures (bind races, dependency warmup) should be logged and surfaced via the returned
 *       closeable's {@code close()} without blocking leadership.
 *   <li>Be idempotent under repeated leader election cycles — a {@code start()} after a previous
 *       {@code close()} must produce a fresh instance in the same state as the first call.
 * </ul>
 */
@PublicEvolving
public interface CoordinatorLeaderBootstrap {

    /** Short human-readable name for logs, e.g. {@code "Kafka Schema Registry"}. */
    String name();

    /**
     * Startup ordering hint — lower values start first, higher values last (default 100).
     * Catalog-style services that register themselves into the process should return a low number
     * so projections that consume them (SR, Iceberg REST) see them already up. Shutdown happens in
     * reverse.
     */
    default int priority() {
        return 100;
    }

    /**
     * Start the bolt-on service.
     *
     * @param metadataCache coordinator-side {@link ServerMetadataCache} so bolt-ons that need to
     *     open a Fluss client {@code Connection} (catalog service, future Iceberg REST endpoint)
     *     can discover alive tablet-server FLUSS-listener endpoints.
     * @param coordinatorBindEndpoints the RPC endpoints this coordinator is bound on. Useful as a
     *     fallback bootstrap target when the cache has not yet picked up tablet-server
     *     registrations (first leader election after cluster start).
     * @return a closeable whose {@code close()} tears down every resource the service acquired, or
     *     {@code null} if the service is disabled by configuration.
     */
    AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zk,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache,
            List<Endpoint> coordinatorBindEndpoints)
            throws Exception;
}
