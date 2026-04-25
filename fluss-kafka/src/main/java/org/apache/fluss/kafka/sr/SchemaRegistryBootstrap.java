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
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.sr.auth.HttpPrincipalExtractor;
import org.apache.fluss.kafka.sr.auth.JaasHttpPrincipalStore;
import org.apache.fluss.kafka.sr.typed.TypedTableEvolver;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CoordinatorLeaderBootstrap} implementation that stands up the Kafka Schema Registry HTTP
 * listener on the elected coordinator leader. Requires the {@link CatalogService} to be running
 * (started by {@code FlussCatalogBootstrap} at a lower priority); fails fast if absent.
 */
@Internal
public final class SchemaRegistryBootstrap implements CoordinatorLeaderBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryBootstrap.class);

    /** Fluss listener name we bootstrap the typed-table-reshape Admin client against. */
    private static final String FLUSS_LISTENER = "FLUSS";

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
        List<String> trustedProxyCidrs =
                conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_TRUSTED_PROXY_CIDRS);
        String basicAuthJaas = conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_BASIC_AUTH_JAAS_CONFIG);
        JaasHttpPrincipalStore basicAuthStore = JaasHttpPrincipalStore.fromJaasText(basicAuthJaas);
        HttpPrincipalExtractor principalExtractor =
                new HttpPrincipalExtractor(trustedProxyCidrs, basicAuthStore);
        boolean typedTablesEnabled = conf.getBoolean(ConfigOptions.KAFKA_TYPED_TABLES_ENABLED);
        // Lazy Admin used by the TypedTableEvolver's empty-topic guard. We open the Connection
        // on first reshape, not at bootstrap time, so a coordinator that never sees a typed
        // registration never opens a client back-channel. Mirrors FlussCatalogBootstrap's lazy
        // Connection pattern.
        AdminLifecycle adminLifecycle =
                new AdminLifecycle(conf, metadataCache, coordinatorBindEndpoints);
        TypedTableEvolver evolver =
                new TypedTableEvolver(
                        metadataManager,
                        catalog,
                        kafkaDatabase,
                        adminLifecycle::admin,
                        typedTablesEnabled);
        SchemaRegistryService service =
                new SchemaRegistryService(
                        metadataManager, catalog, kafkaDatabase, rbacEnforced, evolver);
        SchemaRegistryHttpServer server =
                new SchemaRegistryHttpServer(host, port, service, principalExtractor);
        server.start();
        return () -> {
            try {
                server.close();
            } finally {
                adminLifecycle.close();
            }
        };
    }

    private static List<String> buildBootstrap(
            ServerMetadataCache cache, List<Endpoint> coordinatorBindEndpoints) {
        List<String> out = new ArrayList<>();
        for (ServerNode node : cache.getAllAliveTabletServers(FLUSS_LISTENER).values()) {
            out.add(node.host() + ":" + node.port());
        }
        if (out.isEmpty()) {
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

    /**
     * Lazily opens (and closes) the Admin client used by {@link TypedTableEvolver}'s empty-topic
     * guard. Holds a single {@link Connection} across the bootstrap's lifetime; idempotent close.
     */
    private static final class AdminLifecycle implements AutoCloseable {

        private final Configuration baseConf;
        private final ServerMetadataCache metadataCache;
        private final List<Endpoint> coordinatorBindEndpoints;
        private final Object lock = new Object();
        private volatile Connection connection;
        private volatile Admin cachedAdmin;
        private volatile boolean closed;

        AdminLifecycle(
                Configuration baseConf,
                ServerMetadataCache metadataCache,
                List<Endpoint> coordinatorBindEndpoints) {
            this.baseConf = baseConf;
            this.metadataCache = metadataCache;
            this.coordinatorBindEndpoints = coordinatorBindEndpoints;
        }

        Admin admin() {
            if (closed) {
                return null;
            }
            Admin a = cachedAdmin;
            if (a != null) {
                return a;
            }
            synchronized (lock) {
                if (closed) {
                    return null;
                }
                if (cachedAdmin != null) {
                    return cachedAdmin;
                }
                List<String> bootstrap = buildBootstrap(metadataCache, coordinatorBindEndpoints);
                if (bootstrap.isEmpty()) {
                    LOG.warn(
                            "Typed-table evolver Admin: no FLUSS listener available, returning null");
                    return null;
                }
                Configuration clientConf = new Configuration(baseConf);
                clientConf.set(ConfigOptions.BOOTSTRAP_SERVERS, bootstrap);
                Connection c = ConnectionFactory.createConnection(clientConf);
                Admin built = c.getAdmin();
                this.connection = c;
                this.cachedAdmin = built;
                return built;
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                closed = true;
                Admin a = cachedAdmin;
                cachedAdmin = null;
                Connection c = connection;
                connection = null;
                if (a != null) {
                    try {
                        a.close();
                    } catch (Exception e) {
                        LOG.warn("Failed to close typed-table evolver Admin", e);
                    }
                }
                if (c != null) {
                    try {
                        c.close();
                    } catch (Exception e) {
                        LOG.warn("Failed to close typed-table evolver Connection", e);
                    }
                }
            }
        }
    }
}
