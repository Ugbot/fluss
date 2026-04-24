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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.auth.KafkaListenerAuthConfig;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.metrics.ServerMetricUtils;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProtocolPlugin.class);

    private Configuration conf;

    /**
     * Shared metric group across every listener + request handler this plugin owns. Resolved lazily
     * on the first {@link #createRequestHandler} call (when we have a {@link TabletService}) and
     * read by the {@link KafkaChannelInitializer} the next time a channel opens.
     */
    private volatile KafkaMetricGroup sharedMetrics;

    /**
     * VisibleForTesting registry of every {@link KafkaMetricGroup} active in this JVM, keyed by
     * tablet-server id. In-process integration tests use this to assert metric values without
     * reaching through {@link ReplicaManager}'s private fields.
     */
    private static final ConcurrentMap<Integer, KafkaMetricGroup> METRIC_GROUPS_BY_SERVER_ID =
            new ConcurrentHashMap<>();

    /**
     * Return the Kafka bolt-on's metric group for the given tablet-server id, or {@code null} when
     * the plugin is not active for that server. In-process tests only.
     */
    @org.apache.fluss.annotation.VisibleForTesting
    public static KafkaMetricGroup metricGroupForServer(int serverId) {
        return METRIC_GROUPS_BY_SERVER_ID.get(serverId);
    }

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        this.conf = conf;
        validateLocalListenerBinding(conf);
    }

    @Override
    public List<String> listenerNames() {
        return conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        KafkaListenerAuthConfig authConfig = KafkaListenerAuthConfig.resolve(conf, listenerName);
        LOG.info(
                "Kafka listener '{}' auth posture: {} (enabled mechanisms={})",
                listenerName,
                authConfig.protocol(),
                authConfig.enabledMechanisms());
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.KAFKA_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST),
                authConfig,
                () -> sharedMetrics);
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        if (!(service instanceof TabletServerGateway)) {
            // Plugin is loaded on a non-tablet server (e.g. CoordinatorServer when kafka.enabled
            // is true but no KAFKA listener is bound). Return an idle handler; it will never
            // receive requests because no Kafka endpoint is bound here.
            LOG.info(
                    "Kafka protocol plugin loaded on {} without a KAFKA listener; "
                            + "request handler will be idle.",
                    service.getClass().getSimpleName());
            KafkaServerContext context = buildContext(service);
            return new KafkaRequestHandler(null, context);
        }
        TabletServerGateway gateway = (TabletServerGateway) service;
        KafkaServerContext context = buildContext(service);
        return new KafkaRequestHandler(gateway, context);
    }

    private KafkaServerContext buildContext(RpcGatewayService service) {
        String clusterId = ServerMetricUtils.validateAndGetClusterId(conf);
        String kafkaDatabase = conf.get(ConfigOptions.KAFKA_DATABASE);
        if (service instanceof TabletService) {
            TabletService ts = (TabletService) service;
            KafkaMetricGroup metrics = buildMetricGroup(ts);
            this.sharedMetrics = metrics;
            if (metrics != null) {
                METRIC_GROUPS_BY_SERVER_ID.put(ts.getServerId(), metrics);
            }
            LOG.info(
                    "Kafka bolt-on attached (serverId={}, authorizer={}, metrics={})",
                    ts.getServerId(),
                    extractAuthorizer(ts) != null ? "enabled" : "disabled",
                    metrics != null ? "enabled" : "disabled");
            return new KafkaServerContext(
                    ts.getMetadataCache(),
                    ts.getMetadataManager(),
                    ts.getCoordinatorGateway(),
                    ts.getReplicaManager(),
                    ts.getZooKeeperClient(),
                    extractAuthorizer(ts),
                    metrics,
                    clusterId,
                    kafkaDatabase,
                    ts.getServerId(),
                    conf);
        }
        // Test-only gateway services (e.g. TestingTabletGatewayService) land here.
        LOG.warn(
                "Kafka protocol gateway {} does not expose TabletServer state; "
                        + "Metadata and DescribeCluster requests will fail until a full TabletService is used.",
                service.getClass().getSimpleName());
        return new KafkaServerContext(null, null, null, null, null, clusterId, kafkaDatabase);
    }

    /**
     * Builds a {@link KafkaMetricGroup} parented on the {@link
     * org.apache.fluss.server.metrics.group.TabletServerMetricGroup} owned by the server's {@link
     * ReplicaManager}. We reach the metric group through the replica manager rather than modifying
     * the TabletService API surface; the reflection stays contained here and mirrors the {@link
     * #extractAuthorizer} pattern. Returns {@code null} when the metric group isn't wired (test
     * harnesses, metric-less test servers) — handlers no-op their metric calls in that case.
     */
    @javax.annotation.Nullable
    private KafkaMetricGroup buildMetricGroup(TabletService service) {
        ReplicaManager rm = service.getReplicaManager();
        if (rm == null) {
            return null;
        }
        AbstractMetricGroup parent = extractServerMetricGroup(rm);
        if (parent == null) {
            LOG.warn(
                    "Kafka bolt-on could not locate the TabletServerMetricGroup via reflection;"
                            + " per-request metrics will be disabled.");
            return null;
        }
        int topicCap = conf.get(ConfigOptions.KAFKA_METRICS_PER_TOPIC_MAX_CARDINALITY);
        int groupCap = conf.get(ConfigOptions.KAFKA_METRICS_PER_GROUP_MAX_CARDINALITY);
        boolean topicEnabled = conf.getBoolean(ConfigOptions.KAFKA_METRICS_PER_TOPIC_ENABLED);
        boolean groupEnabled = conf.getBoolean(ConfigOptions.KAFKA_METRICS_PER_GROUP_ENABLED);
        if (!topicEnabled) {
            topicCap = 0;
        }
        if (!groupEnabled) {
            groupCap = 0;
        }
        return new KafkaMetricGroup(parent.getMetricRegistry(), parent, topicCap, groupCap);
    }

    /**
     * Pulls the server-side metric group off {@link ReplicaManager}. The field is private and has
     * no public accessor, so we reach through reflection rather than expanding the fluss-server API
     * surface (same rationale as {@link #extractAuthorizer}).
     */
    @javax.annotation.Nullable
    private static AbstractMetricGroup extractServerMetricGroup(ReplicaManager rm) {
        Class<?> c = rm.getClass();
        while (c != null && c != Object.class) {
            try {
                Field f = c.getDeclaredField("serverMetricGroup");
                f.setAccessible(true);
                Object value = f.get(rm);
                return value instanceof AbstractMetricGroup ? (AbstractMetricGroup) value : null;
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Pulls the {@link Authorizer} off the service's {@code RpcServiceBase} parent. The field is
     * package-private to {@code fluss-server} and has no public accessor, so we reach through
     * reflection rather than modifying a server-module API surface. Returns {@code null} if the
     * server was started without an authorizer.
     */
    private static Authorizer extractAuthorizer(TabletService service) {
        Class<?> c = service.getClass();
        while (c != null && c != Object.class) {
            try {
                Field f = c.getDeclaredField("authorizer");
                f.setAccessible(true);
                Object value = f.get(service);
                return value instanceof Authorizer ? (Authorizer) value : null;
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            } catch (IllegalAccessException e) {
                LOG.warn(
                        "Failed to read Authorizer from {}; ACL enforcement will be disabled"
                                + " for the Kafka listener.",
                        service.getClass().getSimpleName(),
                        e);
                return null;
            }
        }
        return null;
    }

    private void validateLocalListenerBinding(Configuration conf) {
        List<String> kafkaListeners = conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
        checkArgument(
                kafkaListeners != null && !kafkaListeners.isEmpty(),
                "%s must be set when %s=true",
                ConfigOptions.KAFKA_LISTENER_NAMES.key(),
                ConfigOptions.KAFKA_ENABLED.key());

        // BIND_LISTENERS is the authoritative source for a production server's bound listeners.
        // If the operator wired endpoints programmatically (tests), defer validation to runtime.
        if (!conf.getOptional(ConfigOptions.BIND_LISTENERS).isPresent()) {
            return;
        }

        ServerType serverType =
                conf.getOptional(ConfigOptions.TABLET_SERVER_ID).isPresent()
                        ? ServerType.TABLET_SERVER
                        : ServerType.COORDINATOR;
        List<Endpoint> bindEndpoints = Endpoint.loadBindEndpoints(conf, serverType);
        Set<String> boundListenerNames = new HashSet<>();
        for (Endpoint endpoint : bindEndpoints) {
            boundListenerNames.add(endpoint.getListenerName());
        }

        // A server where none of the declared Kafka listeners are bound is not a Kafka broker
        // (typical for coordinator-only deployments). Log and move on; createRequestHandler
        // returns an idle handler in that case.
        boolean anyBound = false;
        for (String kafkaListener : kafkaListeners) {
            if (boundListenerNames.contains(kafkaListener)) {
                anyBound = true;
                break;
            }
        }
        if (!anyBound) {
            LOG.info(
                    "No Kafka listener bound on this server ({} present in {}). "
                            + "Kafka protocol will be idle here.",
                    bindEndpoints,
                    ConfigOptions.BIND_LISTENERS.key());
            return;
        }

        for (String kafkaListener : kafkaListeners) {
            checkArgument(
                    boundListenerNames.contains(kafkaListener),
                    "Kafka listener '%s' declared in %s is not present in %s=%s. "
                            + "Add it to bind.listeners or remove it from kafka.listener.names.",
                    kafkaListener,
                    ConfigOptions.KAFKA_LISTENER_NAMES.key(),
                    ConfigOptions.BIND_LISTENERS.key(),
                    bindEndpoints);
        }
    }
}
