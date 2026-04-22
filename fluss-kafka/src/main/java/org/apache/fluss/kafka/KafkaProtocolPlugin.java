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
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.server.metrics.ServerMetricUtils;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProtocolPlugin.class);

    private Configuration conf;

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
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.KAFKA_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST));
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
            return new KafkaServerContext(
                    ts.getMetadataCache(),
                    ts.getMetadataManager(),
                    ts.getCoordinatorGateway(),
                    ts.getReplicaManager(),
                    ts.getZooKeeperClient(),
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
