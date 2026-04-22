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

package org.apache.fluss.kafka.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.metadata.ServerMetadataCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Opens a single Fluss {@link Connection} per {@code KafkaRequestHandler} instance, bootstrapping
 * from this tablet server's own FLUSS listener. Close once at handler shutdown.
 *
 * <p>See {@code dev-docs/design/notes-fluss-client-from-tabletserver.md} for why this is safe: the
 * client is used from the protocol-handler thread and issues standard RPCs to the local FLUSS
 * listener; no thread-local session leakage.
 */
@Internal
public final class OffsetStoreConnections {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetStoreConnections.class);

    /** Fluss listener name we bootstrap against. */
    public static final String FLUSS_LISTENER = "FLUSS";

    private OffsetStoreConnections() {}

    /**
     * Build a {@link Connection} whose bootstrap list is this tablet server's own advertised FLUSS
     * endpoint (plus any peer FLUSS endpoints discovered via the metadata cache — they act as a
     * safety net in case the self endpoint is not yet propagated). The caller owns the returned
     * {@code Connection} and must close it.
     *
     * @return an open {@link Connection}. Never {@code null}.
     * @throws IllegalStateException if no FLUSS endpoint can be resolved.
     */
    public static Connection open(
            ServerMetadataCache metadataCache, int ownServerId, Configuration serverConf) {
        List<String> bootstrap = buildBootstrap(metadataCache, ownServerId);
        if (bootstrap.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot open Fluss client Connection for FlussPkOffsetStore: "
                            + "no FLUSS-listener tablet server is registered in the metadata cache. "
                            + "The Kafka plugin requires at least one FLUSS listener on this or a peer TS.");
        }
        Configuration clientConf = new Configuration(serverConf);
        clientConf.set(ConfigOptions.BOOTSTRAP_SERVERS, bootstrap);
        LOG.info(
                "Opening Fluss client Connection for kafka.__consumer_offsets__ (bootstrap={})",
                bootstrap);
        return ConnectionFactory.createConnection(clientConf);
    }

    private static List<String> buildBootstrap(ServerMetadataCache cache, int ownServerId) {
        List<String> out = new ArrayList<>(2);
        // Prefer our own registration so we always end up connecting locally when possible.
        cache.getTabletServer(ownServerId, FLUSS_LISTENER)
                .ifPresent(self -> out.add(self.host() + ":" + self.port()));
        Map<Integer, ServerNode> all = cache.getAllAliveTabletServers(FLUSS_LISTENER);
        for (Map.Entry<Integer, ServerNode> entry : all.entrySet()) {
            if (entry.getKey() == ownServerId) {
                continue;
            }
            ServerNode node = entry.getValue();
            out.add(node.host() + ":" + node.port());
        }
        return out;
    }
}
