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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.ZooKeeperClient;

/**
 * Reflection target called by {@code CoordinatorServer} on leadership acquisition to avoid a {@code
 * fluss-server → fluss-kafka} Maven dependency (the cycle exists because fluss-kafka already
 * depends on fluss-server).
 *
 * <p>Contract: static {@link #start(Configuration, ZooKeeperClient, MetadataManager)} returns an
 * {@link AutoCloseable}; callers close it on leadership loss / shutdown. Returns {@code null} if
 * the SR is not enabled in config.
 */
@Internal
public final class SchemaRegistryBootstrap {

    private SchemaRegistryBootstrap() {}

    /**
     * Called reflectively from CoordinatorServer. If the SR is disabled in config, returns {@code
     * null} so the caller can skip lifecycle tracking.
     */
    public static AutoCloseable start(
            Configuration conf, ZooKeeperClient zkClient, MetadataManager metadataManager)
            throws Exception {
        if (!conf.getBoolean(ConfigOptions.KAFKA_ENABLED)
                || !conf.getBoolean(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED)) {
            return null;
        }
        String host = conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST);
        int port = conf.get(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT);
        String kafkaDatabase = conf.get(ConfigOptions.KAFKA_DATABASE);
        ConfluentIdAllocator allocator = new ConfluentIdAllocator(zkClient);
        SchemaRegistryService service =
                new SchemaRegistryService(metadataManager, allocator, kafkaDatabase);
        SchemaRegistryHttpServer server = new SchemaRegistryHttpServer(host, port, service);
        server.start();
        return server;
    }
}
