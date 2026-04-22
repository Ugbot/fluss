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
import org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.ZooKeeperClient;

/**
 * {@link CoordinatorLeaderBootstrap} implementation that stands up the Kafka Schema Registry HTTP
 * listener on the elected coordinator leader. Discovered via {@link java.util.ServiceLoader}
 * through {@code META-INF/services/...CoordinatorLeaderBootstrap} in this module's JAR.
 *
 * <p>Returns {@code null} from {@link #start} when either {@code kafka.enabled=false} or {@code
 * kafka.schema-registry.enabled=false}.
 */
@Internal
public final class SchemaRegistryBootstrap implements CoordinatorLeaderBootstrap {

    @Override
    public String name() {
        return "Kafka Schema Registry";
    }

    @Override
    public AutoCloseable start(
            Configuration conf,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            ServerMetadataCache metadataCache)
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
