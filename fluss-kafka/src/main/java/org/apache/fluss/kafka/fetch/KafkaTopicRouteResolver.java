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

package org.apache.fluss.kafka.fetch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.catalog.entities.CatalogTableEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Looks up the catalog {@code __tables__.format} for a Kafka topic and translates it into a {@link
 * KafkaTopicRoute} (design 0014 §3). The resolver is the single seam between the Kafka hot path and
 * the catalog row that gates the typed branch — keeping it interface-shaped lets the IT pre-stage a
 * typed format without going through T.3's ALTER flow.
 *
 * <p>The default implementation returned by {@link #fromCatalogServices(boolean, String)} reads
 * {@link CatalogServices#current()} when available; on tablet servers that are not the coord leader
 * the resolver returns {@link KafkaTopicRoute#PASSTHROUGH} (which is also what the flag defaults
 * to). T.2's invariant: when the typed-tables flag is off, the resolver is never consulted; when
 * the flag is on, a missing catalog falls back to passthrough so a stand-alone tablet server is
 * never blocked.
 */
@Internal
public interface KafkaTopicRouteResolver {

    Logger LOG = LoggerFactory.getLogger(KafkaTopicRouteResolver.class);

    /**
     * Resolve a topic's route. Implementations MUST be safe to call from multiple Produce/Fetch
     * threads concurrently. They MUST NOT throw — failures must be downgraded to {@link
     * KafkaTopicRoute#PASSTHROUGH} with an internal log entry.
     */
    KafkaTopicRoute resolve(String topic);

    /** Always passthrough — used when the feature flag is off. */
    static KafkaTopicRouteResolver alwaysPassthrough() {
        return topic -> KafkaTopicRoute.PASSTHROUGH;
    }

    /**
     * Read-through resolver backed by {@link CatalogServices#current()}. When the catalog isn't
     * registered in this JVM (e.g. tablet server not co-located with the coord leader) this
     * resolver returns passthrough on every call — operators must run T.3's ALTER through the coord
     * leader, then fetch will see the updated row through the catalog distribution.
     *
     * @param typedTablesEnabled the cached feature flag; when {@code false}, returns {@link
     *     #alwaysPassthrough()}
     * @param kafkaDatabase the Kafka topics database name (e.g. {@code "kafka"})
     */
    static KafkaTopicRouteResolver fromCatalogServices(
            boolean typedTablesEnabled, String kafkaDatabase) {
        if (!typedTablesEnabled) {
            return alwaysPassthrough();
        }
        return new CatalogServicesBackedResolver(kafkaDatabase);
    }

    /** Default impl reading {@link CatalogServices#current()} on every call. */
    final class CatalogServicesBackedResolver implements KafkaTopicRouteResolver {

        private final String kafkaDatabase;

        CatalogServicesBackedResolver(String kafkaDatabase) {
            this.kafkaDatabase = kafkaDatabase;
        }

        @Override
        public KafkaTopicRoute resolve(String topic) {
            Optional<CatalogService> catalog = CatalogServices.current();
            if (!catalog.isPresent()) {
                return KafkaTopicRoute.PASSTHROUGH;
            }
            try {
                Optional<CatalogTableEntity> entity = catalog.get().getTable(kafkaDatabase, topic);
                if (!entity.isPresent()) {
                    return KafkaTopicRoute.PASSTHROUGH;
                }
                return KafkaTopicRoute.fromCatalogFormat(entity.get().format());
            } catch (Exception e) {
                LOG.warn(
                        "Catalog format lookup failed for topic '{}'; falling back to passthrough",
                        topic,
                        e);
                return KafkaTopicRoute.PASSTHROUGH;
            }
        }
    }
}
