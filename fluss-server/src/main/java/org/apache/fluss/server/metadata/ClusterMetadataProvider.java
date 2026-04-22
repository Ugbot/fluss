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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;

import java.util.Optional;

/**
 * Stable query surface protocol bolt-ons consume to build wire-format metadata responses. Extends
 * {@link ServerMetadataCache} (cluster-level) with table / partition metadata lookups.
 *
 * <p>{@link TabletServerMetadataCache} is the production implementation. Bolt-ons (Kafka plugin,
 * Schema Registry, future catalog projections) should depend on this interface — not the concrete
 * cache class — so Fluss core is free to swap in a different implementation without forcing a
 * bolt-on rewrite.
 */
@PublicEvolving
public interface ClusterMetadataProvider extends ServerMetadataCache {

    /**
     * Resolve metadata for a non-partitioned table. Returns empty when the table is unknown to this
     * cache (e.g. not yet propagated).
     */
    Optional<TableMetadata> getTableMetadata(TablePath tablePath);

    /**
     * Resolve metadata for a specific partition of a partitioned table. Returns empty when the
     * partition is unknown to this cache.
     */
    Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath);
}
