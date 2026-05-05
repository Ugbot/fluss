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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.ClientQuotaEntry;
import org.apache.fluss.catalog.entities.ClientQuotaFilter;

import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntryData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.OpData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData.ComponentData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData.EntityData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData.ValueData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates Kafka's {@code DESCRIBE_CLIENT_QUOTAS (48)} / {@code ALTER_CLIENT_QUOTAS (49)} against
 * the Fluss catalog's client-quotas store.
 *
 * <p>Phase I.3 Path A: <b>accept-and-store only</b>. Produce / Fetch / request-dispatch paths don't
 * consult these rows; the transcoder exists so kafka-clients tooling ({@code kafka-configs.sh},
 * Kafka-compatible admin tooling, etc.) can read back what it wrote. No throttle is installed.
 * Supported entity types are Kafka's conventional {@code user}, {@code client-id}, {@code ip};
 * unknown entity types are accepted and stored (matching Kafka's own lenient storage semantics).
 *
 * <p>The Kafka wire protocol groups quota entries by <em>entity</em> (a list of {@code (entityType,
 * entityName)} pairs, which together identify a multi-dimensional bucket such as "user=alice,
 * client-id=ingest"). Our storage is single-dimension per row. When the client submits a
 * multi-dimensional entry we flatten it to one row per dimension — compatible with kafka-clients'
 * common case ("user=alice alone"), lenient for the less-common multi-dimensional case (Kafka's
 * {@code (user, client-id)} joint quotas are not enforced anyway in Path A).
 */
@Internal
public final class KafkaClientQuotasTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientQuotasTranscoder.class);

    /** Kafka {@code ClientQuotaFilterComponent#matchType} values. */
    private static final byte MATCH_EXACT = 0;

    private static final byte MATCH_DEFAULT = 1;
    private static final byte MATCH_SPECIFIED = 2;

    private final CatalogService catalog;

    public KafkaClientQuotasTranscoder(CatalogService catalog) {
        this.catalog = catalog;
    }

    public DescribeClientQuotasResponseData describeClientQuotas(
            DescribeClientQuotasRequestData request) {
        DescribeClientQuotasResponseData response = new DescribeClientQuotasResponseData();
        response.setThrottleTimeMs(0);
        ClientQuotaFilter filter;
        try {
            filter = toFilter(request);
        } catch (IllegalArgumentException iae) {
            response.setErrorCode(Errors.INVALID_REQUEST.code());
            response.setErrorMessage(iae.getMessage());
            response.setEntries(new ArrayList<>());
            return response;
        }

        List<ClientQuotaEntry> rows;
        try {
            rows = catalog.listClientQuotas(filter);
        } catch (Throwable t) {
            LOG.error("Catalog listClientQuotas failed", t);
            response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
            response.setErrorMessage(t.getClass().getSimpleName() + ": " + t.getMessage());
            response.setEntries(new ArrayList<>());
            return response;
        }

        // Group single-dimension rows by (entityType, entityName) so one EntryData surfaces all
        // quota keys for the same entity — matching Kafka's wire shape.
        Map<EntityKey, List<ClientQuotaEntry>> grouped = new LinkedHashMap<>();
        for (ClientQuotaEntry e : rows) {
            grouped.computeIfAbsent(
                            new EntityKey(e.entityType(), e.entityName()), k -> new ArrayList<>())
                    .add(e);
        }

        List<DescribeClientQuotasResponseData.EntryData> entries = new ArrayList<>();
        for (Map.Entry<EntityKey, List<ClientQuotaEntry>> group : grouped.entrySet()) {
            DescribeClientQuotasResponseData.EntryData entry =
                    new DescribeClientQuotasResponseData.EntryData();
            EntityData entity = new EntityData();
            entity.setEntityType(group.getKey().entityType);
            // Kafka wire: null name == default entity.
            entity.setEntityName(
                    ClientQuotaEntry.DEFAULT_ENTITY_NAME.equals(group.getKey().entityName)
                            ? null
                            : group.getKey().entityName);
            entry.setEntity(java.util.Collections.singletonList(entity));
            List<ValueData> values = new ArrayList<>();
            // Deterministic ordering for test assertions.
            group.getValue().sort(Comparator.comparing(ClientQuotaEntry::quotaKey));
            for (ClientQuotaEntry e : group.getValue()) {
                values.add(new ValueData().setKey(e.quotaKey()).setValue(e.quotaValue()));
            }
            entry.setValues(values);
            entries.add(entry);
        }
        response.setErrorCode(Errors.NONE.code());
        response.setEntries(entries);
        return response;
    }

    public AlterClientQuotasResponseData alterClientQuotas(AlterClientQuotasRequestData request) {
        AlterClientQuotasResponseData response = new AlterClientQuotasResponseData();
        response.setThrottleTimeMs(0);
        List<AlterClientQuotasResponseData.EntryData> out = new ArrayList<>();
        boolean validateOnly = request.validateOnly();

        for (EntryData entry : request.entries()) {
            AlterClientQuotasResponseData.EntryData respEntry =
                    new AlterClientQuotasResponseData.EntryData();
            List<AlterClientQuotasResponseData.EntityData> respEntity = new ArrayList<>();
            for (AlterClientQuotasRequestData.EntityData ent : entry.entity()) {
                respEntity.add(
                        new AlterClientQuotasResponseData.EntityData()
                                .setEntityType(ent.entityType())
                                .setEntityName(ent.entityName()));
            }
            respEntry.setEntity(respEntity);

            if (entry.entity() == null || entry.entity().isEmpty()) {
                respEntry.setErrorCode(Errors.INVALID_REQUEST.code());
                respEntry.setErrorMessage("Empty entity in AlterClientQuotas entry.");
                out.add(respEntry);
                continue;
            }

            try {
                if (!validateOnly) {
                    applyOps(entry);
                }
                respEntry.setErrorCode(Errors.NONE.code());
            } catch (IllegalArgumentException iae) {
                respEntry.setErrorCode(Errors.INVALID_REQUEST.code());
                respEntry.setErrorMessage(iae.getMessage());
            } catch (Throwable t) {
                LOG.error("AlterClientQuotas apply failed for {}", entry.entity(), t);
                respEntry.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                respEntry.setErrorMessage(t.getClass().getSimpleName() + ": " + t.getMessage());
            }
            out.add(respEntry);
        }
        response.setEntries(out);
        return response;
    }

    /**
     * Apply every op in {@code entry} to every {@code (entityType, entityName)} pair in its entity
     * list. Multi-dimensional entries are flattened: for each entity-type component we
     * upsert/delete a row per op. This is the Path-A approximation of Kafka's joint-quota
     * semantics.
     */
    private void applyOps(EntryData entry) throws Exception {
        for (AlterClientQuotasRequestData.EntityData ent : entry.entity()) {
            String entityType = ent.entityType();
            String entityName = ent.entityName();
            if (entityName == null) {
                // Kafka wire: null name == default entity.
                entityName = ClientQuotaEntry.DEFAULT_ENTITY_NAME;
            }
            for (OpData op : entry.ops()) {
                if (op.remove()) {
                    catalog.deleteClientQuota(entityType, entityName, op.key());
                } else {
                    catalog.upsertClientQuota(entityType, entityName, op.key(), op.value());
                }
            }
        }
    }

    private static ClientQuotaFilter toFilter(DescribeClientQuotasRequestData request) {
        List<ClientQuotaFilter.Component> components = new ArrayList<>();
        if (request.components() != null) {
            for (ComponentData c : request.components()) {
                byte m = c.matchType();
                if (m == MATCH_EXACT) {
                    String match = c.match();
                    if (match == null) {
                        throw new IllegalArgumentException(
                                "Exact match for entity type "
                                        + c.entityType()
                                        + " requires a non-null match value.");
                    }
                    components.add(ClientQuotaFilter.Component.ofEntity(c.entityType(), match));
                } else if (m == MATCH_DEFAULT) {
                    components.add(ClientQuotaFilter.Component.ofDefault(c.entityType()));
                } else if (m == MATCH_SPECIFIED) {
                    components.add(ClientQuotaFilter.Component.ofEntityType(c.entityType()));
                } else {
                    throw new IllegalArgumentException("Unknown matchType=" + m);
                }
            }
        }
        return new ClientQuotaFilter(components, request.strict());
    }

    /** Value-equality key for grouping single-dimension rows back into wire entries. */
    private static final class EntityKey {
        final String entityType;
        final String entityName;

        EntityKey(String entityType, String entityName) {
            this.entityType = entityType;
            this.entityName = entityName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EntityKey)) {
                return false;
            }
            EntityKey that = (EntityKey) o;
            return entityType.equals(that.entityType) && entityName.equals(that.entityName);
        }

        @Override
        public int hashCode() {
            int h = entityType.hashCode();
            h = h * 31 + entityName.hashCode();
            return h;
        }
    }
}
