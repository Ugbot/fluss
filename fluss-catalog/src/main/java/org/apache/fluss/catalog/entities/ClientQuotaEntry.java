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

package org.apache.fluss.catalog.entities;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * One row in {@code _catalog._client_quotas}. Kafka-compatible: {@code entityType} is one of
 * Kafka's conventional {@code user}, {@code client-id}, {@code ip} names; {@code entityName} is the
 * entity identifier (e.g. user name), or the empty string to mean "the default entity" for its
 * type. {@code quotaKey} is a Kafka quota-config name (e.g. {@code producer_byte_rate}).
 *
 * <p>Phase I.3 (Path A) storage-only: nothing else in Fluss consults these rows at request time —
 * Produce / Fetch / request-dispatch paths are untouched. The entry exists so Kafka tooling that
 * calls {@code DESCRIBE_CLIENT_QUOTAS} / {@code ALTER_CLIENT_QUOTAS} can read back what it wrote.
 */
@PublicEvolving
public final class ClientQuotaEntry {

    /** Kafka convention: "the default entity of this type". */
    public static final String DEFAULT_ENTITY_NAME = "";

    private final String entityType;
    private final String entityName;
    private final String quotaKey;
    private final double quotaValue;
    private final long updatedAtMillis;

    public ClientQuotaEntry(
            String entityType,
            String entityName,
            String quotaKey,
            double quotaValue,
            long updatedAtMillis) {
        this.entityType = entityType;
        this.entityName = entityName;
        this.quotaKey = quotaKey;
        this.quotaValue = quotaValue;
        this.updatedAtMillis = updatedAtMillis;
    }

    /** {@code user}, {@code client-id}, or {@code ip}. */
    public String entityType() {
        return entityType;
    }

    /** Entity identifier, or {@link #DEFAULT_ENTITY_NAME} for the default. */
    public String entityName() {
        return entityName;
    }

    /** Kafka quota-config key, e.g. {@code producer_byte_rate}. */
    public String quotaKey() {
        return quotaKey;
    }

    public double quotaValue() {
        return quotaValue;
    }

    public long updatedAtMillis() {
        return updatedAtMillis;
    }

    /** True when this entry targets the "default" entity (empty name). */
    public boolean isDefault() {
        return DEFAULT_ENTITY_NAME.equals(entityName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientQuotaEntry)) {
            return false;
        }
        ClientQuotaEntry that = (ClientQuotaEntry) o;
        return Double.compare(that.quotaValue, quotaValue) == 0
                && updatedAtMillis == that.updatedAtMillis
                && entityType.equals(that.entityType)
                && entityName.equals(that.entityName)
                && quotaKey.equals(that.quotaKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, entityName, quotaKey, quotaValue, updatedAtMillis);
    }

    @Override
    public String toString() {
        return "ClientQuotaEntry{"
                + "entityType='"
                + entityType
                + '\''
                + ", entityName='"
                + entityName
                + '\''
                + ", quotaKey='"
                + quotaKey
                + '\''
                + ", quotaValue="
                + quotaValue
                + ", updatedAtMillis="
                + updatedAtMillis
                + '}';
    }
}
