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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One row in {@code _catalog.__tables__}. Multi-format: a single catalog table can be backed by a
 * native Fluss table, an Iceberg table, a Kafka-passthrough table, etc. The {@code format} + {@code
 * backingRef} pair is the pointer into whatever storage owns the bytes.
 */
@PublicEvolving
public final class CatalogTableEntity {

    /**
     * Format string for Kafka topics whose backing data table holds raw {@code [record_key,
     * payload, event_time, headers]} rows (no Schema Registry registration). The default for every
     * Kafka topic at create time. See {@code KafkaTopicRoute#FORMAT_PASSTHROUGH}.
     */
    public static final String FORMAT_KAFKA_PASSTHROUGH = "KAFKA_PASSTHROUGH";

    /**
     * Format string for Kafka topics whose first Avro schema registration has reshaped the backing
     * data table into typed columns. Phase T.3.
     */
    public static final String FORMAT_KAFKA_TYPED_AVRO = "KAFKA_TYPED_AVRO";

    /**
     * Format string for Kafka topics whose first JSON Schema registration has reshaped the backing
     * data table into typed columns. Phase T.3.
     */
    public static final String FORMAT_KAFKA_TYPED_JSON = "KAFKA_TYPED_JSON";

    /**
     * Format string for Kafka topics whose first Protobuf schema registration has reshaped the
     * backing data table into typed columns. Phase T.3.
     */
    public static final String FORMAT_KAFKA_TYPED_PROTOBUF = "KAFKA_TYPED_PROTOBUF";

    private final String tableId;
    private final String namespaceId;
    private final String name;
    private final String format;
    private final String backingRef;
    private final @Nullable String currentSchemaId;
    private final @Nullable String createdBy;
    private final long createdAtMillis;

    public CatalogTableEntity(
            String tableId,
            String namespaceId,
            String name,
            String format,
            String backingRef,
            @Nullable String currentSchemaId,
            @Nullable String createdBy,
            long createdAtMillis) {
        this.tableId = tableId;
        this.namespaceId = namespaceId;
        this.name = name;
        this.format = format;
        this.backingRef = backingRef;
        this.currentSchemaId = currentSchemaId;
        this.createdBy = createdBy;
        this.createdAtMillis = createdAtMillis;
    }

    public String tableId() {
        return tableId;
    }

    public String namespaceId() {
        return namespaceId;
    }

    public String name() {
        return name;
    }

    public String format() {
        return format;
    }

    public String backingRef() {
        return backingRef;
    }

    @Nullable
    public String currentSchemaId() {
        return currentSchemaId;
    }

    @Nullable
    public String createdBy() {
        return createdBy;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CatalogTableEntity)) {
            return false;
        }
        return tableId.equals(((CatalogTableEntity) o).tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId);
    }
}
