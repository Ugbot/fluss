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
 * One row in {@code _catalog.__schemas__}. Append-only per {@code tableId}; {@code version} is
 * monotone. Every projection (Kafka SR, Iceberg REST, …) reads from here.
 */
@PublicEvolving
public final class SchemaVersionEntity {

    private final String schemaId;
    private final String tableId;
    private final int version;
    private final String format;
    private final String schemaText;
    private final int srSchemaId;
    private final @Nullable String registeredBy;
    private final long registeredAtMillis;

    public SchemaVersionEntity(
            String schemaId,
            String tableId,
            int version,
            String format,
            String schemaText,
            int srSchemaId,
            @Nullable String registeredBy,
            long registeredAtMillis) {
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.version = version;
        this.format = format;
        this.schemaText = schemaText;
        this.srSchemaId = srSchemaId;
        this.registeredBy = registeredBy;
        this.registeredAtMillis = registeredAtMillis;
    }

    public String schemaId() {
        return schemaId;
    }

    public String tableId() {
        return tableId;
    }

    public int version() {
        return version;
    }

    public String format() {
        return format;
    }

    public String schemaText() {
        return schemaText;
    }

    public int srSchemaId() {
        return srSchemaId;
    }

    @Nullable
    public String registeredBy() {
        return registeredBy;
    }

    public long registeredAtMillis() {
        return registeredAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaVersionEntity)) {
            return false;
        }
        return schemaId.equals(((SchemaVersionEntity) o).schemaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId);
    }
}
