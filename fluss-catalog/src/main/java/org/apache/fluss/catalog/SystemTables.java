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

package org.apache.fluss.catalog;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

/**
 * Schema + {@link TableDescriptor} factories for every PK table in the reserved {@code _catalog}
 * database. One place to look when onboarding a new projection or extending the catalog schema.
 *
 * <p>All entity tables share a 16-bucket distribution — cheap by default, can be tuned later. RBAC
 * tables ({@link #PRINCIPALS}, {@link #GRANTS}) are created in Phase C.1 with no enforcement path;
 * Phase C.2 wires them in.
 */
@Internal
public final class SystemTables {

    /** Reserved database name holding every catalog system table. */
    public static final String DATABASE = "_catalog";

    public static final int DEFAULT_BUCKETS = 16;

    public static final Table NAMESPACES =
            new Table(
                    "_namespaces",
                    Schema.newBuilder()
                            .column("namespace_id", DataTypes.STRING().copy(false))
                            .column("parent_id", DataTypes.STRING())
                            .column("name", DataTypes.STRING().copy(false))
                            .column("description", DataTypes.STRING())
                            .column("created_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("namespace_id")
                            .build());

    public static final Table TABLES =
            new Table(
                    "_tables",
                    Schema.newBuilder()
                            .column("table_id", DataTypes.STRING().copy(false))
                            .column("namespace_id", DataTypes.STRING().copy(false))
                            .column("name", DataTypes.STRING().copy(false))
                            .column("format", DataTypes.STRING().copy(false))
                            .column("backing_ref", DataTypes.STRING().copy(false))
                            .column("current_schema_id", DataTypes.STRING())
                            .column("created_by", DataTypes.STRING())
                            .column("created_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("table_id")
                            .build());

    public static final Table SCHEMAS =
            new Table(
                    "_schemas",
                    Schema.newBuilder()
                            .column("schema_id", DataTypes.STRING().copy(false))
                            .column("table_id", DataTypes.STRING().copy(false))
                            .column("version", DataTypes.INT().copy(false))
                            .column("format", DataTypes.STRING().copy(false))
                            .column("schema_text", DataTypes.STRING().copy(false))
                            .column("confluent_id", DataTypes.INT().copy(false))
                            .column("registered_by", DataTypes.STRING())
                            .column("registered_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("schema_id")
                            .build());

    public static final Table PRINCIPALS =
            new Table(
                    "_principals",
                    Schema.newBuilder()
                            .column("principal_id", DataTypes.STRING().copy(false))
                            .column("name", DataTypes.STRING().copy(false))
                            .column("type", DataTypes.STRING().copy(false))
                            .column("created_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("principal_id")
                            .build());

    public static final Table GRANTS =
            new Table(
                    "_grants",
                    Schema.newBuilder()
                            .column("grant_id", DataTypes.STRING().copy(false))
                            .column("principal_id", DataTypes.STRING().copy(false))
                            .column("entity_kind", DataTypes.STRING().copy(false))
                            .column("entity_id", DataTypes.STRING().copy(false))
                            .column("privilege", DataTypes.STRING().copy(false))
                            .column("granted_by", DataTypes.STRING())
                            .column("granted_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("grant_id")
                            .build());

    public static final Table KAFKA_BINDINGS =
            new Table(
                    "_kafka_bindings",
                    Schema.newBuilder()
                            .column("subject", DataTypes.STRING().copy(false))
                            .column("table_id", DataTypes.STRING().copy(false))
                            .column("key_or_value", DataTypes.STRING().copy(false))
                            .column("naming_strategy", DataTypes.STRING().copy(false))
                            .primaryKey("subject")
                            .build());

    public static final Table ID_RESERVATIONS =
            new Table(
                    "_id_reservations",
                    Schema.newBuilder()
                            .column("confluent_id", DataTypes.INT().copy(false))
                            .column("schema_id", DataTypes.STRING().copy(false))
                            .column("format", DataTypes.STRING().copy(false))
                            .column("reserved_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("confluent_id")
                            .build());

    private SystemTables() {}

    /** Every system table, in bootstrap order (namespaces first, grants after their targets). */
    public static Table[] all() {
        return new Table[] {
            NAMESPACES, TABLES, SCHEMAS, PRINCIPALS, GRANTS, KAFKA_BINDINGS, ID_RESERVATIONS
        };
    }

    /** Schema + descriptor for one system table. */
    public static final class Table {
        private final String name;
        private final Schema schema;

        Table(String name, Schema schema) {
            this.name = name;
            this.schema = schema;
        }

        public String name() {
            return name;
        }

        public Schema schema() {
            return schema;
        }

        public TablePath tablePath() {
            return new TablePath(DATABASE, name);
        }

        public TableDescriptor descriptor() {
            return TableDescriptor.builder().schema(schema).distributedBy(DEFAULT_BUCKETS).build();
        }
    }
}
