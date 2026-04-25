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

    /**
     * Bucket count for the low-volume Kafka transaction-coordinator tables ({@code
     * __kafka_txn_state__}, {@code __kafka_producer_ids__}). These tables hold one row per {@code
     * transactional.id} (a few thousand at most) and one row per allocated producer id; 4 buckets
     * is plenty and keeps bucket-leadership-propagation cheap on cluster bootstrap.
     */
    public static final int KAFKA_TXN_BUCKETS = 4;

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

    /**
     * Kafka-compatible client-quotas store. PK is the composite {@code (entityType, entityName,
     * quotaKey)}; {@code entityName} is the empty string for the "default entity" of its type.
     * Phase I.3 (Path A) is storage-only — Produce / Fetch paths don't consult these rows.
     */
    public static final Table CLIENT_QUOTAS =
            new Table(
                    "_client_quotas",
                    Schema.newBuilder()
                            .column("entity_type", DataTypes.STRING().copy(false))
                            .column("entity_name", DataTypes.STRING().copy(false))
                            .column("quota_key", DataTypes.STRING().copy(false))
                            .column("quota_value", DataTypes.DOUBLE().copy(false))
                            .column("updated_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("entity_type", "entity_name", "quota_key")
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

    /**
     * Free-form string-keyed store used by the Schema Registry projection for global and
     * per-subject compatibility / mode config. Keys follow the convention {@code
     * global_compatibility}, {@code subject_compatibility:<subject>}, {@code global_mode}, {@code
     * subject_mode:<subject>}.
     */
    public static final Table SR_CONFIG =
            new Table(
                    "_sr_config",
                    Schema.newBuilder()
                            .column("config_key", DataTypes.STRING().copy(false))
                            .column("config_value", DataTypes.STRING().copy(false))
                            .primaryKey("config_key")
                            .build());

    /**
     * Edge table for Confluent SR schema references. PK is {@code (referrer_schema_id,
     * reference_name)}; trailing fields capture the wire-side {@code (subject, version)} plus the
     * pinned referent UUID. See design 0013.
     */
    /**
     * Kafka transaction state per {@code transactional.id} (design 0016 §5). One row per
     * transactional id; rewritten on every state-machine transition. Owned by the {@code
     * TransactionCoordinator} on the elected coordinator leader.
     */
    public static final Table KAFKA_TXN_STATE =
            new Table(
                    "_kafka_txn_state",
                    Schema.newBuilder()
                            .column("transactional_id", DataTypes.STRING().copy(false))
                            .column("producer_id", DataTypes.BIGINT().copy(false))
                            .column("producer_epoch", DataTypes.SMALLINT().copy(false))
                            .column("state", DataTypes.STRING().copy(false))
                            .column("topic_partitions", DataTypes.STRING())
                            // Phase J.3 — durable group bindings for ADD_OFFSETS_TO_TXN.
                            .column("group_ids", DataTypes.STRING())
                            .column("timeout_ms", DataTypes.INT().copy(false))
                            .column("txn_start_timestamp", DataTypes.TIMESTAMP_LTZ(3))
                            .column("last_updated_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("transactional_id")
                            .build(),
                    KAFKA_TXN_BUCKETS);

    /**
     * Kafka producer-id allocations (design 0016 §6). One row per allocated producer id;
     * accumulates over time. The coordinator-leader's allocator scans for {@code MAX(producer_id)}
     * on bootstrap to seed its in-memory counter.
     */
    public static final Table KAFKA_PRODUCER_IDS =
            new Table(
                    "_kafka_producer_ids",
                    Schema.newBuilder()
                            .column("producer_id", DataTypes.BIGINT().copy(false))
                            .column("transactional_id", DataTypes.STRING())
                            .column("epoch", DataTypes.SMALLINT().copy(false))
                            .column("allocated_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("producer_id")
                            .build(),
                    KAFKA_TXN_BUCKETS);

    /**
     * Kafka transactional-offset durable buffer (Phase J.3 §10). On {@code TXN_OFFSET_COMMIT} a row
     * is upserted; on {@code END_TXN(commit)} the rows for the {@code transactional_id} are
     * read+flushed to {@code __consumer_offsets__} and deleted. On {@code END_TXN(abort)} they are
     * simply deleted.
     */
    public static final Table KAFKA_TXN_OFFSET_BUFFER =
            new Table(
                    "_kafka_txn_offset_buffer",
                    Schema.newBuilder()
                            .column("transactional_id", DataTypes.STRING().copy(false))
                            .column("group_id", DataTypes.STRING().copy(false))
                            .column("topic", DataTypes.STRING().copy(false))
                            .column("partition", DataTypes.INT().copy(false))
                            .column("offset", DataTypes.BIGINT().copy(false))
                            .column("leader_epoch", DataTypes.INT().copy(false))
                            .column("metadata", DataTypes.STRING())
                            .column("committed_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("transactional_id", "group_id", "topic", "partition")
                            .build(),
                    KAFKA_TXN_BUCKETS);

    public static final Table SCHEMA_REFERENCES =
            new Table(
                    "_schema_references",
                    Schema.newBuilder()
                            .column("referrer_schema_id", DataTypes.STRING().copy(false))
                            .column("reference_name", DataTypes.STRING().copy(false))
                            .column("referenced_subject", DataTypes.STRING().copy(false))
                            .column("referenced_version", DataTypes.INT().copy(false))
                            .column("referenced_schema_id", DataTypes.STRING().copy(false))
                            .column("created_at", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                            .primaryKey("referrer_schema_id", "reference_name")
                            .build());

    private SystemTables() {}

    /** Every system table, in bootstrap order (namespaces first, grants after their targets). */
    public static Table[] all() {
        return new Table[] {
            NAMESPACES,
            TABLES,
            SCHEMAS,
            PRINCIPALS,
            GRANTS,
            KAFKA_BINDINGS,
            CLIENT_QUOTAS,
            ID_RESERVATIONS,
            SR_CONFIG,
            SCHEMA_REFERENCES,
            KAFKA_TXN_STATE,
            KAFKA_PRODUCER_IDS,
            KAFKA_TXN_OFFSET_BUFFER
        };
    }

    /** Schema + descriptor for one system table. */
    public static final class Table {
        private final String name;
        private final Schema schema;
        private final int buckets;

        Table(String name, Schema schema) {
            this(name, schema, DEFAULT_BUCKETS);
        }

        Table(String name, Schema schema, int buckets) {
            this.name = name;
            this.schema = schema;
            this.buckets = buckets;
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
            return TableDescriptor.builder().schema(schema).distributedBy(buckets).build();
        }
    }
}
