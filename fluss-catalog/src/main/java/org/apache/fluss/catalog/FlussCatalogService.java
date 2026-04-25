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
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.catalog.entities.ClientQuotaEntry;
import org.apache.fluss.catalog.entities.ClientQuotaFilter;
import org.apache.fluss.catalog.entities.GrantEntity;
import org.apache.fluss.catalog.entities.KafkaProducerIdEntity;
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.KafkaTxnStateEntity;
import org.apache.fluss.catalog.entities.NamespaceEntity;
import org.apache.fluss.catalog.entities.PrincipalEntity;
import org.apache.fluss.catalog.entities.SchemaReference;
import org.apache.fluss.catalog.entities.SchemaReferenceEntity;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * {@link CatalogService} backed by Fluss PK tables in the reserved {@code _catalog} database.
 * Creates the database + seven system tables on first use; subsequent calls hit the lazy table
 * handle cache.
 *
 * <p>Writes go through {@link UpsertWriter#upsert(InternalRow)} (last-writer-wins semantics); reads
 * through {@link Lookuper#lookup(InternalRow)} for PK fetches and a short {@link
 * org.apache.fluss.client.table.scanner.batch.BatchScanner} scan for list endpoints.
 */
@Internal
public final class FlussCatalogService implements CatalogService, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlussCatalogService.class);

    private static final long TIMEOUT_SECONDS = 20L;
    private static final int CONFLUENT_ID_PROBE_LIMIT = 8;
    private static final Duration SCAN_POLL_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Lazy-opened Fluss client {@link Connection}. The bootstrap that owns this catalog passes a
     * supplier that opens the connection on first use — critical because the catalog is
     * instantiated from inside {@code CoordinatorServer#initCoordinatorLeader}, at which point the
     * coordinator has not yet announced itself as leader and its own metadata RPCs reject client
     * calls with {@code NotCoordinatorLeaderException}.
     */
    private final java.util.function.Supplier<Connection> connectionSupplier;

    private volatile Connection connection;
    private final Object connectionLock = new Object();

    private final Object bootstrapLock = new Object();

    /**
     * Tables for which {@link #waitUntilReady(Table, Handle)} has already returned successfully. On
     * subsequent {@link #table(Handle)} calls we can skip the readiness probe — bucket leadership
     * only goes away on a coordinator-leader failover, at which point the singleton itself is
     * rebuilt. This is the hot-path cache for the high-frequency upserts the txn coordinator drives
     * during state-machine transitions.
     */
    private final java.util.Set<Handle> readyHandles =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    private enum Handle {
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
        KAFKA_PRODUCER_IDS
    }

    /**
     * In-memory monotonic counter used by {@link #allocateProducerId(String)}. Lazily seeded from
     * {@code MAX(producer_id) + 1} on first use; bumped atomically on every allocation. The Catalog
     * instance is hosted on the elected coordinator leader, so writes to {@code
     * __kafka_producer_ids__} are single-writer by construction (design 0016 §6).
     */
    private final java.util.concurrent.atomic.AtomicLong nextProducerId =
            new java.util.concurrent.atomic.AtomicLong(0L);

    private final Object producerIdSeedLock = new Object();
    private volatile boolean producerIdSeeded;

    public FlussCatalogService(Connection connection) {
        this(() -> connection);
    }

    public FlussCatalogService(java.util.function.Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    private Connection connection() {
        Connection c = connection;
        if (c != null) {
            return c;
        }
        synchronized (connectionLock) {
            if (connection == null) {
                connection = connectionSupplier.get();
            }
            return connection;
        }
    }

    // =================================================================
    // Namespaces
    // =================================================================

    @Override
    public NamespaceEntity createNamespace(
            @Nullable String parentFqn, String name, @Nullable String description)
            throws Exception {
        requireNonEmpty("name", name);
        String parentId = null;
        if (parentFqn != null && !parentFqn.isEmpty()) {
            parentId =
                    getNamespace(parentFqn)
                            .orElseThrow(
                                    () ->
                                            new CatalogException(
                                                    CatalogException.Kind.NOT_FOUND,
                                                    "parent namespace not found: " + parentFqn))
                            .namespaceId();
        }
        String fqn = parentFqn == null || parentFqn.isEmpty() ? name : parentFqn + "." + name;
        Optional<NamespaceEntity> existing = getNamespace(fqn);
        if (existing.isPresent()) {
            throw new CatalogException(
                    CatalogException.Kind.ALREADY_EXISTS, "namespace exists: " + fqn);
        }
        NamespaceEntity entity =
                new NamespaceEntity(
                        UUID.randomUUID().toString(),
                        parentId,
                        name,
                        description,
                        System.currentTimeMillis());
        GenericRow row = new GenericRow(5);
        row.setField(0, BinaryString.fromString(entity.namespaceId()));
        row.setField(1, parentId == null ? null : BinaryString.fromString(parentId));
        row.setField(2, BinaryString.fromString(name));
        row.setField(3, description == null ? null : BinaryString.fromString(description));
        row.setField(4, TimestampLtz.fromEpochMillis(entity.createdAtMillis()));
        upsert(Handle.NAMESPACES, row);
        return entity;
    }

    @Override
    public Optional<NamespaceEntity> getNamespace(String fqn) throws Exception {
        return findNamespaceByFqn(fqn);
    }

    @Override
    public List<NamespaceEntity> listNamespaces(@Nullable String parentFqn) throws Exception {
        String parentId =
                parentFqn == null || parentFqn.isEmpty()
                        ? null
                        : getNamespace(parentFqn)
                                .orElseThrow(
                                        () ->
                                                new CatalogException(
                                                        CatalogException.Kind.NOT_FOUND,
                                                        "parent namespace not found: " + parentFqn))
                                .namespaceId();
        List<NamespaceEntity> out = new ArrayList<>();
        for (NamespaceEntity ns : scanAllNamespaces()) {
            if (parentId == null ? ns.parentId() == null : parentId.equals(ns.parentId())) {
                out.add(ns);
            }
        }
        return out;
    }

    @Override
    public void dropNamespace(String fqn, boolean cascade) throws Exception {
        NamespaceEntity ns =
                getNamespace(fqn)
                        .orElseThrow(
                                () ->
                                        new CatalogException(
                                                CatalogException.Kind.NOT_FOUND,
                                                "namespace not found: " + fqn));
        // A table's existence inside the namespace blocks drop unless cascade.
        List<CatalogTableEntity> ownedTables = listTablesByNamespaceId(ns.namespaceId());
        if (!ownedTables.isEmpty() && !cascade) {
            throw new CatalogException(
                    CatalogException.Kind.CONFLICT,
                    "namespace is not empty (" + ownedTables.size() + " tables): " + fqn);
        }
        for (CatalogTableEntity table : ownedTables) {
            dropTableById(table);
        }
        delete(Handle.NAMESPACES, deletePlaceholder(Handle.NAMESPACES, ns.namespaceId()));
    }

    // =================================================================
    // Tables
    // =================================================================

    @Override
    public CatalogTableEntity createTable(
            String namespace,
            String name,
            String format,
            String backingRef,
            @Nullable String createdBy)
            throws Exception {
        requireNonEmpty("namespace", namespace);
        requireNonEmpty("name", name);
        requireNonEmpty("format", format);
        requireNonEmpty("backingRef", backingRef);
        NamespaceEntity ns =
                getNamespace(namespace)
                        .orElseThrow(
                                () ->
                                        new CatalogException(
                                                CatalogException.Kind.NOT_FOUND,
                                                "namespace not found: " + namespace));
        if (getTable(namespace, name).isPresent()) {
            throw new CatalogException(
                    CatalogException.Kind.ALREADY_EXISTS,
                    "table exists: " + namespace + "." + name);
        }
        CatalogTableEntity entity =
                new CatalogTableEntity(
                        UUID.randomUUID().toString(),
                        ns.namespaceId(),
                        name,
                        format,
                        backingRef,
                        null,
                        createdBy,
                        System.currentTimeMillis());
        upsertTableRow(entity);
        return entity;
    }

    @Override
    public Optional<CatalogTableEntity> getTable(String namespace, String name) throws Exception {
        Optional<NamespaceEntity> ns = getNamespace(namespace);
        if (!ns.isPresent()) {
            return Optional.empty();
        }
        for (CatalogTableEntity table : listTablesByNamespaceId(ns.get().namespaceId())) {
            if (table.name().equals(name)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }

    @Override
    public List<CatalogTableEntity> listTables(String namespace) throws Exception {
        Optional<NamespaceEntity> ns = getNamespace(namespace);
        return ns.map(
                        n -> {
                            try {
                                return listTablesByNamespaceId(n.namespaceId());
                            } catch (Exception e) {
                                throw internal("listTables", e);
                            }
                        })
                .orElse(Collections.emptyList());
    }

    @Override
    public void dropTable(String namespace, String name) throws Exception {
        Optional<CatalogTableEntity> table = getTable(namespace, name);
        if (!table.isPresent()) {
            throw new CatalogException(
                    CatalogException.Kind.NOT_FOUND, "table not found: " + namespace + "." + name);
        }
        dropTableById(table.get());
    }

    // =================================================================
    // Schema history
    // =================================================================

    @Override
    public SchemaVersionEntity registerSchema(
            String namespace,
            String tableName,
            String format,
            String schemaText,
            @Nullable String registeredBy)
            throws Exception {
        requireNonEmpty("schemaText", schemaText);
        CatalogTableEntity table =
                getTable(namespace, tableName)
                        .orElseThrow(
                                () ->
                                        new CatalogException(
                                                CatalogException.Kind.NOT_FOUND,
                                                "table not found: " + namespace + "." + tableName));
        List<SchemaVersionEntity> existingVersions = listSchemaVersionsByTableId(table.tableId());
        // Idempotent re-submission — exact text match wins, return the same row.
        for (SchemaVersionEntity prior : existingVersions) {
            if (prior.format().equals(format) && prior.schemaText().equals(schemaText)) {
                return prior;
            }
        }
        int nextVersion =
                existingVersions.isEmpty()
                        ? 1
                        : existingVersions.stream()
                                        .mapToInt(SchemaVersionEntity::version)
                                        .max()
                                        .getAsInt()
                                + 1;
        String schemaId = UUID.randomUUID().toString();
        int confluentId = allocateConfluentId(table.tableId(), nextVersion, format, schemaId);
        SchemaVersionEntity entity =
                new SchemaVersionEntity(
                        schemaId,
                        table.tableId(),
                        nextVersion,
                        format,
                        schemaText,
                        confluentId,
                        registeredBy,
                        System.currentTimeMillis());
        GenericRow row = new GenericRow(8);
        row.setField(0, BinaryString.fromString(schemaId));
        row.setField(1, BinaryString.fromString(table.tableId()));
        row.setField(2, nextVersion);
        row.setField(3, BinaryString.fromString(format));
        row.setField(4, BinaryString.fromString(schemaText));
        row.setField(5, confluentId);
        row.setField(6, registeredBy == null ? null : BinaryString.fromString(registeredBy));
        row.setField(7, TimestampLtz.fromEpochMillis(entity.registeredAtMillis()));
        upsert(Handle.SCHEMAS, row);
        // Advance table's current_schema_id pointer.
        upsertTableRow(
                new CatalogTableEntity(
                        table.tableId(),
                        table.namespaceId(),
                        table.name(),
                        table.format(),
                        table.backingRef(),
                        schemaId,
                        table.createdBy(),
                        table.createdAtMillis()));
        return entity;
    }

    @Override
    public Optional<SchemaVersionEntity> getSchemaVersion(
            String namespace, String tableName, int version) throws Exception {
        Optional<CatalogTableEntity> table = getTable(namespace, tableName);
        if (!table.isPresent()) {
            return Optional.empty();
        }
        for (SchemaVersionEntity s : listSchemaVersionsByTableId(table.get().tableId())) {
            if (s.version() == version) {
                return Optional.of(s);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<SchemaVersionEntity> getSchemaById(int confluentId) throws Exception {
        Lookuper lookuper = table(Handle.ID_RESERVATIONS).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, confluentId);
        InternalRow found = awaitLookup(lookuper, key);
        if (found == null) {
            return Optional.empty();
        }
        String schemaId = found.getString(1).toString();
        return getSchemaBySchemaId(schemaId);
    }

    @Override
    public List<SchemaVersionEntity> listSchemaVersions(String namespace, String tableName)
            throws Exception {
        Optional<CatalogTableEntity> table = getTable(namespace, tableName);
        if (!table.isPresent()) {
            return Collections.emptyList();
        }
        return listSchemaVersionsByTableId(table.get().tableId());
    }

    // =================================================================
    // Kafka subject bindings
    // =================================================================

    @Override
    public KafkaSubjectBinding bindKafkaSubject(
            String subject,
            String namespace,
            String tableName,
            String keyOrValue,
            String namingStrategy)
            throws Exception {
        requireNonEmpty("subject", subject);
        CatalogTableEntity table =
                getTable(namespace, tableName)
                        .orElseThrow(
                                () ->
                                        new CatalogException(
                                                CatalogException.Kind.NOT_FOUND,
                                                "table not found: " + namespace + "." + tableName));
        GenericRow row = new GenericRow(4);
        row.setField(0, BinaryString.fromString(subject));
        row.setField(1, BinaryString.fromString(table.tableId()));
        row.setField(2, BinaryString.fromString(keyOrValue));
        row.setField(3, BinaryString.fromString(namingStrategy));
        upsert(Handle.KAFKA_BINDINGS, row);
        return new KafkaSubjectBinding(subject, table.tableId(), keyOrValue, namingStrategy);
    }

    @Override
    public Optional<KafkaSubjectBinding> resolveKafkaSubject(String subject) throws Exception {
        Lookuper lookuper = table(Handle.KAFKA_BINDINGS).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, BinaryString.fromString(subject));
        InternalRow found = awaitLookup(lookuper, key);
        if (found == null) {
            return Optional.empty();
        }
        return Optional.of(
                new KafkaSubjectBinding(
                        found.getString(0).toString(),
                        found.getString(1).toString(),
                        found.getString(2).toString(),
                        found.getString(3).toString()));
    }

    @Override
    public List<KafkaSubjectBinding> listKafkaSubjects() throws Exception {
        List<KafkaSubjectBinding> out = new ArrayList<>();
        scan(
                Handle.KAFKA_BINDINGS,
                row ->
                        out.add(
                                new KafkaSubjectBinding(
                                        row.getString(0).toString(),
                                        row.getString(1).toString(),
                                        row.getString(2).toString(),
                                        row.getString(3).toString())));
        return out;
    }

    @Override
    public void unbindKafkaSubject(String subject) throws Exception {
        requireNonEmpty("subject", subject);
        if (!resolveKafkaSubject(subject).isPresent()) {
            return;
        }
        delete(Handle.KAFKA_BINDINGS, deletePlaceholder(Handle.KAFKA_BINDINGS, subject));
    }

    @Override
    public void deleteSchemaVersion(String schemaId) throws Exception {
        requireNonEmpty("schemaId", schemaId);
        if (!getSchemaBySchemaId(schemaId).isPresent()) {
            return;
        }
        delete(Handle.SCHEMAS, deletePlaceholder(Handle.SCHEMAS, schemaId));
    }

    // =================================================================
    // Schema references
    // =================================================================

    @Override
    public void bindReferences(String referrerSchemaId, List<SchemaReference> refs)
            throws Exception {
        requireNonEmpty("referrerSchemaId", referrerSchemaId);
        if (refs == null) {
            refs = Collections.emptyList();
        }
        // Full replacement: delete every prior edge owned by the referrer first.
        List<SchemaReferenceEntity> existing = scanReferenceRowsByReferrer(referrerSchemaId);
        for (SchemaReferenceEntity row : existing) {
            deleteSchemaReferenceRow(row.referrerSchemaId(), row.referenceName());
        }
        long now = System.currentTimeMillis();
        for (SchemaReference ref : refs) {
            requireNonEmpty("reference name", ref.name());
            requireNonEmpty("reference subject", ref.subject());
            requireNonEmpty("referencedSchemaId", ref.referencedSchemaId());
            GenericRow row = new GenericRow(6);
            row.setField(0, BinaryString.fromString(referrerSchemaId));
            row.setField(1, BinaryString.fromString(ref.name()));
            row.setField(2, BinaryString.fromString(ref.subject()));
            row.setField(3, ref.version());
            row.setField(4, BinaryString.fromString(ref.referencedSchemaId()));
            row.setField(5, TimestampLtz.fromEpochMillis(now));
            upsert(Handle.SCHEMA_REFERENCES, row);
        }
    }

    @Override
    public List<SchemaReference> listReferences(String referrerSchemaId) throws Exception {
        requireNonEmpty("referrerSchemaId", referrerSchemaId);
        List<SchemaReferenceEntity> rows = scanReferenceRowsByReferrer(referrerSchemaId);
        // Stable ordering: by createdAt then by name. Insertion order is approximated by
        // createdAt; ties on createdAt (same write batch / clock granularity) sort by name.
        rows.sort(
                (a, b) -> {
                    int byTime = Long.compare(a.createdAtMillis(), b.createdAtMillis());
                    if (byTime != 0) {
                        return byTime;
                    }
                    return a.referenceName().compareTo(b.referenceName());
                });
        List<SchemaReference> out = new ArrayList<>(rows.size());
        for (SchemaReferenceEntity row : rows) {
            out.add(row.toReference());
        }
        return out;
    }

    @Override
    public List<String> listReferencedBy(String referencedSchemaId) throws Exception {
        requireNonEmpty("referencedSchemaId", referencedSchemaId);
        List<String> out = new ArrayList<>();
        scan(
                Handle.SCHEMA_REFERENCES,
                row -> {
                    String hit = row.getString(4).toString();
                    if (referencedSchemaId.equals(hit)) {
                        String referrer = row.getString(0).toString();
                        if (!out.contains(referrer)) {
                            out.add(referrer);
                        }
                    }
                });
        return out;
    }

    @Override
    public void deleteReferences(String referrerSchemaId) throws Exception {
        requireNonEmpty("referrerSchemaId", referrerSchemaId);
        for (SchemaReferenceEntity row : scanReferenceRowsByReferrer(referrerSchemaId)) {
            deleteSchemaReferenceRow(row.referrerSchemaId(), row.referenceName());
        }
    }

    private List<SchemaReferenceEntity> scanReferenceRowsByReferrer(String referrerSchemaId)
            throws Exception {
        List<SchemaReferenceEntity> out = new ArrayList<>();
        scan(
                Handle.SCHEMA_REFERENCES,
                row -> {
                    String rowReferrer = row.getString(0).toString();
                    if (!referrerSchemaId.equals(rowReferrer)) {
                        return;
                    }
                    out.add(decodeSchemaReference(row));
                });
        return out;
    }

    private static SchemaReferenceEntity decodeSchemaReference(InternalRow row) {
        return new SchemaReferenceEntity(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getString(2).toString(),
                row.getInt(3),
                row.getString(4).toString(),
                row.getTimestampLtz(5, 3).toEpochMicros() / 1000);
    }

    private void deleteSchemaReferenceRow(String referrerSchemaId, String referenceName)
            throws Exception {
        GenericRow row = new GenericRow(6);
        row.setField(0, BinaryString.fromString(referrerSchemaId));
        row.setField(1, BinaryString.fromString(referenceName));
        row.setField(2, BinaryString.fromString(""));
        row.setField(3, 0);
        row.setField(4, BinaryString.fromString(""));
        row.setField(5, TimestampLtz.fromEpochMillis(0));
        delete(Handle.SCHEMA_REFERENCES, row);
    }

    // =================================================================
    // Client quotas (Phase I.3, Path A: storage only)
    // =================================================================

    @Override
    public ClientQuotaEntry upsertClientQuota(
            String entityType, String entityName, String quotaKey, double quotaValue)
            throws Exception {
        requireNonEmpty("entityType", entityType);
        // entityName is allowed to be the empty string (Kafka convention for "default entity").
        if (entityName == null) {
            throw new CatalogException(
                    CatalogException.Kind.INVALID_INPUT, "entityName is required (may be empty)");
        }
        requireNonEmpty("quotaKey", quotaKey);
        long nowMs = System.currentTimeMillis();
        GenericRow row = new GenericRow(5);
        row.setField(0, BinaryString.fromString(entityType));
        row.setField(1, BinaryString.fromString(entityName));
        row.setField(2, BinaryString.fromString(quotaKey));
        row.setField(3, quotaValue);
        row.setField(4, TimestampLtz.fromEpochMillis(nowMs));
        upsert(Handle.CLIENT_QUOTAS, row);
        return new ClientQuotaEntry(entityType, entityName, quotaKey, quotaValue, nowMs);
    }

    @Override
    public void deleteClientQuota(String entityType, String entityName, String quotaKey)
            throws Exception {
        requireNonEmpty("entityType", entityType);
        if (entityName == null) {
            throw new CatalogException(
                    CatalogException.Kind.INVALID_INPUT, "entityName is required (may be empty)");
        }
        requireNonEmpty("quotaKey", quotaKey);
        deleteClientQuotaRow(entityType, entityName, quotaKey);
    }

    @Override
    public List<ClientQuotaEntry> listClientQuotas(ClientQuotaFilter filter) throws Exception {
        List<ClientQuotaEntry> all = new ArrayList<>();
        scan(
                Handle.CLIENT_QUOTAS,
                row -> {
                    String entityType = row.getString(0).toString();
                    String entityName = row.getString(1).toString();
                    String quotaKey = row.getString(2).toString();
                    if ("__readiness__".equals(entityType)) {
                        return; // skip sentinel probe rows if any ever slip in
                    }
                    double quotaValue = row.getDouble(3);
                    long updatedAt = row.getTimestampLtz(4, 3).toEpochMicros() / 1000;
                    all.add(
                            new ClientQuotaEntry(
                                    entityType, entityName, quotaKey, quotaValue, updatedAt));
                });
        if (filter.components().isEmpty()) {
            return all;
        }
        List<ClientQuotaEntry> out = new ArrayList<>();
        for (ClientQuotaEntry e : all) {
            if (matches(e, filter)) {
                out.add(e);
            }
        }
        return out;
    }

    private static boolean matches(ClientQuotaEntry entry, ClientQuotaFilter filter) {
        // Strict mode: every entity-type dimension in the entry must appear in filter components
        // and match. Because our storage schema has a single entity-type per row, strictness
        // reduces to "one component, this component's entityType equals the row's".
        if (filter.strict() && filter.components().size() != 1) {
            return false;
        }
        for (ClientQuotaFilter.Component c : filter.components()) {
            if (!c.entityType().equals(entry.entityType())) {
                if (filter.strict()) {
                    return false;
                }
                continue; // lenient — a component over a different type doesn't reject the row
            }
            if (c.matchDefault()) {
                if (!entry.isDefault()) {
                    return false;
                }
            } else if (c.entityName() != null) {
                if (!c.entityName().equals(entry.entityName())) {
                    return false;
                }
            }
            // ofEntityType (no name, no matchDefault) matches any name under this type.
            return true;
        }
        // No matching component found for the entry's entity type.
        return false;
    }

    // =================================================================
    // SR config (key/value store for compatibility + mode)
    // =================================================================

    @Override
    public Optional<org.apache.fluss.catalog.entities.SrConfigEntry> getSrConfig(String key)
            throws Exception {
        requireNonEmpty("key", key);
        Lookuper lookuper = table(Handle.SR_CONFIG).newLookup().createLookuper();
        GenericRow keyRow = new GenericRow(1);
        keyRow.setField(0, BinaryString.fromString(key));
        InternalRow row = awaitLookup(lookuper, keyRow);
        if (row == null) {
            return Optional.empty();
        }
        String k = row.getString(0).toString();
        String v = row.getString(1).toString();
        return Optional.of(new org.apache.fluss.catalog.entities.SrConfigEntry(k, v));
    }

    @Override
    public org.apache.fluss.catalog.entities.SrConfigEntry setSrConfig(String key, String value)
            throws Exception {
        requireNonEmpty("key", key);
        if (value == null) {
            throw new CatalogException(
                    CatalogException.Kind.INVALID_INPUT,
                    "value is required (use deleteSrConfig to clear)");
        }
        GenericRow row = new GenericRow(2);
        row.setField(0, BinaryString.fromString(key));
        row.setField(1, BinaryString.fromString(value));
        upsert(Handle.SR_CONFIG, row);
        return new org.apache.fluss.catalog.entities.SrConfigEntry(key, value);
    }

    @Override
    public void deleteSrConfig(String key) throws Exception {
        requireNonEmpty("key", key);
        // Fluss UpsertWriter#delete needs a full-width row even though only the PK is consulted.
        GenericRow keyRow = new GenericRow(2);
        keyRow.setField(0, BinaryString.fromString(key));
        keyRow.setField(1, BinaryString.fromString(""));
        delete(Handle.SR_CONFIG, keyRow);
    }

    // =================================================================
    // Principals
    // =================================================================

    @Override
    public PrincipalEntity ensurePrincipal(String name, String type) throws Exception {
        requireNonEmpty("name", name);
        Optional<PrincipalEntity> existing = getPrincipal(name);
        if (existing.isPresent()) {
            return existing.get();
        }
        PrincipalEntity entity =
                new PrincipalEntity(
                        UUID.randomUUID().toString(),
                        name,
                        type == null ? PrincipalEntity.TYPE_USER : type,
                        System.currentTimeMillis());
        GenericRow row = new GenericRow(4);
        row.setField(0, BinaryString.fromString(entity.principalId()));
        row.setField(1, BinaryString.fromString(entity.name()));
        row.setField(2, BinaryString.fromString(entity.type()));
        row.setField(3, TimestampLtz.fromEpochMillis(entity.createdAtMillis()));
        upsert(Handle.PRINCIPALS, row);
        return entity;
    }

    @Override
    public Optional<PrincipalEntity> getPrincipal(String name) throws Exception {
        for (PrincipalEntity p : scanAllPrincipals()) {
            if (p.name().equals(name)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    @Override
    public List<PrincipalEntity> listPrincipals() throws Exception {
        return scanAllPrincipals();
    }

    // =================================================================
    // Grants
    // =================================================================

    @Override
    public GrantEntity grant(
            String principalName,
            String entityKind,
            String entityId,
            String privilege,
            String grantedBy)
            throws Exception {
        requireNonEmpty("principalName", principalName);
        requireNonEmpty("entityKind", entityKind);
        requireNonEmpty("entityId", entityId);
        requireNonEmpty("privilege", privilege);
        PrincipalEntity principal = ensurePrincipal(principalName, PrincipalEntity.TYPE_USER);
        // Idempotent on (principal, entityKind, entityId, privilege).
        for (GrantEntity g : scanAllGrants()) {
            if (g.principalId().equals(principal.principalId())
                    && g.entityKind().equals(entityKind)
                    && g.entityId().equals(entityId)
                    && g.privilege().equals(privilege)) {
                return g;
            }
        }
        GrantEntity entity =
                new GrantEntity(
                        UUID.randomUUID().toString(),
                        principal.principalId(),
                        entityKind,
                        entityId,
                        privilege,
                        grantedBy,
                        System.currentTimeMillis());
        GenericRow row = new GenericRow(7);
        row.setField(0, BinaryString.fromString(entity.grantId()));
        row.setField(1, BinaryString.fromString(entity.principalId()));
        row.setField(2, BinaryString.fromString(entity.entityKind()));
        row.setField(3, BinaryString.fromString(entity.entityId()));
        row.setField(4, BinaryString.fromString(entity.privilege()));
        row.setField(5, grantedBy == null ? null : BinaryString.fromString(grantedBy));
        row.setField(6, TimestampLtz.fromEpochMillis(entity.grantedAtMillis()));
        upsert(Handle.GRANTS, row);
        return entity;
    }

    @Override
    public void revoke(String principalName, String entityKind, String entityId, String privilege)
            throws Exception {
        Optional<PrincipalEntity> principal = getPrincipal(principalName);
        if (!principal.isPresent()) {
            return;
        }
        for (GrantEntity g : scanAllGrants()) {
            if (g.principalId().equals(principal.get().principalId())
                    && g.entityKind().equals(entityKind)
                    && g.entityId().equals(entityId)
                    && g.privilege().equals(privilege)) {
                delete(Handle.GRANTS, deletePlaceholder(Handle.GRANTS, g.grantId()));
                return;
            }
        }
    }

    @Override
    public List<GrantEntity> listGrantsForPrincipal(String principalName) throws Exception {
        Optional<PrincipalEntity> principal = getPrincipal(principalName);
        if (!principal.isPresent()) {
            return Collections.emptyList();
        }
        List<GrantEntity> out = new ArrayList<>();
        for (GrantEntity g : scanAllGrants()) {
            if (g.principalId().equals(principal.get().principalId())) {
                out.add(g);
            }
        }
        return out;
    }

    @Override
    public boolean checkPrivilege(
            String principalName, String entityKind, String entityId, String privilege)
            throws Exception {
        Optional<PrincipalEntity> principal = getPrincipal(principalName);
        if (!principal.isPresent()) {
            return false;
        }
        String pid = principal.get().principalId();
        for (GrantEntity g : scanAllGrants()) {
            if (!g.principalId().equals(pid) || !g.privilege().equals(privilege)) {
                continue;
            }
            // Direct match on the specific entity.
            if (g.entityKind().equals(entityKind) && g.entityId().equals(entityId)) {
                return true;
            }
            // Catalog-wide wildcard.
            if (GrantEntity.KIND_CATALOG.equals(g.entityKind())
                    && GrantEntity.CATALOG_WILDCARD.equals(g.entityId())) {
                return true;
            }
        }
        return false;
    }

    private List<PrincipalEntity> scanAllPrincipals() throws Exception {
        List<PrincipalEntity> out = new ArrayList<>();
        scan(
                Handle.PRINCIPALS,
                row ->
                        out.add(
                                new PrincipalEntity(
                                        row.getString(0).toString(),
                                        row.getString(1).toString(),
                                        row.getString(2).toString(),
                                        row.getTimestampLtz(3, 3).toEpochMicros() / 1000)));
        return out;
    }

    private List<GrantEntity> scanAllGrants() throws Exception {
        List<GrantEntity> out = new ArrayList<>();
        scan(
                Handle.GRANTS,
                row ->
                        out.add(
                                new GrantEntity(
                                        row.getString(0).toString(),
                                        row.getString(1).toString(),
                                        row.getString(2).toString(),
                                        row.getString(3).toString(),
                                        row.getString(4).toString(),
                                        row.isNullAt(5) ? null : row.getString(5).toString(),
                                        row.getTimestampLtz(6, 3).toEpochMicros() / 1000)));
        return out;
    }

    // =================================================================
    // Kafka transactions (Phase J.1)
    // =================================================================

    @Override
    public KafkaTxnStateEntity upsertKafkaTxnState(KafkaTxnStateEntity entity) throws Exception {
        if (entity == null) {
            throw new CatalogException(CatalogException.Kind.INVALID_INPUT, "entity is required");
        }
        requireNonEmpty("transactionalId", entity.transactionalId());
        requireNonEmpty("state", entity.state());
        GenericRow row = new GenericRow(8);
        row.setField(0, BinaryString.fromString(entity.transactionalId()));
        row.setField(1, entity.producerId());
        row.setField(2, entity.producerEpoch());
        row.setField(3, BinaryString.fromString(entity.state()));
        String encoded = KafkaTxnStateEntity.encodePartitions(entity.topicPartitions());
        row.setField(4, encoded == null ? null : BinaryString.fromString(encoded));
        row.setField(5, entity.timeoutMs());
        row.setField(
                6,
                entity.txnStartTimestampMillis() == null
                        ? null
                        : TimestampLtz.fromEpochMillis(entity.txnStartTimestampMillis()));
        row.setField(7, TimestampLtz.fromEpochMillis(entity.lastUpdatedAtMillis()));
        upsert(Handle.KAFKA_TXN_STATE, row);
        return entity;
    }

    @Override
    public Optional<KafkaTxnStateEntity> getKafkaTxnState(String transactionalId) throws Exception {
        requireNonEmpty("transactionalId", transactionalId);
        Lookuper lookuper = table(Handle.KAFKA_TXN_STATE).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, BinaryString.fromString(transactionalId));
        InternalRow row = awaitLookup(lookuper, key);
        return Optional.ofNullable(row).map(FlussCatalogService::decodeKafkaTxnState);
    }

    @Override
    public List<KafkaTxnStateEntity> listKafkaTxnStates() throws Exception {
        List<KafkaTxnStateEntity> out = new ArrayList<>();
        scan(Handle.KAFKA_TXN_STATE, row -> out.add(decodeKafkaTxnState(row)));
        return out;
    }

    @Override
    public void deleteKafkaTxnState(String transactionalId) throws Exception {
        requireNonEmpty("transactionalId", transactionalId);
        if (!getKafkaTxnState(transactionalId).isPresent()) {
            return;
        }
        delete(Handle.KAFKA_TXN_STATE, deletePlaceholder(Handle.KAFKA_TXN_STATE, transactionalId));
    }

    @Override
    public KafkaProducerIdEntity allocateProducerId(@Nullable String transactionalId)
            throws Exception {
        seedProducerIdCounterIfNeeded();
        long producerId = nextProducerId.getAndIncrement();
        long now = System.currentTimeMillis();
        GenericRow row = new GenericRow(4);
        row.setField(0, producerId);
        row.setField(1, transactionalId == null ? null : BinaryString.fromString(transactionalId));
        row.setField(2, (short) 0);
        row.setField(3, TimestampLtz.fromEpochMillis(now));
        upsert(Handle.KAFKA_PRODUCER_IDS, row);
        return new KafkaProducerIdEntity(producerId, transactionalId, (short) 0, now);
    }

    @Override
    public Optional<KafkaProducerIdEntity> getKafkaProducerId(long producerId) throws Exception {
        Lookuper lookuper = table(Handle.KAFKA_PRODUCER_IDS).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, producerId);
        InternalRow row = awaitLookup(lookuper, key);
        return Optional.ofNullable(row).map(FlussCatalogService::decodeKafkaProducerId);
    }

    @Override
    public List<KafkaProducerIdEntity> listKafkaProducerIds() throws Exception {
        List<KafkaProducerIdEntity> out = new ArrayList<>();
        scan(Handle.KAFKA_PRODUCER_IDS, row -> out.add(decodeKafkaProducerId(row)));
        return out;
    }

    private void seedProducerIdCounterIfNeeded() throws Exception {
        if (producerIdSeeded) {
            return;
        }
        synchronized (producerIdSeedLock) {
            if (producerIdSeeded) {
                return;
            }
            long maxAllocated = -1L;
            for (KafkaProducerIdEntity e : listKafkaProducerIds()) {
                if (e.producerId() > maxAllocated) {
                    maxAllocated = e.producerId();
                }
            }
            // First allocation produces id == max + 1; pre-seed at the cutover so getAndIncrement
            // hands out (maxAllocated + 1) on the next call. When the table is empty the seed is
            // 1L (Kafka producers reject id 0 as a sentinel).
            long seed = maxAllocated < 0 ? 1L : maxAllocated + 1L;
            nextProducerId.set(seed);
            producerIdSeeded = true;
            LOG.info("Seeded Kafka producer-id counter to {} from __kafka_producer_ids__", seed);
        }
    }

    private static KafkaTxnStateEntity decodeKafkaTxnState(InternalRow row) {
        String txnId = row.getString(0).toString();
        long producerId = row.getLong(1);
        short epoch = row.getShort(2);
        String state = row.getString(3).toString();
        java.util.Set<String> partitions =
                row.isNullAt(4)
                        ? java.util.Collections.emptySet()
                        : KafkaTxnStateEntity.decodePartitions(row.getString(4).toString());
        int timeoutMs = row.getInt(5);
        Long startTs = row.isNullAt(6) ? null : row.getTimestampLtz(6, 3).toEpochMicros() / 1000;
        long updatedAt = row.getTimestampLtz(7, 3).toEpochMicros() / 1000;
        return new KafkaTxnStateEntity(
                txnId, producerId, epoch, state, partitions, timeoutMs, startTs, updatedAt);
    }

    private static KafkaProducerIdEntity decodeKafkaProducerId(InternalRow row) {
        long producerId = row.getLong(0);
        String transactionalId = row.isNullAt(1) ? null : row.getString(1).toString();
        short epoch = row.getShort(2);
        long allocatedAt = row.getTimestampLtz(3, 3).toEpochMicros() / 1000;
        return new KafkaProducerIdEntity(producerId, transactionalId, epoch, allocatedAt);
    }

    // =================================================================
    // AutoCloseable
    // =================================================================

    @Override
    public void close() {
        Connection c = connection;
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.warn("Failed to close catalog Fluss client Connection", e);
            }
            connection = null;
        }
    }

    // =================================================================
    // Internals
    // =================================================================

    private Table table(Handle handle) throws Exception {
        synchronized (bootstrapLock) {
            // Always re-verify the database + table exist. Cheap when they do (ignoreIfExists
            // no-ops) and essential when an external actor (tests, administrative drops) has
            // deleted them.
            ensureDatabase();
            SystemTables.Table def = tableFor(handle);
            Admin admin = connection().getAdmin();
            boolean freshlyCreated = false;
            try {
                // ignoreIfExists=false so the catch block tells us whether we just created the
                // table or it was already there. Important for the readiness cache below: we
                // only want to skip waitUntilReady when we know the bucket-leadership window
                // has been paid in the current process lifetime — and we want to *re*-pay it
                // whenever an external actor (tests, administrative drops) recreates the table.
                admin.createTable(def.tablePath(), def.descriptor(), false)
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                freshlyCreated = true;
                readyHandles.remove(handle);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (!(cause instanceof TableAlreadyExistException)) {
                    throw unwrap(ee);
                }
            }
            Table t = connection().getTable(def.tablePath());
            if (freshlyCreated || readyHandles.add(handle)) {
                waitUntilReady(t, handle);
                readyHandles.add(handle);
            }
            return t;
        }
    }

    /**
     * After {@code createTable} returns the table is registered in ZK but bucket leadership can
     * take a beat to propagate through the Fluss client's metadata cache. We poll with a sentinel
     * lookup until it either succeeds or returns an empty result (both mean "bucket reachable"),
     * retrying on {@code NotLeaderOrFollowerException} and similar transient client errors.
     */
    private void waitUntilReady(Table t, Handle handle) throws Exception {
        Lookuper lookuper = t.newLookup().createLookuper();
        InternalRow key = readinessKey(handle);
        long deadlineMillis =
                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);
        Exception last = null;
        while (System.currentTimeMillis() < deadlineMillis) {
            try {
                lookuper.lookup(key).get(2, TimeUnit.SECONDS);
                return;
            } catch (ExecutionException ee) {
                last = ee;
            } catch (Exception other) {
                last = other;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
        throw new CatalogException(
                CatalogException.Kind.INTERNAL,
                "Catalog table " + handle + " did not become ready within " + TIMEOUT_SECONDS + "s",
                last);
    }

    /** Sentinel PK per handle — never written, only used to probe readiness. */
    private InternalRow readinessKey(Handle handle) {
        switch (handle) {
            case ID_RESERVATIONS:
                {
                    GenericRow r = new GenericRow(1);
                    r.setField(0, 0);
                    return r;
                }
            case CLIENT_QUOTAS:
                {
                    GenericRow r = new GenericRow(3);
                    r.setField(0, BinaryString.fromString("__readiness__"));
                    r.setField(1, BinaryString.fromString(""));
                    r.setField(2, BinaryString.fromString(""));
                    return r;
                }
            case SCHEMA_REFERENCES:
                {
                    GenericRow r = new GenericRow(2);
                    r.setField(0, BinaryString.fromString("__readiness__"));
                    r.setField(1, BinaryString.fromString(""));
                    return r;
                }
            case KAFKA_PRODUCER_IDS:
                {
                    GenericRow r = new GenericRow(1);
                    r.setField(0, 0L);
                    return r;
                }
            default:
                {
                    GenericRow r = new GenericRow(1);
                    r.setField(0, BinaryString.fromString("__readiness__"));
                    return r;
                }
        }
    }

    /**
     * Always attempt to (re-)create the {@code _catalog} database. {@code ignoreIfExists} makes the
     * no-op case cheap, and the always-attempt path handles external cleanup (tests dropping
     * databases between runs, administrative drops) without us needing stateful recovery.
     */
    private void ensureDatabase() throws Exception {
        Admin admin = connection().getAdmin();
        try {
            admin.createDatabase(SystemTables.DATABASE, DatabaseDescriptor.EMPTY, true)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (!(cause instanceof DatabaseAlreadyExistException)) {
                throw unwrap(ee);
            }
        }
    }

    private SystemTables.Table tableFor(Handle handle) {
        switch (handle) {
            case NAMESPACES:
                return SystemTables.NAMESPACES;
            case TABLES:
                return SystemTables.TABLES;
            case SCHEMAS:
                return SystemTables.SCHEMAS;
            case PRINCIPALS:
                return SystemTables.PRINCIPALS;
            case GRANTS:
                return SystemTables.GRANTS;
            case KAFKA_BINDINGS:
                return SystemTables.KAFKA_BINDINGS;
            case CLIENT_QUOTAS:
                return SystemTables.CLIENT_QUOTAS;
            case ID_RESERVATIONS:
                return SystemTables.ID_RESERVATIONS;
            case SR_CONFIG:
                return SystemTables.SR_CONFIG;
            case SCHEMA_REFERENCES:
                return SystemTables.SCHEMA_REFERENCES;
            case KAFKA_TXN_STATE:
                return SystemTables.KAFKA_TXN_STATE;
            case KAFKA_PRODUCER_IDS:
                return SystemTables.KAFKA_PRODUCER_IDS;
            default:
                throw new IllegalStateException("Unknown handle: " + handle);
        }
    }

    private void upsert(Handle handle, InternalRow row) throws Exception {
        UpsertWriter writer = table(handle).newUpsert().createWriter();
        try {
            writer.upsert(row).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
        writer.flush();
    }

    private void delete(Handle handle, InternalRow fullRow) throws Exception {
        UpsertWriter writer = table(handle).newUpsert().createWriter();
        try {
            writer.delete(fullRow).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
        writer.flush();
    }

    /**
     * Build a full-width placeholder row for a delete against {@code handle}. Fluss's {@link
     * UpsertWriter#delete} requires every column to be present even though only the PK is
     * semantically consulted; non-PK columns can be {@code null} / zero.
     */
    private InternalRow deletePlaceholder(Handle handle, String pkString) {
        switch (handle) {
            case NAMESPACES:
                {
                    GenericRow row = new GenericRow(5);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, null);
                    row.setField(2, BinaryString.fromString(""));
                    row.setField(3, null);
                    row.setField(4, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case TABLES:
                {
                    GenericRow row = new GenericRow(8);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, BinaryString.fromString(""));
                    row.setField(2, BinaryString.fromString(""));
                    row.setField(3, BinaryString.fromString(""));
                    row.setField(4, BinaryString.fromString(""));
                    row.setField(5, null);
                    row.setField(6, null);
                    row.setField(7, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case SCHEMAS:
                {
                    GenericRow row = new GenericRow(8);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, BinaryString.fromString(""));
                    row.setField(2, 0);
                    row.setField(3, BinaryString.fromString(""));
                    row.setField(4, BinaryString.fromString(""));
                    row.setField(5, 0);
                    row.setField(6, null);
                    row.setField(7, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case KAFKA_BINDINGS:
                {
                    GenericRow row = new GenericRow(4);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, BinaryString.fromString(""));
                    row.setField(2, BinaryString.fromString(""));
                    row.setField(3, BinaryString.fromString(""));
                    return row;
                }
            case PRINCIPALS:
                {
                    GenericRow row = new GenericRow(4);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, BinaryString.fromString(""));
                    row.setField(2, BinaryString.fromString(""));
                    row.setField(3, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case GRANTS:
                {
                    GenericRow row = new GenericRow(7);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, BinaryString.fromString(""));
                    row.setField(2, BinaryString.fromString(""));
                    row.setField(3, BinaryString.fromString(""));
                    row.setField(4, BinaryString.fromString(""));
                    row.setField(5, null);
                    row.setField(6, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case ID_RESERVATIONS:
                throw new IllegalArgumentException(
                        "ID_RESERVATIONS delete uses a numeric PK; use deletePlaceholderInt()");
            case CLIENT_QUOTAS:
                throw new IllegalArgumentException(
                        "CLIENT_QUOTAS delete uses a composite PK; use deleteClientQuotaRow()");
            case KAFKA_TXN_STATE:
                {
                    GenericRow row = new GenericRow(8);
                    row.setField(0, BinaryString.fromString(pkString));
                    row.setField(1, 0L);
                    row.setField(2, (short) 0);
                    row.setField(3, BinaryString.fromString(""));
                    row.setField(4, null);
                    row.setField(5, 0);
                    row.setField(6, null);
                    row.setField(7, TimestampLtz.fromEpochMillis(0));
                    return row;
                }
            case KAFKA_PRODUCER_IDS:
                throw new IllegalArgumentException(
                        "KAFKA_PRODUCER_IDS delete uses a numeric PK; use deletePlaceholderLong()");
            default:
                throw new IllegalStateException("Unknown handle: " + handle);
        }
    }

    /** Delete a single client-quota row by its composite PK. */
    private void deleteClientQuotaRow(String entityType, String entityName, String quotaKey)
            throws Exception {
        GenericRow row = new GenericRow(5);
        row.setField(0, BinaryString.fromString(entityType));
        row.setField(1, BinaryString.fromString(entityName));
        row.setField(2, BinaryString.fromString(quotaKey));
        row.setField(3, 0.0);
        row.setField(4, TimestampLtz.fromEpochMillis(0));
        delete(Handle.CLIENT_QUOTAS, row);
    }

    private @Nullable InternalRow awaitLookup(Lookuper lookuper, InternalRow key) throws Exception {
        try {
            return lookuper.lookup(key).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getSingletonRow();
        } catch (ExecutionException ee) {
            throw unwrap(ee);
        }
    }

    private void scan(Handle handle, java.util.function.Consumer<InternalRow> consumer)
            throws Exception {
        // BatchScanner requires a pre-configured limit. 100_000 is far above the row count we
        // expect in any single catalog entity table for Phase C.1 (namespaces, bindings, etc.);
        // Phase C.2+ will replace full-table scans with either a LogScanner tail or a proper
        // secondary-index entity table.
        try (org.apache.fluss.client.table.scanner.batch.BatchScanner scanner =
                table(handle).newScan().limit(100_000).createBatchScanner()) {
            CloseableIterator<InternalRow> batch;
            while ((batch = scanner.pollBatch(SCAN_POLL_TIMEOUT)) != null) {
                try {
                    while (batch.hasNext()) {
                        consumer.accept(batch.next());
                    }
                } finally {
                    batch.close();
                }
            }
        }
    }

    // ---------- entity-specific scans ----------

    private Optional<NamespaceEntity> findNamespaceByFqn(String fqn) throws Exception {
        if (fqn == null || fqn.isEmpty()) {
            return Optional.empty();
        }
        String[] segments = fqn.split("\\.");
        Map<String, NamespaceEntity> all = new java.util.HashMap<>();
        List<NamespaceEntity> allList = scanAllNamespaces();
        for (NamespaceEntity ns : allList) {
            all.put(ns.namespaceId(), ns);
        }
        NamespaceEntity match = null;
        for (String segment : segments) {
            String parentId = match == null ? null : match.namespaceId();
            NamespaceEntity next = null;
            for (NamespaceEntity candidate : allList) {
                if (candidate.name().equals(segment)
                        && java.util.Objects.equals(candidate.parentId(), parentId)) {
                    next = candidate;
                    break;
                }
            }
            if (next == null) {
                return Optional.empty();
            }
            match = next;
        }
        return Optional.ofNullable(match);
    }

    private List<NamespaceEntity> scanAllNamespaces() throws Exception {
        List<NamespaceEntity> out = new ArrayList<>();
        scan(
                Handle.NAMESPACES,
                row ->
                        out.add(
                                new NamespaceEntity(
                                        row.getString(0).toString(),
                                        row.isNullAt(1) ? null : row.getString(1).toString(),
                                        row.getString(2).toString(),
                                        row.isNullAt(3) ? null : row.getString(3).toString(),
                                        row.getTimestampLtz(4, 3).toEpochMicros() / 1000)));
        return out;
    }

    private List<CatalogTableEntity> listTablesByNamespaceId(String namespaceId) throws Exception {
        List<CatalogTableEntity> out = new ArrayList<>();
        scan(
                Handle.TABLES,
                row -> {
                    String rowNs = row.getString(1).toString();
                    if (!namespaceId.equals(rowNs)) {
                        return;
                    }
                    out.add(decodeTable(row));
                });
        return out;
    }

    private static CatalogTableEntity decodeTable(InternalRow row) {
        return new CatalogTableEntity(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getString(2).toString(),
                row.getString(3).toString(),
                row.getString(4).toString(),
                row.isNullAt(5) ? null : row.getString(5).toString(),
                row.isNullAt(6) ? null : row.getString(6).toString(),
                row.getTimestampLtz(7, 3).toEpochMicros() / 1000);
    }

    private void upsertTableRow(CatalogTableEntity entity) throws Exception {
        GenericRow row = new GenericRow(8);
        row.setField(0, BinaryString.fromString(entity.tableId()));
        row.setField(1, BinaryString.fromString(entity.namespaceId()));
        row.setField(2, BinaryString.fromString(entity.name()));
        row.setField(3, BinaryString.fromString(entity.format()));
        row.setField(4, BinaryString.fromString(entity.backingRef()));
        row.setField(
                5,
                entity.currentSchemaId() == null
                        ? null
                        : BinaryString.fromString(entity.currentSchemaId()));
        row.setField(
                6, entity.createdBy() == null ? null : BinaryString.fromString(entity.createdBy()));
        row.setField(7, TimestampLtz.fromEpochMillis(entity.createdAtMillis()));
        upsert(Handle.TABLES, row);
    }

    private void dropTableById(CatalogTableEntity table) throws Exception {
        delete(Handle.TABLES, deletePlaceholder(Handle.TABLES, table.tableId()));
        for (SchemaVersionEntity s : listSchemaVersionsByTableId(table.tableId())) {
            delete(Handle.SCHEMAS, deletePlaceholder(Handle.SCHEMAS, s.schemaId()));
        }
    }

    private List<SchemaVersionEntity> listSchemaVersionsByTableId(String tableId) throws Exception {
        List<SchemaVersionEntity> out = new ArrayList<>();
        scan(
                Handle.SCHEMAS,
                row -> {
                    String rowTableId = row.getString(1).toString();
                    if (!tableId.equals(rowTableId)) {
                        return;
                    }
                    out.add(decodeSchema(row));
                });
        out.sort((a, b) -> Integer.compare(a.version(), b.version()));
        return out;
    }

    @Override
    public Optional<SchemaVersionEntity> getSchemaBySchemaId(String schemaId) throws Exception {
        Lookuper lookuper = table(Handle.SCHEMAS).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, BinaryString.fromString(schemaId));
        InternalRow found = awaitLookup(lookuper, key);
        return Optional.ofNullable(found).map(FlussCatalogService::decodeSchema);
    }

    @Override
    public Optional<CatalogTableEntity> getTableById(String tableId) throws Exception {
        Lookuper lookuper = table(Handle.TABLES).newLookup().createLookuper();
        GenericRow key = new GenericRow(1);
        key.setField(0, BinaryString.fromString(tableId));
        InternalRow found = awaitLookup(lookuper, key);
        return Optional.ofNullable(found).map(FlussCatalogService::decodeTable);
    }

    private static SchemaVersionEntity decodeSchema(InternalRow row) {
        return new SchemaVersionEntity(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getInt(2),
                row.getString(3).toString(),
                row.getString(4).toString(),
                row.getInt(5),
                row.isNullAt(6) ? null : row.getString(6).toString(),
                row.getTimestampLtz(7, 3).toEpochMicros() / 1000);
    }

    // ---------- Confluent id allocator (no-CAS variant) ----------

    /**
     * Deterministic base id = {@code hash(tableId, schemaVersion, format) & 0x7fffffff}. Because
     * the id is deterministic, idempotent re-submission of an identical tuple always produces the
     * same id. Hash collisions across distinct tuples are resolved by linear probing; the probe
     * loop races aren't strictly atomic under concurrent registration of different tuples, but the
     * 31-bit id space makes real-world collisions vanishingly rare and the penalty (losing to a
     * concurrent writer) is at most one wasted probe slot.
     */
    private int allocateConfluentId(
            String tableId, int schemaVersion, String format, String schemaId) throws Exception {
        int base = stableHash(tableId, schemaVersion, format) & 0x7fffffff;
        Lookuper lookuper = table(Handle.ID_RESERVATIONS).newLookup().createLookuper();
        for (int probe = 0; probe < CONFLUENT_ID_PROBE_LIMIT; probe++) {
            int id = (base + probe) & 0x7fffffff;
            GenericRow key = new GenericRow(1);
            key.setField(0, id);
            InternalRow existing = awaitLookup(lookuper, key);
            if (existing == null) {
                GenericRow row = new GenericRow(4);
                row.setField(0, id);
                row.setField(1, BinaryString.fromString(schemaId));
                row.setField(2, BinaryString.fromString(format));
                row.setField(3, TimestampLtz.fromEpochMillis(System.currentTimeMillis()));
                upsert(Handle.ID_RESERVATIONS, row);
                return id;
            }
            String occupantSchemaId = existing.getString(1).toString();
            String occupantFormat = existing.getString(2).toString();
            Optional<SchemaVersionEntity> occupant = getSchemaBySchemaId(occupantSchemaId);
            if (!occupant.isPresent()) {
                // Dangling reservation — reclaim.
                GenericRow row = new GenericRow(4);
                row.setField(0, id);
                row.setField(1, BinaryString.fromString(schemaId));
                row.setField(2, BinaryString.fromString(format));
                row.setField(3, TimestampLtz.fromEpochMillis(System.currentTimeMillis()));
                upsert(Handle.ID_RESERVATIONS, row);
                return id;
            }
            SchemaVersionEntity entity = occupant.get();
            if (entity.tableId().equals(tableId)
                    && entity.version() == schemaVersion
                    && entity.format().equals(format)
                    && occupantFormat.equals(format)) {
                return id; // idempotent — same tuple hashed here and we won
            }
            // distinct tuple landed here; probe next
        }
        throw new CatalogException(
                CatalogException.Kind.INTERNAL,
                "Could not reserve a Confluent id for ("
                        + tableId
                        + ", v"
                        + schemaVersion
                        + ", "
                        + format
                        + ") after "
                        + CONFLUENT_ID_PROBE_LIMIT
                        + " probes");
    }

    private static int stableHash(String tableId, int schemaVersion, String format) {
        int h = tableId.hashCode();
        h = h * 31 + schemaVersion;
        h = h * 31 + format.hashCode();
        return h;
    }

    // ---------- Errors ----------

    private static void requireNonEmpty(String field, String value) {
        if (value == null || value.isEmpty()) {
            throw new CatalogException(CatalogException.Kind.INVALID_INPUT, field + " is required");
        }
    }

    private static CatalogException internal(String operation, Throwable t) {
        return new CatalogException(
                CatalogException.Kind.INTERNAL, operation + " failed: " + t.getMessage(), t);
    }

    private static Exception unwrap(ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return ee;
    }
}
