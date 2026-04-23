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
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.NamespaceEntity;
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

    private enum Handle {
        NAMESPACES,
        TABLES,
        SCHEMAS,
        PRINCIPALS,
        GRANTS,
        KAFKA_BINDINGS,
        ID_RESERVATIONS
    }

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

    // =================================================================
    // RBAC (Phase C.2 — no-op stubs)
    // =================================================================

    @Override
    public void grant(String principal, String entityFqn, String privilege) {
        throw unsupported("grant");
    }

    @Override
    public void revoke(String principal, String entityFqn, String privilege) {
        throw unsupported("revoke");
    }

    @Override
    public List<String> listGrants(String principal) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkPrivilege(String principal, String entityFqn, String privilege) {
        // Allow-all until Phase C.2 wires enforcement.
        return true;
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
                admin.createTable(def.tablePath(), def.descriptor(), true)
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                freshlyCreated = true;
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (!(cause instanceof TableAlreadyExistException)) {
                    throw unwrap(ee);
                }
            }
            Table t = connection().getTable(def.tablePath());
            if (freshlyCreated) {
                waitUntilReady(t, handle);
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
            case ID_RESERVATIONS:
                return SystemTables.ID_RESERVATIONS;
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
            default:
                throw new IllegalStateException("Unknown handle: " + handle);
        }
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

    private static CatalogException unsupported(String operation) {
        return new CatalogException(
                CatalogException.Kind.UNSUPPORTED, operation + " is not yet wired (Phase C.2)");
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
