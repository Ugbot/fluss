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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.exception.InvalidReplicationFactorException;
import org.apache.fluss.kafka.fetch.KafkaTopicRoute;
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.kafka.sr.SchemaRegistryException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;

/**
 * Phase T.3 — ALTER-on-register orchestrator. Sits between {@code SchemaRegistryService.register}
 * and the underlying Fluss data table, and either:
 *
 * <ul>
 *   <li>flips a {@code KAFKA_PASSTHROUGH}-shaped Kafka topic into {@code KAFKA_TYPED_<FORMAT>} on
 *       first registration (drop-and-recreate on empty — Option C of design 0015 §3), or
 *   <li>extends a {@code KAFKA_TYPED_<FORMAT>}-shaped topic with newly-appended nullable columns on
 *       subsequent registrations (additive evolve — design 0015 §5).
 * </ul>
 *
 * <p>Kafka SR schema ids are stored in {@code _catalog.__schemas__} keyed by the schema's UUID and
 * are not touched by either path: the table reshape changes the data table's column layout, but the
 * schema row's id column is append-only. T.2's decoder cache is keyed on {@code (tableId,
 * schemaId)}, which survives a reshape (design 0015 §9).
 *
 * <p>This evolver is a no-op when {@code kafka.typed-tables.enabled = false} (the T.2-introduced
 * flag) — typed routing is gated globally on that flag, so flipping a catalog row to a typed format
 * with the flag off would just produce passthrough fetches against a typed-shaped table. The flag
 * check is the policy seam; production rollouts must turn the flag on AND register schemas through
 * this path before the typed Produce/Fetch hot path engages.
 *
 * <p>TopicNameStrategy only — RecordNameStrategy and TopicRecordNameStrategy are out of scope per
 * design 0015 §6 and the §13 deferral list.
 *
 * <p>Thread-safe: every method is server-side coordinator work and is invoked under the SR HTTP
 * handler's request scope; no shared mutable state.
 */
@Internal
public final class TypedTableEvolver {

    private static final Logger LOG = LoggerFactory.getLogger(TypedTableEvolver.class);

    private final MetadataManager metadataManager;
    private final CatalogService catalog;
    private final String kafkaDatabase;
    private final Supplier<Admin> adminSupplier;
    @Nullable private final ServerMetadataCache metadataCache;
    private final boolean enabled;

    public TypedTableEvolver(
            MetadataManager metadataManager,
            CatalogService catalog,
            String kafkaDatabase,
            Supplier<Admin> adminSupplier,
            @Nullable ServerMetadataCache metadataCache,
            boolean enabled) {
        this.metadataManager = metadataManager;
        this.catalog = catalog;
        this.kafkaDatabase = kafkaDatabase;
        this.adminSupplier = adminSupplier;
        this.metadataCache = metadataCache;
        this.enabled = enabled;
    }

    /** Whether the typed-tables feature flag is on. */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Apply the typed-table reshape (first-register) or additive extension (evolve) for {@code
     * subject}'s topic. Called from {@code SchemaRegistryService.register} after the SR's own
     * subject / format / compatibility validation has succeeded but BEFORE {@code
     * catalog.registerSchema(...)} mints a new Kafka SR schema id.
     *
     * <p>Failure modes:
     *
     * <ul>
     *   <li>Empty-topic guard fires on first register → 409 with a body pointing to manual drop.
     *   <li>Diff rejects on evolve (rename / type change / reorder / non-null add / column drop) →
     *       409 with a structured per-field message.
     *   <li>Drop / create / alter underlying Fluss primitives fail → wrapped {@link
     *       SchemaRegistryException} of kind {@code INTERNAL}, leaving the catalog row consistent
     *       with the physical table shape (design 0015 §8).
     * </ul>
     *
     * <p>This method is a no-op (returns immediately) when {@link #enabled()} is {@code false}: the
     * subject still gets registered through the normal SR path, but the Fluss table stays in {@code
     * KAFKA_PASSTHROUGH} shape and T.2's typed Produce/Fetch hot path does not engage. Flipping the
     * flag on requires a server restart (T.2 doc §3); operators who want existing topics to reshape
     * after enabling the flag must re-register at least one schema per topic.
     */
    public void onRegister(String topic, String formatId, String schemaText) {
        if (!enabled) {
            return;
        }
        if (topic == null || topic.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "topic is required");
        }
        if (formatId == null || formatId.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "formatId is required");
        }
        String typedFormat = typedFormatFor(formatId);

        FormatTranslator translator = FormatRegistry.instance().translator(formatId);
        if (translator == null) {
            // Caller already canonicalised; defensive.
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNSUPPORTED,
                    "no translator registered for format '" + formatId + "'");
        }

        RowType proposedUserRowType;
        try {
            proposedUserRowType = translator.translateTo(schemaText);
        } catch (SchemaTranslationException ste) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY, ste.getMessage(), ste);
        }
        validateProposedUserShape(proposedUserRowType);

        TablePath path = new TablePath(kafkaDatabase, topic);
        CatalogTableEntity entity;
        try {
            entity =
                    catalog.getTable(kafkaDatabase, topic)
                            .orElseThrow(
                                    () ->
                                            new SchemaRegistryException(
                                                    SchemaRegistryException.Kind.NOT_FOUND,
                                                    "catalog row missing for topic '"
                                                            + topic
                                                            + "'"));
        } catch (SchemaRegistryException sre) {
            throw sre;
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "failed to look up catalog row for topic '" + topic + "': " + e.getMessage(),
                    e);
        }

        String currentFormat = entity.format();
        if (CatalogTableEntity.FORMAT_KAFKA_PASSTHROUGH.equals(currentFormat)) {
            firstRegister(path, topic, formatId, typedFormat, proposedUserRowType);
            return;
        }
        if (isTypedFormat(currentFormat)) {
            String currentTypedFormat = currentFormat;
            String requestedTyped = typedFormat;
            if (!currentTypedFormat.equals(requestedTyped)) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.CONFLICT,
                        "cannot re-type topic '"
                                + topic
                                + "' from "
                                + currentTypedFormat
                                + " to "
                                + requestedTyped);
            }
            evolveTyped(path, topic, proposedUserRowType);
            return;
        }
        // Unknown format — leave alone. Defensive against future formats added between releases.
        LOG.warn(
                "TypedTableEvolver: catalog row for topic '{}' has unknown format '{}'; skipping",
                topic,
                currentFormat);
    }

    // -------------------------------------------------------------------------
    //  First registration: drop-and-recreate on empty + catalog format flip.
    // -------------------------------------------------------------------------

    private void firstRegister(
            TablePath path,
            String topic,
            String formatId,
            String typedFormat,
            RowType proposedUserRowType) {
        TableInfo current;
        try {
            current = metadataManager.getTable(path);
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.NOT_FOUND,
                    "Kafka data table '" + path + "' does not exist: " + e.getMessage(),
                    e);
        }
        // §4.7: empty-topic guard. Refuse the reshape if any record has been produced.
        EmptinessProbeResult emptyResult = probeEmpty(path, current);
        if (emptyResult.nonEmpty) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.CONFLICT,
                    "typed-tables registration requires the topic to be empty; topic '"
                            + topic
                            + "' already has records (bucket "
                            + emptyResult.firstNonEmptyBucket
                            + " end="
                            + emptyResult.firstNonEmptyEnd
                            + " start="
                            + emptyResult.firstNonEmptyStart
                            + "); produce records only after first schema registration, "
                            + "or drop and recreate the topic to register a typed schema.");
        }

        boolean compacted = current.hasPrimaryKey();
        TableDescriptor newDescriptor =
                buildTypedDescriptor(current, proposedUserRowType, compacted);

        try {
            metadataManager.dropTable(path, /* ignoreIfNotExists */ false);
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "drop-table failed for '" + path + "' during typed reshape: " + e.getMessage(),
                    e);
        }
        try {
            // Allow a brief retry window because dropTable schedules deletion through the
            // coordinator event loop; createTable is rejected with ALREADY_EXISTS until the
            // delete propagates. Bounded retries keep the SR HTTP handler responsive while
            // covering the common case.
            createTableWithRetries(path, newDescriptor);
        } catch (SchemaRegistryException sre) {
            throw sre;
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "create-table failed for '"
                            + path
                            + "' during typed reshape: "
                            + e.getMessage(),
                    e);
        }
        try {
            catalog.updateTableFormat(kafkaDatabase, topic, typedFormat);
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "catalog format flip failed for '"
                            + path
                            + "' (table is now typed-shape but catalog still reads "
                            + "KAFKA_PASSTHROUGH; retry the registration to converge): "
                            + e.getMessage(),
                    e);
        }
        LOG.info(
                "TypedTableEvolver: reshaped '{}' KAFKA_PASSTHROUGH -> {} ({} user columns; format={})",
                path,
                typedFormat,
                proposedUserRowType.getFieldCount(),
                formatId);
    }

    private void createTableWithRetries(TablePath path, TableDescriptor descriptor)
            throws Exception {
        // dropTable schedules ZK deletion through the coordinator's event loop; the create-side
        // sees ALREADY_EXISTS until that's drained. Bounded backoff: 8 attempts × 50ms = ~400ms
        // worst case, well below the SR HTTP handler's request timeout.
        //
        // Compute bucket assignment locally (same as CoordinatorService.createTable) so
        // metadataManager.createTable writes the assignment ZNode before the table ZNode.
        // Without assignment, TableChangeWatcher silently drops the CreateTableEvent and tablets
        // never receive UpdateMetadata, leaving the reshaped table in LEADER_NOT_AVAILABLE.
        TableAssignment assignment = computeAssignment(descriptor);
        Exception last = null;
        for (int attempt = 0; attempt < 8; attempt++) {
            try {
                metadataManager.createTable(
                        path, descriptor, assignment, /* ignoreIfExists */ false);
                return;
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg == null) {
                    msg = "";
                }
                if (!msg.contains("already exists") && !msg.contains("ALREADY_EXISTS")) {
                    throw e;
                }
                last = e;
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
        throw last != null ? last : new IllegalStateException("createTable failed after retries");
    }

    @Nullable
    private TableAssignment computeAssignment(TableDescriptor descriptor) {
        if (metadataCache == null) {
            return null;
        }
        TabletServerInfo[] servers = metadataCache.getLiveServers();
        if (servers.length == 0) {
            return null;
        }
        int buckets =
                descriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElse(1);
        int replication = descriptor.getReplicationFactor();
        try {
            return generateAssignment(buckets, replication, servers);
        } catch (InvalidReplicationFactorException e) {
            LOG.warn(
                    "TypedTableEvolver: cannot compute assignment (buckets={}, rf={}, servers={}): {}; "
                            + "falling back to null assignment",
                    buckets,
                    replication,
                    servers.length,
                    e.getMessage());
            return null;
        }
    }

    // -------------------------------------------------------------------------
    //  Subsequent registrations: additive evolution (AddColumn LAST nullable).
    // -------------------------------------------------------------------------

    private void evolveTyped(TablePath path, String topic, RowType proposedUserRowType) {
        TableInfo current;
        try {
            current = metadataManager.getTable(path);
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.NOT_FOUND,
                    "Kafka data table '" + path + "' missing during evolve: " + e.getMessage(),
                    e);
        }
        RowType currentUserRowType = stripReserved(current.getRowType(), current.hasPrimaryKey());
        AdditiveDelta delta = computeAdditiveDelta(currentUserRowType, proposedUserRowType);
        if (delta.reject != null) {
            throw new SchemaRegistryException(SchemaRegistryException.Kind.CONFLICT, delta.reject);
        }
        if (delta.changes.isEmpty()) {
            // Same shape — typed re-register of identical columns; nothing to alter.
            return;
        }
        try {
            metadataManager.alterTableSchema(
                    path,
                    delta.changes,
                    /* ignoreIfNotExists */ false,
                    org.apache.fluss.security.acl.FlussPrincipal.ANONYMOUS);
        } catch (Exception e) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "alterTable failed for '" + path + "': " + e.getMessage(),
                    e);
        }
        LOG.info(
                "TypedTableEvolver: extended '{}' typed shape with {} new column(s)",
                path,
                delta.changes.size());
    }

    /**
     * Diff {@code currentUserRowType} vs {@code proposedUserRowType} per design 0015 §5.3. Returns
     * an {@link AdditiveDelta} describing either the {@link TableChange#addColumn} list to apply,
     * or a single rejection message string. Visible-for-tests so unit suites can exercise it
     * without running the cluster.
     */
    @VisibleForTesting
    public static AdditiveDelta computeAdditiveDelta(
            RowType currentUserRowType, RowType proposedUserRowType) {
        List<DataField> current = currentUserRowType.getFields();
        List<DataField> proposed = proposedUserRowType.getFields();

        // Build name index sets so we can distinguish removals from renames.
        Set<String> currentNames = new HashSet<>();
        for (DataField f : current) {
            currentNames.add(f.getName());
        }
        Set<String> proposedNames = new HashSet<>();
        for (DataField f : proposed) {
            proposedNames.add(f.getName());
        }

        if (proposed.size() < current.size()) {
            // Surface this as "column removed: <first-missing>" so operators see a useful name.
            for (DataField f : current) {
                if (!proposedNames.contains(f.getName())) {
                    return AdditiveDelta.reject("column removed: " + f.getName());
                }
            }
            return AdditiveDelta.reject(
                    "column count regressed: " + current.size() + " -> " + proposed.size());
        }

        // Reorder detection — same set of names (within the current prefix's range) but in a
        // different order at the same positions. Distinguished from rename and from add at the
        // tail. We only trigger this when every current name appears somewhere in the matching
        // prefix of proposed but at a different index.
        for (int i = 0; i < current.size(); i++) {
            DataField a = current.get(i);
            DataField b = proposed.get(i);
            if (a.getName().equals(b.getName())) {
                continue;
            }
            // Names differ at this position. Three sub-cases:
            //   1) Both names exist in the other's set and the types match → reorder.
            //   2) The current name is absent from proposed → either a rename (if proposed[i]'s
            //      name is new) or a removal (if proposed[i]'s name is somewhere else in
            //      proposed). We surface both as "rename" because in practice operator-supplied
            //      renames look identical to drop+add at this layer.
            //   3) The current name still exists but at a different proposed index → reorder.
            if (currentNames.contains(b.getName()) && proposedNames.contains(a.getName())) {
                return AdditiveDelta.reject(
                        "column reorder not supported: " + a.getName() + " <-> " + b.getName());
            }
            // a.name is not in proposed → either a rename or a removal. Distinguish by whether
            // b.name is brand-new (rename) or already-present (then a.name was simply removed).
            if (!proposedNames.contains(a.getName())) {
                if (!currentNames.contains(b.getName())) {
                    // Brand-new name at this position; pre-existing name disappeared. Most
                    // operator-friendly framing: rename.
                    return AdditiveDelta.reject(
                            "column rename not supported on typed tables: "
                                    + a.getName()
                                    + " -> "
                                    + b.getName());
                }
                return AdditiveDelta.reject("column removed: " + a.getName());
            }
            // a.name is in proposed (just at a different position) — that's a reorder.
            return AdditiveDelta.reject(
                    "column reorder not supported: " + a.getName() + " <-> " + b.getName());
        }

        // After matching every position by name, every current name must still be present.
        for (DataField f : current) {
            if (!proposedNames.contains(f.getName())) {
                return AdditiveDelta.reject("column removed: " + f.getName());
            }
        }

        // Type validation on matched positions.
        for (int i = 0; i < current.size(); i++) {
            DataField a = current.get(i);
            DataField b = proposed.get(i);
            if (!a.getType().copy(true).equals(b.getType().copy(true))) {
                return AdditiveDelta.reject(
                        "column type changed: "
                                + a.getName()
                                + ": "
                                + a.getType()
                                + " -> "
                                + b.getType());
            }
        }
        // Validate appended columns: each MUST be nullable per §5.3 (and SchemaUpdate's invariant).
        // Use AFTER(<predecessor>) so each new column is inserted before the reserved suffix
        // (event_time, headers) rather than at the absolute table tail.
        List<TableChange> changes = new ArrayList<>();
        String lastColName = current.isEmpty() ? null : current.get(current.size() - 1).getName();
        for (int i = current.size(); i < proposed.size(); i++) {
            DataField appended = proposed.get(i);
            if (!appended.getType().isNullable()) {
                return AdditiveDelta.reject("new column must be nullable: " + appended.getName());
            }
            TableChange.ColumnPosition pos =
                    lastColName != null
                            ? TableChange.ColumnPosition.after(lastColName)
                            : TableChange.ColumnPosition.last();
            changes.add(
                    TableChange.addColumn(
                            appended.getName(),
                            appended.getType(),
                            appended.getDescription().orElse(null),
                            pos));
            lastColName = appended.getName();
        }
        return AdditiveDelta.accept(changes);
    }

    // -------------------------------------------------------------------------
    //  Helpers.
    // -------------------------------------------------------------------------

    /** Strip the reserved record_key prefix and (event_time, headers) suffix from a typed row. */
    @VisibleForTesting
    static RowType stripReserved(RowType full, boolean compacted) {
        // KAFKA_TYPED layout: record_key, f1..fN, event_time, headers.
        // (compacted topics still have record_key as the leading column; the only difference
        // is its non-null marker + primary-key constraint, neither of which appears in the
        // RowType representation directly.)
        List<DataField> all = full.getFields();
        if (all.size() < 3) {
            // Unexpected — return as-is so the caller's diff path produces a meaningful error.
            return full;
        }
        // Sanity-check the bracketing names. Fall through to identity diff if unexpected.
        if (!KafkaDataTable.COL_RECORD_KEY.equals(all.get(0).getName())
                || !KafkaDataTable.COL_HEADERS.equals(all.get(all.size() - 1).getName())
                || !KafkaDataTable.COL_EVENT_TIME.equals(all.get(all.size() - 2).getName())) {
            LOG.warn(
                    "TypedTableEvolver.stripReserved: unexpected column names {} (compacted={}); "
                            + "diff will fall back to identity",
                    full.getFieldNames(),
                    compacted);
            return full;
        }
        return new RowType(full.isNullable(), new ArrayList<>(all.subList(1, all.size() - 2)));
    }

    /** Validate the user-facing portion of a translator output against design 0015 §4.6. */
    private static void validateProposedUserShape(RowType proposed) {
        if (proposed == null) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                    "translator produced a null RowType");
        }
        if (proposed.getFieldCount() == 0) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                    "translator produced an empty RowType (no fields)");
        }
        Set<String> seen = new HashSet<>();
        Set<String> reserved =
                new HashSet<>(
                        Arrays.asList(
                                KafkaDataTable.COL_RECORD_KEY,
                                KafkaDataTable.COL_PAYLOAD,
                                KafkaDataTable.COL_EVENT_TIME,
                                KafkaDataTable.COL_HEADERS));
        for (DataField f : proposed.getFields()) {
            String n = f.getName();
            if (n == null || n.isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                        "translator produced an unnamed field");
            }
            if (!seen.add(n)) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                        "translator produced a duplicate field name: " + n);
            }
            if (reserved.contains(n)) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                        "reserved column name '" + n + "' cannot be used in a typed schema");
            }
        }
    }

    /**
     * Build a {@link TableDescriptor} for the typed shape. Preserves bucket count / replication and
     * carries the pre-existing custom properties / properties forward so Kafka admin metadata
     * (timestamp type, compression, topic id, …) still resolves after the reshape. Forces every
     * user-region column to nullable so subsequent additive evolution is consistent with {@code
     * SchemaUpdate}'s nullable invariant (design 0015 §4.8).
     */
    private static TableDescriptor buildTypedDescriptor(
            TableInfo current, RowType proposedUserRowType, boolean compacted) {
        Schema.Builder b = Schema.newBuilder();
        DataType bytesNonNull = DataTypes.BYTES().copy(false);
        if (compacted) {
            b.column(KafkaDataTable.COL_RECORD_KEY, bytesNonNull);
        } else {
            b.column(KafkaDataTable.COL_RECORD_KEY, DataTypes.BYTES());
        }
        for (DataField f : proposedUserRowType.getFields()) {
            DataType nullable = f.getType().copy(true);
            b.column(f.getName(), nullable);
            f.getDescription().ifPresent(b::withComment);
        }
        b.column(KafkaDataTable.COL_EVENT_TIME, DataTypes.TIMESTAMP_LTZ(3).copy(false))
                .column(
                        KafkaDataTable.COL_HEADERS,
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                KafkaDataTable.HEADER_FIELD_NAME,
                                                DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                KafkaDataTable.HEADER_FIELD_VALUE,
                                                DataTypes.BYTES()))));
        if (compacted) {
            b.primaryKey(KafkaDataTable.COL_RECORD_KEY);
        }

        TableDescriptor.Builder out =
                TableDescriptor.builder()
                        .schema(b.build())
                        .distributedBy(current.getNumBuckets())
                        .properties(current.getProperties().toMap())
                        .customProperties(current.getCustomProperties().toMap());
        current.getComment().ifPresent(out::comment);
        return out.build();
    }

    /**
     * Probe whether the underlying log table has any records via {@code Admin.listOffsets}. When
     * the supplier is {@code null} (e.g. tests that don't wire an Admin), the probe declares the
     * topic empty — the design's empty-table guard is a server-side safety check that becomes a
     * no-op without an Admin client, and the IT injects a real one to exercise scenario 7.
     */
    private EmptinessProbeResult probeEmpty(TablePath path, TableInfo current) {
        if (adminSupplier == null) {
            return EmptinessProbeResult.empty();
        }
        Admin admin;
        try {
            admin = adminSupplier.get();
        } catch (Exception e) {
            LOG.warn(
                    "TypedTableEvolver: admin supplier threw; treating topic as empty for first-register: {}",
                    e.toString());
            return EmptinessProbeResult.empty();
        }
        if (admin == null) {
            return EmptinessProbeResult.empty();
        }
        int numBuckets = current.getNumBuckets();
        List<Integer> buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(i);
        }
        try {
            ListOffsetsResult start =
                    admin.listOffsets(path, buckets, new OffsetSpec.EarliestSpec());
            ListOffsetsResult end = admin.listOffsets(path, buckets, new OffsetSpec.LatestSpec());
            for (int b : buckets) {
                Long s = await(start.bucketResult(b));
                Long e = await(end.bucketResult(b));
                if (s == null || e == null) {
                    continue;
                }
                if (e.longValue() > s.longValue()) {
                    return EmptinessProbeResult.nonEmpty(b, s.longValue(), e.longValue());
                }
            }
            return EmptinessProbeResult.empty();
        } catch (Exception e) {
            LOG.warn(
                    "TypedTableEvolver: listOffsets for '{}' failed; treating topic as empty for first-register: {}",
                    path,
                    e.toString());
            return EmptinessProbeResult.empty();
        }
    }

    @Nullable
    private static Long await(java.util.concurrent.CompletableFuture<Long> f) {
        try {
            return f.get(3, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            return null;
        }
    }

    /**
     * Map a canonical Schema-Registry format id (one of {@code AVRO}, {@code JSON}, {@code
     * PROTOBUF}) to the matching catalog typed-format string.
     */
    private static String typedFormatFor(String formatId) {
        switch (formatId) {
            case "AVRO":
                return CatalogTableEntity.FORMAT_KAFKA_TYPED_AVRO;
            case "JSON":
                return CatalogTableEntity.FORMAT_KAFKA_TYPED_JSON;
            case "PROTOBUF":
                return CatalogTableEntity.FORMAT_KAFKA_TYPED_PROTOBUF;
            default:
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNSUPPORTED,
                        "format '" + formatId + "' has no typed catalog mapping");
        }
    }

    private static boolean isTypedFormat(String catalogFormat) {
        if (catalogFormat == null) {
            return false;
        }
        return CatalogTableEntity.FORMAT_KAFKA_TYPED_AVRO.equals(catalogFormat)
                || CatalogTableEntity.FORMAT_KAFKA_TYPED_JSON.equals(catalogFormat)
                || CatalogTableEntity.FORMAT_KAFKA_TYPED_PROTOBUF.equals(catalogFormat);
    }

    /**
     * Maps {@link KafkaTopicRoute.Kind} to the corresponding catalog format string. Convenience for
     * tests that pre-stage typed routes; production code uses {@link
     * CatalogTableEntity#FORMAT_KAFKA_TYPED_AVRO} et al directly.
     */
    @VisibleForTesting
    static Map<KafkaTopicRoute.Kind, String> kindToCatalogFormat() {
        Map<KafkaTopicRoute.Kind, String> m = new LinkedHashMap<>();
        m.put(KafkaTopicRoute.Kind.PASSTHROUGH, CatalogTableEntity.FORMAT_KAFKA_PASSTHROUGH);
        m.put(KafkaTopicRoute.Kind.TYPED_AVRO, CatalogTableEntity.FORMAT_KAFKA_TYPED_AVRO);
        m.put(KafkaTopicRoute.Kind.TYPED_JSON, CatalogTableEntity.FORMAT_KAFKA_TYPED_JSON);
        m.put(KafkaTopicRoute.Kind.TYPED_PROTOBUF, CatalogTableEntity.FORMAT_KAFKA_TYPED_PROTOBUF);
        return Collections.unmodifiableMap(m);
    }

    /**
     * Diff result. Either a list of {@link TableChange#addColumn} changes (possibly empty for a
     * no-op alter) or a single rejection message that the SR HTTP layer surfaces as 409.
     */
    public static final class AdditiveDelta {
        private final List<TableChange> changes;
        private final @Nullable String reject;

        private AdditiveDelta(List<TableChange> changes, @Nullable String reject) {
            this.changes = changes;
            this.reject = reject;
        }

        static AdditiveDelta accept(List<TableChange> changes) {
            return new AdditiveDelta(Collections.unmodifiableList(changes), null);
        }

        static AdditiveDelta reject(String message) {
            return new AdditiveDelta(Collections.emptyList(), message);
        }

        public List<TableChange> changes() {
            return changes;
        }

        @Nullable
        public String reject() {
            return reject;
        }
    }

    /** Holder for {@link #probeEmpty} so callers can format a useful error message. */
    private static final class EmptinessProbeResult {
        final boolean nonEmpty;
        final int firstNonEmptyBucket;
        final long firstNonEmptyStart;
        final long firstNonEmptyEnd;

        private EmptinessProbeResult(
                boolean nonEmpty, int bucket, long startOffset, long endOffset) {
            this.nonEmpty = nonEmpty;
            this.firstNonEmptyBucket = bucket;
            this.firstNonEmptyStart = startOffset;
            this.firstNonEmptyEnd = endOffset;
        }

        static EmptinessProbeResult empty() {
            return new EmptinessProbeResult(false, -1, 0L, 0L);
        }

        static EmptinessProbeResult nonEmpty(int bucket, long start, long end) {
            return new EmptinessProbeResult(true, bucket, start, end);
        }
    }
}
