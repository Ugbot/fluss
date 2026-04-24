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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogException;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.GrantEntity;
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.PrincipalEntity;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.sr.compat.AvroCompatibilityChecker;
import org.apache.fluss.kafka.sr.compat.CompatLevel;
import org.apache.fluss.kafka.sr.compat.CompatibilityResult;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.MetadataManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Projection over {@link CatalogService} that speaks the Confluent Schema Registry REST shape.
 *
 * <p>Every SR operation is a read / write against the catalog entity tables in the reserved {@code
 * _catalog} database:
 *
 * <ul>
 *   <li><b>register</b> → {@code bindKafkaSubject} + {@code registerSchema}.
 *   <li><b>schemaById</b> → {@code getSchemaById}.
 *   <li><b>listSubjects</b> → {@code listKafkaSubjects}.
 *   <li><b>latestForSubject</b> → {@code resolveKafkaSubject} + {@code listSchemaVersions}.
 * </ul>
 *
 * <p>Confluent ids are deterministic on {@code (catalog table id, version, format)}; schema history
 * is append-only. There is no per-topic custom-property storage.
 *
 * <p>The only non-catalog dependency is {@link MetadataManager}, used to verify that the Fluss
 * Kafka data table ({@code <kafkaDatabase>.<topic>}) exists before we let a subject bind — keeping
 * the "can't bind a subject to a topic that doesn't exist" error in the SR layer.
 */
@Internal
public final class SchemaRegistryService {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryService.class);

    private static final String FORMAT_AVRO = "AVRO";
    private static final String KEY_OR_VALUE_VALUE = "value";
    private static final String NAMING_STRATEGY = "TopicNameStrategy";

    private final MetadataManager metadataManager;
    private final CatalogService catalog;
    private final String kafkaDatabase;
    private final boolean rbacEnforced;

    public SchemaRegistryService(
            MetadataManager metadataManager, CatalogService catalog, String kafkaDatabase) {
        this(metadataManager, catalog, kafkaDatabase, false);
    }

    public SchemaRegistryService(
            MetadataManager metadataManager,
            CatalogService catalog,
            String kafkaDatabase,
            boolean rbacEnforced) {
        this.metadataManager = metadataManager;
        this.catalog = catalog;
        this.kafkaDatabase = kafkaDatabase;
        this.rbacEnforced = rbacEnforced;
    }

    /** Fallback compatibility when no global config row is stored. */
    public static final String DEFAULT_COMPATIBILITY = "BACKWARD";

    /** Fallback mode when no global mode row is stored. */
    public static final String DEFAULT_MODE = "READWRITE";

    private static final String KEY_GLOBAL_COMPAT = "global_compatibility";
    private static final String KEY_SUBJECT_COMPAT_PREFIX = "subject_compatibility:";
    private static final String KEY_GLOBAL_MODE = "global_mode";
    private static final String KEY_SUBJECT_MODE_PREFIX = "subject_mode:";

    /**
     * Tombstone keys stored in {@code _sr_config}. Presence of either key marks the corresponding
     * entity as soft-deleted. Re-registering the subject or version clears the tombstone. The
     * per-schema key is the catalog schema UUID (not the Confluent id) so that a hard-delete of the
     * row doesn't orphan a tombstone.
     */
    private static final String KEY_TOMBSTONE_SUBJECT_PREFIX = "tombstone_subject:";

    private static final String KEY_TOMBSTONE_SCHEMA_PREFIX = "tombstone_schema:";

    private final AvroCompatibilityChecker compatibilityChecker = new AvroCompatibilityChecker();

    /** Effective global compatibility level; persisted via {@link #setGlobalCompatibility}. */
    public String defaultCompatibility() {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            return catalog.getSrConfig(KEY_GLOBAL_COMPAT)
                    .map(e -> e.value())
                    .orElse(DEFAULT_COMPATIBILITY);
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void setGlobalCompatibility(String level) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        validateCompatibility(level);
        try {
            catalog.setSrConfig(KEY_GLOBAL_COMPAT, level.toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public Optional<String> subjectCompatibility(String subject) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            return catalog.getSrConfig(KEY_SUBJECT_COMPAT_PREFIX + subject).map(e -> e.value());
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void setSubjectCompatibility(String subject, String level) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        validateCompatibility(level);
        try {
            catalog.setSrConfig(
                    KEY_SUBJECT_COMPAT_PREFIX + subject, level.toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void deleteSubjectCompatibility(String subject) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        try {
            catalog.deleteSrConfig(KEY_SUBJECT_COMPAT_PREFIX + subject);
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public String globalMode() {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            return catalog.getSrConfig(KEY_GLOBAL_MODE).map(e -> e.value()).orElse(DEFAULT_MODE);
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void setGlobalMode(String mode) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        validateMode(mode);
        try {
            catalog.setSrConfig(KEY_GLOBAL_MODE, mode.toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public Optional<String> subjectMode(String subject) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            return catalog.getSrConfig(KEY_SUBJECT_MODE_PREFIX + subject).map(e -> e.value());
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void setSubjectMode(String subject, String mode) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        validateMode(mode);
        try {
            catalog.setSrConfig(
                    KEY_SUBJECT_MODE_PREFIX + subject, mode.toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public void deleteSubjectMode(String subject) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        try {
            catalog.deleteSrConfig(KEY_SUBJECT_MODE_PREFIX + subject);
        } catch (Exception e) {
            throw translate(e);
        }
    }

    private static void validateCompatibility(String level) {
        if (level == null) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "compatibility level is required");
        }
        String normalised = level.toUpperCase(java.util.Locale.ROOT);
        switch (normalised) {
            case "NONE":
            case "BACKWARD":
            case "BACKWARD_TRANSITIVE":
            case "FORWARD":
            case "FORWARD_TRANSITIVE":
            case "FULL":
            case "FULL_TRANSITIVE":
                return;
            default:
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "Unsupported compatibility level '" + level + "'");
        }
    }

    private static void validateMode(String mode) {
        if (mode == null) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "mode is required");
        }
        String normalised = mode.toUpperCase(java.util.Locale.ROOT);
        switch (normalised) {
            case "READWRITE":
            case "READONLY":
            case "IMPORT":
                return;
            default:
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "Unsupported mode '" + mode + "'");
        }
    }

    /**
     * Resolve the caller principal. Reads the per-request principal extracted by {@link
     * SchemaRegistryHttpHandler} (see {@link SchemaRegistryCallContext}); falls back to {@link
     * PrincipalEntity#ANONYMOUS} when no principal was extracted — either because no trust path
     * fired, or because callers (tests) reach this service outside the HTTP path.
     */
    private String callerPrincipal() {
        FlussPrincipal current = SchemaRegistryCallContext.current();
        if (current == null || current.getName() == null || current.getName().isEmpty()) {
            return PrincipalEntity.ANONYMOUS;
        }
        return current.getName();
    }

    /**
     * Verify the caller holds {@code privilege} on the catalog wildcard. No-op when {@code
     * kafka.schema-registry.rbac.enforced=false} (the default); flipping it on without principal
     * extraction locks the SR down until an operator creates matching grants.
     */
    private void authorize(String privilege) {
        if (!rbacEnforced) {
            return;
        }
        try {
            boolean allowed =
                    catalog.checkPrivilege(
                            callerPrincipal(),
                            GrantEntity.KIND_CATALOG,
                            GrantEntity.CATALOG_WILDCARD,
                            privilege);
            if (!allowed) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.FORBIDDEN,
                        "principal '"
                                + callerPrincipal()
                                + "' is not granted "
                                + privilege
                                + " on the catalog");
            }
        } catch (SchemaRegistryException sre) {
            throw sre;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public int register(String subject, String avroSchema) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        if (avroSchema == null || avroSchema.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "schema body is required");
        }
        String topic = SubjectResolver.topicFromValueSubject(subject);
        // Fail fast if the Kafka data table doesn't exist — keeps the 404 clean.
        requireKafkaTable(topic, subject);
        try {
            // Clear a subject tombstone first so the idempotent fast path and compat check see the
            // subject as live again (Confluent SR behaviour: re-registering a soft-deleted subject
            // resurrects it).
            clearSubjectTombstone(subject);

            // Idempotent fast path: if the subject is already bound and the latest <live> schema
            // text matches, return the existing Confluent id without appending a new version.
            // latestForSubject already filters out tombstoned versions, so a soft-deleted match
            // will not short-circuit here; instead we'll clear its tombstone and re-register.
            Optional<RegisteredSchema> latest = latestForSubject(subject);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "register subject={} latestPresent={} latestMatches={}",
                        subject,
                        latest.isPresent(),
                        latest.isPresent() && avroSchema.equals(latest.get().schema()));
            }
            if (latest.isPresent() && avroSchema.equals(latest.get().schema())) {
                return latest.get().id();
            }

            // Resurrect soft-deleted version if the text matches an existing (tombstoned) row —
            // clears the tombstone and returns that row's Confluent id rather than minting a new
            // version.
            Optional<RegisteredSchema> resurrected =
                    resurrectTombstonedSchemaIfMatches(subject, topic, avroSchema);
            if (resurrected.isPresent()) {
                // The binding may also be soft-deleted / absent — re-bind to be safe.
                ensureCatalogEntities(topic);
                catalog.bindKafkaSubject(
                        subject, kafkaDatabase, topic, KEY_OR_VALUE_VALUE, NAMING_STRATEGY);
                return resurrected.get().id();
            }

            // Compatibility gate — runs before registration when there's a live prior history.
            enforceCompatibilityOrThrow(subject, topic, avroSchema);

            ensureCatalogEntities(topic);
            SchemaVersionEntity version =
                    catalog.registerSchema(
                            kafkaDatabase, topic, FORMAT_AVRO, avroSchema, /* registeredBy */ null);
            catalog.bindKafkaSubject(
                    subject, kafkaDatabase, topic, KEY_OR_VALUE_VALUE, NAMING_STRATEGY);
            return version.confluentId();
        } catch (Exception e) {
            throw translate(e);
        }
    }

    // ---------- soft-delete ----------

    /**
     * Soft-delete one version under {@code subject}. {@code permanent=true} hard-deletes the schema
     * row instead of tombstoning it. Returns the deleted version number (Confluent's behaviour —
     * the HTTP response body is the integer version).
     */
    public int deleteVersion(String subject, int version, boolean permanent) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        try {
            String topic = SubjectResolver.topicFromValueSubject(subject);
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' not found");
            }
            Optional<SchemaVersionEntity> schema =
                    catalog.getSchemaVersion(kafkaDatabase, topic, version);
            if (!schema.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' version " + version + " not found");
            }
            String schemaId = schema.get().schemaId();
            boolean alreadyTombstoned = isSchemaTombstoned(schemaId);
            if (permanent) {
                catalog.deleteSchemaVersion(schemaId);
                // Always clear the tombstone on permanent delete so stale rows don't linger.
                catalog.deleteSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + schemaId);
            } else {
                if (alreadyTombstoned) {
                    // Confluent SR returns 404 when the same version is soft-deleted twice.
                    throw new SchemaRegistryException(
                            SchemaRegistryException.Kind.NOT_FOUND,
                            "Subject '"
                                    + subject
                                    + "' version "
                                    + version
                                    + " is already soft-deleted");
                }
                catalog.setSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + schemaId, "1");
            }
            return version;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Soft-delete {@code subject} (tombstone its binding). {@code permanent=true} hard-deletes the
     * binding and cascades into every schema version bound to its table. Returns the list of
     * versions that were deleted, in ascending order — mirrors Confluent's {@code DELETE
     * /subjects/{s}} response body.
     */
    public List<Integer> deleteSubject(String subject, boolean permanent) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' not found");
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> versions = catalog.listSchemaVersions(kafkaDatabase, topic);
            boolean alreadyTombstoned = isSubjectTombstoned(subject);

            List<Integer> out = new ArrayList<>();
            if (permanent) {
                for (SchemaVersionEntity v : versions) {
                    catalog.deleteSchemaVersion(v.schemaId());
                    catalog.deleteSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + v.schemaId());
                    out.add(v.version());
                }
                catalog.unbindKafkaSubject(subject);
                catalog.deleteSrConfig(KEY_TOMBSTONE_SUBJECT_PREFIX + subject);
                catalog.deleteSrConfig(KEY_SUBJECT_COMPAT_PREFIX + subject);
                catalog.deleteSrConfig(KEY_SUBJECT_MODE_PREFIX + subject);
            } else {
                if (alreadyTombstoned) {
                    throw new SchemaRegistryException(
                            SchemaRegistryException.Kind.NOT_FOUND,
                            "Subject '" + subject + "' is already soft-deleted");
                }
                catalog.setSrConfig(KEY_TOMBSTONE_SUBJECT_PREFIX + subject, "1");
                for (SchemaVersionEntity v : versions) {
                    if (!isSchemaTombstoned(v.schemaId())) {
                        out.add(v.version());
                    }
                }
            }
            Collections.sort(out);
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Check the proposed {@code schemaText} against {@code subject}'s existing versions at the
     * effective compatibility level. Does <b>not</b> register — backs {@code POST
     * /compatibility/subjects/{s}/versions/{v}}. {@code version == -1} is Confluent's alias for
     * "latest".
     */
    public CompatibilityResult checkCompatibility(String subject, int version, String schemaText) {
        authorize(GrantEntity.PRIVILEGE_READ);
        if (schemaText == null || schemaText.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "schema body is required");
        }
        try {
            String topic = SubjectResolver.topicFromValueSubject(subject);
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.NOT_FOUND,
                        "Subject '" + subject + "' not found");
            }
            List<SchemaVersionEntity> liveVersions = liveVersionsAscending(topic);
            if (liveVersions.isEmpty()) {
                // No prior history → vacuously compatible.
                return CompatibilityResult.compatible();
            }
            // The {version} path segment scopes which priors participate; Confluent lets callers
            // target any live version. For non-"latest" values we ensure the requested version
            // actually exists (returns 404 otherwise).
            if (version != -1) {
                boolean found = false;
                for (SchemaVersionEntity v : liveVersions) {
                    if (v.version() == version) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SchemaRegistryException(
                            SchemaRegistryException.Kind.NOT_FOUND,
                            "Subject '" + subject + "' version " + version + " not found");
                }
            }
            List<String> priorTexts = new ArrayList<>(liveVersions.size());
            for (SchemaVersionEntity v : liveVersions) {
                priorTexts.add(v.schemaText());
            }
            CompatLevel level = effectiveLevel(subject);
            return compatibilityChecker.check(schemaText, priorTexts, level);
        } catch (Exception e) {
            throw translate(e);
        }
    }

    private CompatLevel effectiveLevel(String subject) throws Exception {
        Optional<String> subjectLevel =
                catalog.getSrConfig(KEY_SUBJECT_COMPAT_PREFIX + subject).map(e -> e.value());
        if (subjectLevel.isPresent()) {
            return CompatLevel.fromString(subjectLevel.get());
        }
        Optional<String> globalLevel = catalog.getSrConfig(KEY_GLOBAL_COMPAT).map(e -> e.value());
        if (globalLevel.isPresent()) {
            return CompatLevel.fromString(globalLevel.get());
        }
        return CompatLevel.fromString(DEFAULT_COMPATIBILITY);
    }

    private void enforceCompatibilityOrThrow(String subject, String topic, String proposed)
            throws Exception {
        CompatLevel level = effectiveLevel(subject);
        if (level == CompatLevel.NONE) {
            return;
        }
        List<SchemaVersionEntity> liveVersions = liveVersionsAscending(topic);
        if (liveVersions.isEmpty()) {
            return;
        }
        List<String> priorTexts = new ArrayList<>(liveVersions.size());
        for (SchemaVersionEntity v : liveVersions) {
            priorTexts.add(v.schemaText());
        }
        CompatibilityResult result = compatibilityChecker.check(proposed, priorTexts, level);
        if (!result.isCompatible()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Schema is incompatible with ")
                    .append(level.name())
                    .append(" for subject '")
                    .append(subject)
                    .append("':");
            for (String m : result.messages()) {
                sb.append("\n  ").append(m);
            }
            throw new SchemaRegistryException(SchemaRegistryException.Kind.CONFLICT, sb.toString());
        }
    }

    private List<SchemaVersionEntity> liveVersionsAscending(String topic) throws Exception {
        List<SchemaVersionEntity> all = catalog.listSchemaVersions(kafkaDatabase, topic);
        List<SchemaVersionEntity> live = new ArrayList<>(all.size());
        for (SchemaVersionEntity v : all) {
            if (!isSchemaTombstoned(v.schemaId())) {
                live.add(v);
            }
        }
        live.sort((a, b) -> Integer.compare(a.version(), b.version()));
        return live;
    }

    private Optional<RegisteredSchema> resurrectTombstonedSchemaIfMatches(
            String subject, String topic, String schemaText) throws Exception {
        List<SchemaVersionEntity> all = catalog.listSchemaVersions(kafkaDatabase, topic);
        for (SchemaVersionEntity v : all) {
            if (schemaText.equals(v.schemaText()) && isSchemaTombstoned(v.schemaId())) {
                catalog.deleteSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + v.schemaId());
                return Optional.of(
                        new RegisteredSchema(
                                v.confluentId(), subject, v.version(), v.format(), v.schemaText()));
            }
        }
        return Optional.empty();
    }

    private void clearSubjectTombstone(String subject) throws Exception {
        if (isSubjectTombstoned(subject)) {
            catalog.deleteSrConfig(KEY_TOMBSTONE_SUBJECT_PREFIX + subject);
        }
    }

    private boolean isSubjectTombstoned(String subject) throws Exception {
        return catalog.getSrConfig(KEY_TOMBSTONE_SUBJECT_PREFIX + subject).isPresent();
    }

    private boolean isSchemaTombstoned(String schemaId) throws Exception {
        return catalog.getSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + schemaId).isPresent();
    }

    public Optional<RegisteredSchema> schemaById(int id) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<SchemaVersionEntity> schema = catalog.getSchemaById(id);
            if (!schema.isPresent() || isSchemaTombstoned(schema.get().schemaId())) {
                return Optional.empty();
            }
            SchemaVersionEntity s = schema.get();
            return Optional.of(
                    new RegisteredSchema(
                            s.confluentId(),
                            /* subject unused by GET /schemas/ids */ "",
                            s.version(),
                            s.format(),
                            s.schemaText()));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public List<String> listSubjects() {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            List<String> out = new ArrayList<>();
            for (KafkaSubjectBinding b : catalog.listKafkaSubjects()) {
                if (isSubjectTombstoned(b.subject())) {
                    continue;
                }
                out.add(b.subject());
            }
            Collections.sort(out);
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Supported schema types advertised by {@code GET /schemas/types}. Phase SR-X.1 is Avro-only;
     * JSON / Protobuf land in Phase T alongside typed-table support.
     */
    public List<String> supportedTypes() {
        return Collections.singletonList(FORMAT_AVRO);
    }

    /**
     * Subjects that have bound any schema version sharing the Confluent global {@code id}. Backs
     * {@code GET /schemas/ids/{id}/subjects}. Returns empty when the id is unknown.
     */
    public List<String> subjectsForId(int id) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<SchemaVersionEntity> schema = catalog.getSchemaById(id);
            if (!schema.isPresent() || isSchemaTombstoned(schema.get().schemaId())) {
                return Collections.emptyList();
            }
            String tableId = schema.get().tableId();
            List<String> out = new ArrayList<>();
            for (KafkaSubjectBinding b : catalog.listKafkaSubjects()) {
                if (tableId.equals(b.tableId()) && !isSubjectTombstoned(b.subject())) {
                    out.add(b.subject());
                }
            }
            Collections.sort(out);
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * {@code (subject, version)} tuples for the Confluent id. Backs {@code GET
     * /schemas/ids/{id}/versions}. Returns empty when the id is unknown.
     */
    public List<SubjectVersion> subjectVersionsForId(int id) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<SchemaVersionEntity> schema = catalog.getSchemaById(id);
            if (!schema.isPresent() || isSchemaTombstoned(schema.get().schemaId())) {
                return Collections.emptyList();
            }
            SchemaVersionEntity s = schema.get();
            String tableId = s.tableId();
            int version = s.version();
            List<SubjectVersion> out = new ArrayList<>();
            for (KafkaSubjectBinding b : catalog.listKafkaSubjects()) {
                if (tableId.equals(b.tableId()) && !isSubjectTombstoned(b.subject())) {
                    out.add(new SubjectVersion(b.subject(), version));
                }
            }
            Collections.sort(out, (a, c) -> a.subject().compareTo(c.subject()));
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /** {@code (subject, version)} tuple for reverse schema-id lookups. */
    public static final class SubjectVersion {
        private final String subject;
        private final int version;

        public SubjectVersion(String subject, int version) {
            this.subject = subject;
            this.version = version;
        }

        public String subject() {
            return subject;
        }

        public int version() {
            return version;
        }
    }

    /** Version numbers (1-based, monotone) registered against a subject, sorted ascending. */
    public List<Integer> listVersions(String subject) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                return Collections.emptyList();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> versions = catalog.listSchemaVersions(kafkaDatabase, topic);
            List<Integer> out = new ArrayList<>(versions.size());
            for (SchemaVersionEntity v : versions) {
                if (isSchemaTombstoned(v.schemaId())) {
                    continue;
                }
                out.add(v.version());
            }
            Collections.sort(out);
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Resolve a specific version for a subject. {@code version == -1} is Confluent's alias for
     * "latest" and is accepted here; callers translate textual {@code "latest"} before invoking.
     */
    public Optional<RegisteredSchema> versionForSubject(String subject, int version) {
        if (version == -1) {
            return latestForSubject(subject);
        }
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                return Optional.empty();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            Optional<SchemaVersionEntity> schema =
                    catalog.getSchemaVersion(kafkaDatabase, topic, version);
            if (!schema.isPresent() || isSchemaTombstoned(schema.get().schemaId())) {
                return Optional.empty();
            }
            SchemaVersionEntity s = schema.get();
            return Optional.of(
                    new RegisteredSchema(
                            s.confluentId(), subject, s.version(), s.format(), s.schemaText()));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Probe: does {@code schemaText} already exist under {@code subject}? Returns the registered
     * tuple if so, {@link Optional#empty()} otherwise. Never writes.
     */
    public Optional<RegisteredSchema> schemaExists(String subject, String schemaText) {
        authorize(GrantEntity.PRIVILEGE_READ);
        if (schemaText == null || schemaText.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "schema body is required");
        }
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                return Optional.empty();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> versions = catalog.listSchemaVersions(kafkaDatabase, topic);
            for (SchemaVersionEntity v : versions) {
                if (schemaText.equals(v.schemaText()) && !isSchemaTombstoned(v.schemaId())) {
                    return Optional.of(
                            new RegisteredSchema(
                                    v.confluentId(),
                                    subject,
                                    v.version(),
                                    v.format(),
                                    v.schemaText()));
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            throw translate(e);
        }
    }

    public Optional<RegisteredSchema> latestForSubject(String subject) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            // PK-lookup chain: binding → table.currentSchemaId → schema. Every hop is a Fluss
            // PK lookup (read-after-write consistent); no scans involved. Falls through to a
            // version-scan when the table pointer lands on a tombstoned row, so the caller sees
            // the greatest live version rather than a phantom "no schema".
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                return Optional.empty();
            }
            Optional<org.apache.fluss.catalog.entities.CatalogTableEntity> table =
                    catalog.getTableById(binding.get().tableId());
            if (!table.isPresent() || table.get().currentSchemaId() == null) {
                return Optional.empty();
            }
            Optional<SchemaVersionEntity> schema =
                    catalog.getSchemaBySchemaId(table.get().currentSchemaId());
            if (schema.isPresent() && !isSchemaTombstoned(schema.get().schemaId())) {
                SchemaVersionEntity s = schema.get();
                return Optional.of(
                        new RegisteredSchema(
                                s.confluentId(), subject, s.version(), s.format(), s.schemaText()));
            }
            // Fall back to the live history tail.
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> live = liveVersionsAscending(topic);
            if (live.isEmpty()) {
                return Optional.empty();
            }
            SchemaVersionEntity s = live.get(live.size() - 1);
            return Optional.of(
                    new RegisteredSchema(
                            s.confluentId(), subject, s.version(), s.format(), s.schemaText()));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Ensure the catalog has a namespace named {@link #kafkaDatabase} and a {@code
     * KAFKA_PASSTHROUGH}-format table entity for {@code topic}. Idempotent — swallows {@code
     * ALREADY_EXISTS}.
     */
    private void ensureCatalogEntities(String topic) throws Exception {
        if (!catalog.getNamespace(kafkaDatabase).isPresent()) {
            try {
                catalog.createNamespace(
                        null, kafkaDatabase, "Kafka-compat data tables (Fluss topic store)");
            } catch (CatalogException ce) {
                if (ce.kind() != CatalogException.Kind.ALREADY_EXISTS) {
                    throw ce;
                }
            }
        }
        if (!catalog.getTable(kafkaDatabase, topic).isPresent()) {
            try {
                catalog.createTable(
                        kafkaDatabase,
                        topic,
                        "KAFKA_PASSTHROUGH",
                        kafkaDatabase + "." + topic,
                        null);
            } catch (CatalogException ce) {
                if (ce.kind() != CatalogException.Kind.ALREADY_EXISTS) {
                    throw ce;
                }
            }
        }
    }

    private void requireKafkaTable(String topic, String subject) {
        TablePath path = new TablePath(kafkaDatabase, topic);
        try {
            metadataManager.getTable(path);
        } catch (TableNotExistException gone) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.NOT_FOUND,
                    "Subject "
                            + subject
                            + " requires a pre-existing Kafka topic ("
                            + path
                            + "). Create the topic first (e.g. via Kafka Admin "
                            + "CreateTopics).");
        }
    }

    private static RuntimeException translate(Exception e) {
        if (e instanceof SchemaRegistryException) {
            return (SchemaRegistryException) e;
        }
        if (e instanceof CatalogException) {
            CatalogException ce = (CatalogException) e;
            switch (ce.kind()) {
                case INVALID_INPUT:
                    return new SchemaRegistryException(
                            SchemaRegistryException.Kind.INVALID_INPUT, ce.getMessage(), ce);
                case NOT_FOUND:
                    return new SchemaRegistryException(
                            SchemaRegistryException.Kind.NOT_FOUND, ce.getMessage(), ce);
                case ALREADY_EXISTS:
                case CONFLICT:
                    return new SchemaRegistryException(
                            SchemaRegistryException.Kind.CONFLICT, ce.getMessage(), ce);
                case UNSUPPORTED:
                    return new SchemaRegistryException(
                            SchemaRegistryException.Kind.UNSUPPORTED, ce.getMessage(), ce);
                case INTERNAL:
                default:
                    return new SchemaRegistryException(
                            SchemaRegistryException.Kind.INTERNAL, ce.getMessage(), ce);
            }
        }
        return new SchemaRegistryException(
                SchemaRegistryException.Kind.INTERNAL, e.getMessage(), e);
    }

    /** Immutable snapshot of one registered schema. */
    public static final class RegisteredSchema {
        private final int id;
        private final String subject;
        private final int version;
        private final String format;
        private final String schema;

        public RegisteredSchema(int id, String subject, int version, String format, String schema) {
            this.id = id;
            this.subject = subject;
            this.version = version;
            this.format = format;
            this.schema = schema;
        }

        public int id() {
            return id;
        }

        public String subject() {
            return subject;
        }

        public int version() {
            return version;
        }

        public String format() {
            return format;
        }

        public String schema() {
            return schema;
        }
    }
}
