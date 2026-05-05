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
import org.apache.fluss.catalog.entities.SchemaReference;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.sr.compat.AvroCompatibilityChecker;
import org.apache.fluss.kafka.sr.compat.CompatLevel;
import org.apache.fluss.kafka.sr.compat.CompatibilityChecker;
import org.apache.fluss.kafka.sr.compat.CompatibilityResult;
import org.apache.fluss.kafka.sr.references.CatalogReferenceResolver;
import org.apache.fluss.kafka.sr.references.ReferenceResolver;
import org.apache.fluss.kafka.sr.typed.FormatRegistry;
import org.apache.fluss.kafka.sr.typed.TypedTableEvolver;
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
 * Projection over {@link CatalogService} that speaks the Kafka Schema Registry REST shape.
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
 * <p>Kafka SR schema ids are deterministic on {@code (catalog table id, version, format)}; schema
 * history is append-only. There is no per-topic custom-property storage.
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
    private final TypedTableEvolver typedTableEvolver;

    public SchemaRegistryService(
            MetadataManager metadataManager, CatalogService catalog, String kafkaDatabase) {
        this(metadataManager, catalog, kafkaDatabase, false);
    }

    public SchemaRegistryService(
            MetadataManager metadataManager,
            CatalogService catalog,
            String kafkaDatabase,
            boolean rbacEnforced) {
        this(
                metadataManager,
                catalog,
                kafkaDatabase,
                rbacEnforced,
                new TypedTableEvolver(metadataManager, catalog, kafkaDatabase, null, null, false));
    }

    /**
     * Phase T.3 constructor: wires in the {@link TypedTableEvolver} so registrations reshape the
     * underlying Fluss data table when the {@code kafka.typed-tables.enabled} flag is on. The
     * evolver is a no-op when that flag is off.
     */
    public SchemaRegistryService(
            MetadataManager metadataManager,
            CatalogService catalog,
            String kafkaDatabase,
            boolean rbacEnforced,
            TypedTableEvolver typedTableEvolver) {
        this.metadataManager = metadataManager;
        this.catalog = catalog;
        this.kafkaDatabase = kafkaDatabase;
        this.rbacEnforced = rbacEnforced;
        this.typedTableEvolver = typedTableEvolver;
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
     * per-schema key is the catalog schema UUID (not the Kafka SR schema id) so that a hard-delete
     * of the row doesn't orphan a tombstone.
     */
    private static final String KEY_TOMBSTONE_SUBJECT_PREFIX = "tombstone_subject:";

    private static final String KEY_TOMBSTONE_SCHEMA_PREFIX = "tombstone_schema:";

    /**
     * Fallback compat checker for the legacy single-format code path. Real per-format dispatch goes
     * through {@link FormatRegistry#checker(String)} keyed on the subject's stored format.
     */
    private final AvroCompatibilityChecker legacyAvroChecker = new AvroCompatibilityChecker();

    private CompatibilityChecker checkerFor(String formatId) {
        CompatibilityChecker registered = FormatRegistry.instance().checker(formatId);
        if (registered != null) {
            return registered;
        }
        // AVRO is always registered via ServiceLoader; unknown formats (JSON / PROTOBUF before
        // their checkers land) fall through to the legacy Avro one so existing subjects keep
        // working. Wrong-format registrations are caught upstream at translator resolution.
        return legacyAvroChecker;
    }

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
        return register(subject, avroSchema, FORMAT_AVRO);
    }

    /**
     * Register under an explicit {@code formatId} (one of {@code "AVRO"}, {@code "JSON"}, {@code
     * "PROTOBUF"}). Validates the format through {@link FormatRegistry} so unknown formats produce
     * a clean 422. Phase T-MF.5.
     */
    public int register(String subject, String schemaText, String formatId) {
        return register(subject, schemaText, formatId, Collections.emptyList());
    }

    /**
     * Register under an explicit {@code formatId} with a {@code references} list. Each entry's
     * {@code (subject, version)} is resolved against the catalog and pinned to the referent's
     * {@code schema_id} before the compat gate runs and the row is written. A missing referent
     * yields HTTP 422. Phase SR-X.5.
     */
    public int register(
            String subject, String schemaText, String formatId, List<RequestedReference> refs) {
        authorize(GrantEntity.PRIVILEGE_WRITE);
        if (schemaText == null || schemaText.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "schema body is required");
        }
        if (refs == null) {
            refs = Collections.emptyList();
        }
        String canonicalFormat = canonicaliseFormat(formatId);
        String topic = SubjectResolver.topicFromValueSubject(subject);
        // Fail fast if the Kafka data table doesn't exist — keeps the 404 clean.
        requireKafkaTable(topic, subject);
        try {
            // Clear a subject tombstone first so the idempotent fast path and compat check see the
            // subject as live again (Kafka SR behaviour: re-registering a soft-deleted subject
            // resurrects it).
            clearSubjectTombstone(subject);

            // Resolve every reference up-front: pin (subject, version) to schema_id, fetch text.
            List<ResolvedReference> resolved = resolveReferences(refs);
            ReferenceResolver resolver = referenceResolver(resolved);

            // Idempotent fast path: if the subject is already bound and the latest <live> schema
            // text matches AND the prior reference set matches, return the existing Kafka SR schema
            // id
            // without appending a new version. Same text but different references must mint a new
            // version (Kafka SR semantics).
            Optional<RegisteredSchemaWithRefs> latest = latestForSubjectWithRefs(subject);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "register subject={} format={} latestPresent={} latestMatches={} refs={}",
                        subject,
                        canonicalFormat,
                        latest.isPresent(),
                        latest.isPresent() && schemaText.equals(latest.get().schema().schema()),
                        resolved.size());
            }
            if (latest.isPresent()
                    && schemaText.equals(latest.get().schema().schema())
                    && referenceListsEqual(latest.get().references(), resolved)) {
                return latest.get().schema().id();
            }

            // Resurrect soft-deleted version if the text + refs match an existing (tombstoned)
            // row — clears the tombstone and returns that row's Kafka SR schema id rather than
            // minting
            // a new version.
            Optional<RegisteredSchema> resurrected =
                    resurrectTombstonedSchemaIfMatches(subject, topic, schemaText, resolved);
            if (resurrected.isPresent()) {
                // The binding may also be soft-deleted / absent — re-bind to be safe. Re-apply
                // the references on the resurrected schema_id so a soft-deleted row that lost
                // its references regains them.
                ensureCatalogEntities(topic);
                catalog.bindKafkaSubject(
                        subject, kafkaDatabase, topic, KEY_OR_VALUE_VALUE, NAMING_STRATEGY);
                Optional<SchemaVersionEntity> existingRow =
                        catalog.getSchemaById(resurrected.get().id());
                if (existingRow.isPresent()) {
                    catalog.bindReferences(existingRow.get().schemaId(), toCatalogRefs(resolved));
                }
                return resurrected.get().id();
            }

            // Compatibility gate — runs before registration when there's a live prior history.
            enforceCompatibilityOrThrow(subject, topic, schemaText, resolver);

            ensureCatalogEntities(topic);
            // Phase T.3 — reshape the underlying Fluss table from KAFKA_PASSTHROUGH to
            // KAFKA_TYPED_<FORMAT> on first registration, or apply additive evolution on
            // subsequent ones. No-op when kafka.typed-tables.enabled = false. Runs AFTER
            // ensureCatalogEntities (so the catalog row exists) and BEFORE catalog.registerSchema
            // (so the Kafka SR schema id is minted only when the reshape / alter has succeeded).
            // Kafka SR schema ids live in __schemas__ keyed by schema_id UUID and are not touched
            // by
            // the reshape; T.2's decoder cache key (tableId, schemaId) is reshape-stable.
            typedTableEvolver.onRegister(topic, canonicalFormat, schemaText);
            SchemaVersionEntity version =
                    catalog.registerSchema(
                            kafkaDatabase,
                            topic,
                            canonicalFormat,
                            schemaText,
                            /* registeredBy */ null);
            catalog.bindReferences(version.schemaId(), toCatalogRefs(resolved));
            catalog.bindKafkaSubject(
                    subject, kafkaDatabase, topic, KEY_OR_VALUE_VALUE, NAMING_STRATEGY);
            return version.srSchemaId();
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /** Normalise a caller-supplied schemaType to the registry's canonical form (uppercase). */
    private String canonicaliseFormat(String formatId) {
        if (formatId == null || formatId.isEmpty()) {
            return FORMAT_AVRO;
        }
        String upper = formatId.trim().toUpperCase(java.util.Locale.ROOT);
        if (FormatRegistry.instance().translator(upper) == null) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNSUPPORTED,
                    "schemaType '"
                            + formatId
                            + "' is not registered; supported: "
                            + FormatRegistry.instance().formatIds());
        }
        return upper;
    }

    // ---------- soft-delete ----------

    /**
     * Soft-delete one version under {@code subject}. {@code permanent=true} hard-deletes the schema
     * row instead of tombstoning it. Returns the deleted version number (Kafka SR behaviour — the
     * HTTP response body is the integer version).
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
                refuseIfReferenced(schemaId, subject, version);
                // Cascade-delete reference rows owned by this referrer first; orphan reference
                // rows that point AT this schema id are left for `listReferencedBy` to ignore.
                catalog.deleteReferences(schemaId);
                catalog.deleteSchemaVersion(schemaId);
                // Always clear the tombstone on permanent delete so stale rows don't linger.
                catalog.deleteSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + schemaId);
            } else {
                if (alreadyTombstoned) {
                    // Kafka SR returns 404 when the same version is soft-deleted twice.
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
     * versions that were deleted, in ascending order — mirrors Kafka SR {@code DELETE
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
                // Pre-flight: refuse the subject delete if any of its versions is still
                // referenced — fail fast so we don't half-delete and leave dangling rows.
                for (SchemaVersionEntity v : versions) {
                    refuseIfReferenced(v.schemaId(), subject, v.version());
                }
                for (SchemaVersionEntity v : versions) {
                    catalog.deleteReferences(v.schemaId());
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
     * /compatibility/subjects/{s}/versions/{v}}. {@code version == -1} is Kafka SR alias for
     * "latest".
     */
    public CompatibilityResult checkCompatibility(String subject, int version, String schemaText) {
        return checkCompatibility(subject, version, schemaText, Collections.emptyList());
    }

    /**
     * Compatibility probe with references. Resolves {@code refs} the same way the register path
     * does (422 on missing referent) and feeds the resulting resolver to the per-format checker.
     * Phase SR-X.5.
     */
    public CompatibilityResult checkCompatibility(
            String subject, int version, String schemaText, List<RequestedReference> refs) {
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
            // The {version} path segment scopes which priors participate; Kafka SR lets callers
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
            String subjectFormat = liveVersions.get(0).format();
            List<ResolvedReference> resolved = resolveReferences(refs);
            ReferenceResolver resolver = referenceResolver(resolved);
            return checkerFor(subjectFormat).check(schemaText, priorTexts, level, resolver);
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

    /**
     * Resolve every requested reference against the catalog. A missing {@code (subject, version)}
     * pair, or a tombstoned referent, raises {@link SchemaRegistryException.Kind#INVALID_INPUT} →
     * HTTP 422 with the Kafka SR-style "referenced subject X version Y does not exist" message.
     * Empty input returns an empty list.
     */
    private List<ResolvedReference> resolveReferences(List<RequestedReference> refs)
            throws Exception {
        if (refs == null || refs.isEmpty()) {
            return Collections.emptyList();
        }
        List<ResolvedReference> out = new ArrayList<>(refs.size());
        for (RequestedReference req : refs) {
            if (req.subject() == null || req.subject().isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT,
                        "reference subject is required (anonymous references are not supported)");
            }
            if (req.name() == null || req.name().isEmpty()) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INVALID_INPUT, "reference name is required");
            }
            String referentTopic = SubjectResolver.topicFromValueSubject(req.subject());
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(req.subject());
            if (!binding.isPresent() || isSubjectTombstoned(req.subject())) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                        "referenced subject "
                                + req.subject()
                                + " version "
                                + req.version()
                                + " does not exist");
            }
            Optional<SchemaVersionEntity> referent =
                    catalog.getSchemaVersion(kafkaDatabase, referentTopic, req.version());
            if (!referent.isPresent() || isSchemaTombstoned(referent.get().schemaId())) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                        "referenced subject "
                                + req.subject()
                                + " version "
                                + req.version()
                                + " does not exist");
            }
            out.add(
                    new ResolvedReference(
                            req.name(),
                            req.subject(),
                            req.version(),
                            referent.get().schemaId(),
                            referent.get().schemaText()));
        }
        return out;
    }

    private static ReferenceResolver referenceResolver(List<ResolvedReference> resolved) {
        if (resolved == null || resolved.isEmpty()) {
            return ReferenceResolver.empty();
        }
        List<String> names = new ArrayList<>(resolved.size());
        List<String> texts = new ArrayList<>(resolved.size());
        for (ResolvedReference r : resolved) {
            names.add(r.name);
            texts.add(r.referencedText);
        }
        return CatalogReferenceResolver.of(names, texts);
    }

    private static List<SchemaReference> toCatalogRefs(List<ResolvedReference> resolved) {
        List<SchemaReference> out = new ArrayList<>(resolved.size());
        for (ResolvedReference r : resolved) {
            out.add(new SchemaReference(r.name, r.subject, r.version, r.referencedSchemaId));
        }
        return out;
    }

    private static List<SubjectReference> refs(List<SchemaReference> catalogRefs) {
        List<SubjectReference> out = new ArrayList<>(catalogRefs.size());
        for (SchemaReference r : catalogRefs) {
            out.add(new SubjectReference(r.name(), r.subject(), r.version()));
        }
        return out;
    }

    private static boolean referenceListsEqual(
            List<SchemaReference> stored, List<ResolvedReference> proposed) {
        if (stored.size() != proposed.size()) {
            return false;
        }
        for (int i = 0; i < stored.size(); i++) {
            SchemaReference s = stored.get(i);
            ResolvedReference p = proposed.get(i);
            if (!s.name().equals(p.name)
                    || !s.subject().equals(p.subject)
                    || s.version() != p.version
                    || !s.referencedSchemaId().equals(p.referencedSchemaId)) {
                return false;
            }
        }
        return true;
    }

    private static boolean referenceListsEqualToCatalog(
            List<SchemaReference> stored, List<ResolvedReference> proposed) {
        return referenceListsEqual(stored, proposed);
    }

    /**
     * Like {@link #latestForSubject(String)} but also fetches the referenced-by edges for the
     * latest live row. Used by the register fast path so that a same-text + same-refs re-submission
     * is recognised as idempotent.
     */
    private Optional<RegisteredSchemaWithRefs> latestForSubjectWithRefs(String subject)
            throws Exception {
        Optional<RegisteredSchema> latest = latestForSubject(subject);
        if (!latest.isPresent()) {
            return Optional.empty();
        }
        // We need the catalog UUID for the latest version to fetch its references.
        Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
        if (!binding.isPresent()) {
            return Optional.of(new RegisteredSchemaWithRefs(latest.get(), Collections.emptyList()));
        }
        String topic = SubjectResolver.topicFromValueSubject(subject);
        Optional<SchemaVersionEntity> ent =
                catalog.getSchemaVersion(kafkaDatabase, topic, latest.get().version());
        if (!ent.isPresent()) {
            return Optional.of(new RegisteredSchemaWithRefs(latest.get(), Collections.emptyList()));
        }
        List<SchemaReference> bound = catalog.listReferences(ent.get().schemaId());
        return Optional.of(new RegisteredSchemaWithRefs(latest.get(), bound));
    }

    /**
     * Look up the references bound to {@code schemaId} as wire-side {@link SubjectReference}
     * tuples. Empty when none. Used to populate the {@code references} array on read responses.
     */
    public List<SubjectReference> referencesForSchemaId(String schemaId) {
        try {
            return refs(catalog.listReferences(schemaId));
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Reverse-index lookup: every {@code (subject, version)} tuple that references the schema with
     * the given Kafka SR schema {@code id}. Backs {@code GET /schemas/ids/{id}/referencedby}.
     */
    public List<SubjectVersion> referencedBy(int id) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<SchemaVersionEntity> referent = catalog.getSchemaById(id);
            if (!referent.isPresent()) {
                return Collections.emptyList();
            }
            List<String> referrerIds = catalog.listReferencedBy(referent.get().schemaId());
            List<SubjectVersion> out = new ArrayList<>();
            for (String referrerId : referrerIds) {
                Optional<SchemaVersionEntity> ref = catalog.getSchemaBySchemaId(referrerId);
                if (!ref.isPresent() || isSchemaTombstoned(ref.get().schemaId())) {
                    continue;
                }
                String tableId = ref.get().tableId();
                int version = ref.get().version();
                for (KafkaSubjectBinding b : catalog.listKafkaSubjects()) {
                    if (tableId.equals(b.tableId()) && !isSubjectTombstoned(b.subject())) {
                        out.add(new SubjectVersion(b.subject(), version));
                    }
                }
            }
            // Stable, deterministic ordering: by subject, then version.
            out.sort(
                    (a, b) -> {
                        int s = a.subject().compareTo(b.subject());
                        if (s != 0) {
                            return s;
                        }
                        return Integer.compare(a.version(), b.version());
                    });
            return out;
        } catch (Exception e) {
            throw translate(e);
        }
    }

    /**
     * Look up the catalog UUID of {@code (subject, version)}. Returns empty when the binding,
     * version, or schema row is missing or tombstoned. Used by the SR HTTP layer to populate {@code
     * references} echoes by fetching forward edges off the catalog.
     */
    public Optional<String> schemaIdFor(String subject, int version) {
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent() || isSubjectTombstoned(subject)) {
                return Optional.empty();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            int target = version;
            if (target == -1) {
                Optional<RegisteredSchema> latest = latestForSubject(subject);
                if (!latest.isPresent()) {
                    return Optional.empty();
                }
                target = latest.get().version();
            }
            Optional<SchemaVersionEntity> ent =
                    catalog.getSchemaVersion(kafkaDatabase, topic, target);
            if (!ent.isPresent() || isSchemaTombstoned(ent.get().schemaId())) {
                return Optional.empty();
            }
            return Optional.of(ent.get().schemaId());
        } catch (Exception e) {
            throw translate(e);
        }
    }

    private void enforceCompatibilityOrThrow(
            String subject, String topic, String proposed, ReferenceResolver resolver)
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
        String subjectFormat = liveVersions.get(0).format();
        CompatibilityResult result =
                checkerFor(subjectFormat).check(proposed, priorTexts, level, resolver);
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
            String subject, String topic, String schemaText, List<ResolvedReference> refs)
            throws Exception {
        List<SchemaVersionEntity> all = catalog.listSchemaVersions(kafkaDatabase, topic);
        for (SchemaVersionEntity v : all) {
            if (schemaText.equals(v.schemaText()) && isSchemaTombstoned(v.schemaId())) {
                List<SchemaReference> priorRefs = catalog.listReferences(v.schemaId());
                if (!referenceListsEqualToCatalog(priorRefs, refs)) {
                    continue;
                }
                catalog.deleteSrConfig(KEY_TOMBSTONE_SCHEMA_PREFIX + v.schemaId());
                return Optional.of(
                        new RegisteredSchema(
                                v.srSchemaId(),
                                subject,
                                v.version(),
                                v.format(),
                                v.schemaText(),
                                refs(priorRefs)));
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

    /**
     * Refuse to hard-delete {@code schemaId} when at least one live referrer points at it. The 422
     * message enumerates every blocking referrer as a Kafka SR-shape {@code (subject, version)}
     * tuple — multi-binding referrers expand to one tuple per binding.
     */
    private void refuseIfReferenced(String schemaId, String subject, int version) throws Exception {
        List<String> referrerIds = catalog.listReferencedBy(schemaId);
        if (referrerIds.isEmpty()) {
            return;
        }
        List<String> blockers = new ArrayList<>();
        for (String referrerId : referrerIds) {
            Optional<SchemaVersionEntity> referrer = catalog.getSchemaBySchemaId(referrerId);
            if (!referrer.isPresent() || isSchemaTombstoned(referrer.get().schemaId())) {
                continue;
            }
            String tableId = referrer.get().tableId();
            int v = referrer.get().version();
            for (KafkaSubjectBinding b : catalog.listKafkaSubjects()) {
                if (tableId.equals(b.tableId()) && !isSubjectTombstoned(b.subject())) {
                    blockers.add(b.subject() + "/v" + v);
                }
            }
        }
        if (blockers.isEmpty()) {
            return;
        }
        Collections.sort(blockers);
        throw new SchemaRegistryException(
                SchemaRegistryException.Kind.UNPROCESSABLE_ENTITY,
                "Subject '"
                        + subject
                        + "' version "
                        + version
                        + " is still referenced by "
                        + blockers.size()
                        + " schemas: "
                        + blockers);
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
                            s.srSchemaId(),
                            /* subject unused by GET /schemas/ids */ "",
                            s.version(),
                            s.format(),
                            s.schemaText(),
                            refs(catalog.listReferences(s.schemaId()))));
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
     * Supported schema types advertised by {@code GET /schemas/types}. Reflects whatever
     * translators are registered in {@link FormatRegistry}; after Phase T-MF this includes {@code
     * AVRO}, {@code JSON}, and {@code PROTOBUF}.
     */
    public List<String> supportedTypes() {
        return FormatRegistry.instance().formatIds();
    }

    /**
     * Subjects that have bound any schema version sharing the Kafka SR global {@code id}. Backs
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
     * {@code (subject, version)} tuples for the Kafka SR schema id. Backs {@code GET
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
     * Resolve a specific version for a subject. {@code version == -1} is Kafka SR alias for
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
                            s.srSchemaId(),
                            subject,
                            s.version(),
                            s.format(),
                            s.schemaText(),
                            refs(catalog.listReferences(s.schemaId()))));
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
                                    v.srSchemaId(),
                                    subject,
                                    v.version(),
                                    v.format(),
                                    v.schemaText(),
                                    refs(catalog.listReferences(v.schemaId()))));
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
                                s.srSchemaId(),
                                subject,
                                s.version(),
                                s.format(),
                                s.schemaText(),
                                refs(catalog.listReferences(s.schemaId()))));
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
                            s.srSchemaId(),
                            subject,
                            s.version(),
                            s.format(),
                            s.schemaText(),
                            refs(catalog.listReferences(s.schemaId()))));
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

    /** Immutable snapshot of one registered schema (plus its references, when known). */
    public static final class RegisteredSchema {
        private final int id;
        private final String subject;
        private final int version;
        private final String format;
        private final String schema;
        private final List<SubjectReference> references;

        public RegisteredSchema(int id, String subject, int version, String format, String schema) {
            this(id, subject, version, format, schema, Collections.emptyList());
        }

        public RegisteredSchema(
                int id,
                String subject,
                int version,
                String format,
                String schema,
                List<SubjectReference> references) {
            this.id = id;
            this.subject = subject;
            this.version = version;
            this.format = format;
            this.schema = schema;
            this.references =
                    references == null
                            ? Collections.emptyList()
                            : Collections.unmodifiableList(new ArrayList<>(references));
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

        public List<SubjectReference> references() {
            return references;
        }
    }

    /**
     * Caller-supplied reference shape on the register / compatibility wire path: just {@code (name,
     * subject, version)}. Resolved against the catalog before any compat or write step runs.
     */
    public static final class RequestedReference {
        private final String name;
        private final String subject;
        private final int version;

        public RequestedReference(String name, String subject, int version) {
            this.name = name;
            this.subject = subject;
            this.version = version;
        }

        public String name() {
            return name;
        }

        public String subject() {
            return subject;
        }

        public int version() {
            return version;
        }
    }

    /** Kafka SR-shape echo of one reference on a read response. */
    public static final class SubjectReference {
        private final String name;
        private final String subject;
        private final int version;

        public SubjectReference(String name, String subject, int version) {
            this.name = name;
            this.subject = subject;
            this.version = version;
        }

        public String name() {
            return name;
        }

        public String subject() {
            return subject;
        }

        public int version() {
            return version;
        }
    }

    /** Internal resolved reference: pinned schema id + the referent's text. */
    private static final class ResolvedReference {
        final String name;
        final String subject;
        final int version;
        final String referencedSchemaId;
        final String referencedText;

        ResolvedReference(
                String name,
                String subject,
                int version,
                String referencedSchemaId,
                String referencedText) {
            this.name = name;
            this.subject = subject;
            this.version = version;
            this.referencedSchemaId = referencedSchemaId;
            this.referencedText = referencedText;
        }

        SubjectReference toSubjectReference() {
            return new SubjectReference(name, subject, version);
        }
    }

    /** Latest-version snapshot bundled with its bound references for fast-path comparison. */
    private static final class RegisteredSchemaWithRefs {
        private final RegisteredSchema schema;
        private final List<SchemaReference> references;

        RegisteredSchemaWithRefs(RegisteredSchema schema, List<SchemaReference> references) {
            this.schema = schema;
            this.references = references;
        }

        RegisteredSchema schema() {
            return schema;
        }

        List<SchemaReference> references() {
            return references;
        }
    }
}
