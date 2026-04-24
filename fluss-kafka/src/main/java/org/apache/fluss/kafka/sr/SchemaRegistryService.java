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
import org.apache.fluss.metadata.TablePath;
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
     * Resolve the caller principal. Until authentication (SASL, HTTP forwarded-headers) lands every
     * request is {@link PrincipalEntity#ANONYMOUS}. A single point to rewire once auth arrives —
     * none of the call sites need to change.
     */
    private String callerPrincipal() {
        return PrincipalEntity.ANONYMOUS;
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
                        SchemaRegistryException.Kind.UNSUPPORTED,
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
            // Idempotent fast path: if the subject is already bound and the latest schema text
            // matches, return the existing Confluent id without appending a new version. This
            // avoids relying on catalog-side scan consistency for read-after-write semantics.
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

    public Optional<RegisteredSchema> schemaById(int id) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            return catalog.getSchemaById(id)
                    .map(
                            s ->
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

    /** Version numbers (1-based, monotone) registered against a subject, sorted ascending. */
    public List<Integer> listVersions(String subject) {
        authorize(GrantEntity.PRIVILEGE_READ);
        try {
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent()) {
                return Collections.emptyList();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> versions = catalog.listSchemaVersions(kafkaDatabase, topic);
            List<Integer> out = new ArrayList<>(versions.size());
            for (SchemaVersionEntity v : versions) {
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
            if (!binding.isPresent()) {
                return Optional.empty();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            Optional<SchemaVersionEntity> schema =
                    catalog.getSchemaVersion(kafkaDatabase, topic, version);
            if (!schema.isPresent()) {
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
            if (!binding.isPresent()) {
                return Optional.empty();
            }
            String topic = SubjectResolver.topicFromValueSubject(subject);
            List<SchemaVersionEntity> versions = catalog.listSchemaVersions(kafkaDatabase, topic);
            for (SchemaVersionEntity v : versions) {
                if (schemaText.equals(v.schemaText())) {
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
            // PK lookup (read-after-write consistent); no scans involved.
            Optional<KafkaSubjectBinding> binding = catalog.resolveKafkaSubject(subject);
            if (!binding.isPresent()) {
                return Optional.empty();
            }
            Optional<org.apache.fluss.catalog.entities.CatalogTableEntity> table =
                    catalog.getTableById(binding.get().tableId());
            if (!table.isPresent() || table.get().currentSchemaId() == null) {
                return Optional.empty();
            }
            Optional<SchemaVersionEntity> schema =
                    catalog.getSchemaBySchemaId(table.get().currentSchemaId());
            if (!schema.isPresent()) {
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
