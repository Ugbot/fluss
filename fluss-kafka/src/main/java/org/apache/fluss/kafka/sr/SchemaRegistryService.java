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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.TablePropertyChanges;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Orchestrates the Phase A1 Schema Registry endpoints against Fluss table metadata. Stateless
 * beyond its collaborators — every call re-reads the underlying {@link MetadataManager}.
 */
@Internal
public final class SchemaRegistryService {

    private final MetadataManager metadataManager;
    private final ConfluentIdAllocator idAllocator;
    private final String kafkaDatabase;

    public SchemaRegistryService(
            MetadataManager metadataManager,
            ConfluentIdAllocator idAllocator,
            String kafkaDatabase) {
        this.metadataManager = metadataManager;
        this.idAllocator = idAllocator;
        this.kafkaDatabase = kafkaDatabase;
    }

    /** Global default compatibility — hardcoded to BACKWARD for Phase A1 per design 0002. */
    public String defaultCompatibility() {
        return "BACKWARD";
    }

    /**
     * Register (or idempotently re-register) the Avro schema bound to {@code subject}.
     *
     * @return the Confluent global id — stable for identical {@code (subject, schema)} submissions.
     */
    public int register(String subject, String avroSchema) {
        if (avroSchema == null || avroSchema.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "schema body is required");
        }
        String topic = SubjectResolver.topicFromValueSubject(subject);
        TableInfo tableInfo = loadTable(topic, subject);

        Map<String, String> customProps = tableInfo.getCustomProperties().toMap();
        String existingSubject = customProps.get(SrTableProperties.SUBJECT);
        String existingSchema = customProps.get(SrTableProperties.AVRO_SCHEMA);
        String existingId = customProps.get(SrTableProperties.CONFLUENT_ID);
        if (subject.equals(existingSubject)
                && avroSchema.equals(existingSchema)
                && existingId != null) {
            return Integer.parseInt(existingId);
        }

        int schemaVersion = 1; // Phase A1: one version per table.
        int id =
                idAllocator.allocate(
                        tableInfo.getTableId(), schemaVersion, SrTableProperties.FORMAT_AVRO);

        TablePropertyChanges.Builder changes = TablePropertyChanges.builder();
        changes.setCustomProperty(SrTableProperties.SUBJECT, subject);
        changes.setCustomProperty(SrTableProperties.FORMAT, SrTableProperties.FORMAT_AVRO);
        changes.setCustomProperty(SrTableProperties.AVRO_SCHEMA, avroSchema);
        changes.setCustomProperty(
                SrTableProperties.SCHEMA_VERSION, Integer.toString(schemaVersion));
        changes.setCustomProperty(SrTableProperties.CONFLUENT_ID, Integer.toString(id));
        metadataManager.alterTableProperties(
                tableInfo.getTablePath(),
                Collections.emptyList(),
                changes.build(),
                false,
                FlussPrincipal.ANONYMOUS);
        return id;
    }

    /** Look up a schema by its Confluent id, following the reservation pointer into a table. */
    public Optional<RegisteredSchema> schemaById(int id) {
        Optional<ConfluentIdAllocator.Reservation> reservation = idAllocator.lookup(id);
        if (!reservation.isPresent()) {
            return Optional.empty();
        }
        // Phase A1 rule: a reservation always points to exactly one table in the kafka database.
        // Linear scan of that database is acceptable for the MVP; Phase A2 adds a
        // tableId→tablePath index.
        for (String tableName : metadataManager.listTables(kafkaDatabase)) {
            TableInfo ti;
            try {
                ti = metadataManager.getTable(new TablePath(kafkaDatabase, tableName));
            } catch (TableNotExistException gone) {
                continue;
            }
            if (ti.getTableId() != reservation.get().tableId()) {
                continue;
            }
            Map<String, String> custom = ti.getCustomProperties().toMap();
            String storedSchema = custom.get(SrTableProperties.AVRO_SCHEMA);
            String storedSubject = custom.get(SrTableProperties.SUBJECT);
            if (storedSchema == null || storedSubject == null) {
                continue;
            }
            return Optional.of(
                    new RegisteredSchema(
                            id,
                            storedSubject,
                            reservation.get().schemaVersion(),
                            reservation.get().format(),
                            storedSchema));
        }
        return Optional.empty();
    }

    /** Names of every Kafka-bound table that carries an SR subject binding. */
    public List<String> listSubjects() {
        List<String> tableNames = metadataManager.listTables(kafkaDatabase);
        List<String> subjects = new ArrayList<>(tableNames.size());
        for (String tableName : tableNames) {
            try {
                TableInfo ti = metadataManager.getTable(new TablePath(kafkaDatabase, tableName));
                String subject = ti.getCustomProperties().toMap().get(SrTableProperties.SUBJECT);
                if (subject != null) {
                    subjects.add(subject);
                }
            } catch (TableNotExistException gone) {
                // race with delete; skip
            }
        }
        Collections.sort(subjects);
        return subjects;
    }

    /** Latest-version view for Phase A1 (there is only one version per subject). */
    public Optional<RegisteredSchema> latestForSubject(String subject) {
        String topic = SubjectResolver.topicFromValueSubject(subject);
        TableInfo ti;
        try {
            ti = metadataManager.getTable(new TablePath(kafkaDatabase, topic));
        } catch (TableNotExistException gone) {
            return Optional.empty();
        }
        Map<String, String> custom = ti.getCustomProperties().toMap();
        String storedSubject = custom.get(SrTableProperties.SUBJECT);
        String storedSchema = custom.get(SrTableProperties.AVRO_SCHEMA);
        String storedId = custom.get(SrTableProperties.CONFLUENT_ID);
        String storedVersion = custom.get(SrTableProperties.SCHEMA_VERSION);
        if (storedSubject == null || storedSchema == null || storedId == null) {
            return Optional.empty();
        }
        return Optional.of(
                new RegisteredSchema(
                        Integer.parseInt(storedId),
                        storedSubject,
                        storedVersion == null ? 1 : Integer.parseInt(storedVersion),
                        SrTableProperties.FORMAT_AVRO,
                        storedSchema));
    }

    private TableInfo loadTable(String topic, String subject) {
        TablePath path = new TablePath(kafkaDatabase, topic);
        try {
            return metadataManager.getTable(path);
        } catch (TableNotExistException gone) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.NOT_FOUND,
                    "Subject "
                            + subject
                            + " requires a pre-existing Kafka topic ("
                            + path
                            + "); auto-create arrives in Phase A2.");
        }
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
