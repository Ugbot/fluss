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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.catalog.entities.ClientQuotaEntry;
import org.apache.fluss.catalog.entities.ClientQuotaFilter;
import org.apache.fluss.catalog.entities.GrantEntity;
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.NamespaceEntity;
import org.apache.fluss.catalog.entities.PrincipalEntity;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Fluss's catalog — the Polaris/Unity-style metadata service whose storage lives in Fluss PK tables
 * under the reserved {@code _catalog} database.
 *
 * <p>Consumed by HTTP-shaped projections (Kafka Schema Registry now, Iceberg REST Catalog and Flink
 * multi-format catalogs next) so that catalog semantics — namespaces, schema evolution history,
 * deterministic Confluent ids, eventual RBAC — live in one place, not spread across every wire
 * protocol Fluss speaks.
 *
 * <p>Phase C.1 scope: namespaces, tables, schema history, Kafka subject bindings, deterministic
 * Confluent id allocation. Phase C.2 adds {@link #grant}, {@link #revoke}, {@link #listGrants},
 * {@link #checkPrivilege}. See {@code dev-docs/design/0002-kafka-schema-registry-and-typed-
 * tables.md} and the structural plan for context.
 */
@PublicEvolving
public interface CatalogService {

    // --------------------------- Namespaces --------------------------- //

    NamespaceEntity createNamespace(
            @Nullable String parentName, String name, @Nullable String description)
            throws Exception;

    Optional<NamespaceEntity> getNamespace(String fullyQualifiedName) throws Exception;

    List<NamespaceEntity> listNamespaces(@Nullable String parentFullyQualifiedName)
            throws Exception;

    void dropNamespace(String fullyQualifiedName, boolean cascade) throws Exception;

    // --------------------------- Tables ------------------------------- //

    CatalogTableEntity createTable(
            String namespace,
            String name,
            String format,
            String backingRef,
            @Nullable String createdBy)
            throws Exception;

    Optional<CatalogTableEntity> getTable(String namespace, String name) throws Exception;

    List<CatalogTableEntity> listTables(String namespace) throws Exception;

    void dropTable(String namespace, String name) throws Exception;

    // --------------------------- Schema history ----------------------- //

    /**
     * Register a new schema version against an existing table. Returns the new version (monotone
     * per table). Schemas are append-only; previous versions are preserved and queryable via {@link
     * #listSchemaVersions}.
     */
    SchemaVersionEntity registerSchema(
            String namespace,
            String tableName,
            String format,
            String schemaText,
            @Nullable String registeredBy)
            throws Exception;

    Optional<SchemaVersionEntity> getSchemaVersion(String namespace, String tableName, int version)
            throws Exception;

    Optional<SchemaVersionEntity> getSchemaById(int confluentId) throws Exception;

    /** PK-lookup by schema id. Read-after-write consistent, unlike scans. */
    Optional<SchemaVersionEntity> getSchemaBySchemaId(String schemaId) throws Exception;

    /** PK-lookup by the internal catalog table id. Read-after-write consistent. */
    Optional<CatalogTableEntity> getTableById(String tableId) throws Exception;

    List<SchemaVersionEntity> listSchemaVersions(String namespace, String tableName)
            throws Exception;

    // --------------------------- Kafka subject bindings --------------- //

    /**
     * Bind a Confluent-style Kafka subject to a catalog table. Idempotent on {@code (subject,
     * namespace, tableName, keyOrValue)}.
     */
    KafkaSubjectBinding bindKafkaSubject(
            String subject,
            String namespace,
            String tableName,
            String keyOrValue,
            String namingStrategy)
            throws Exception;

    Optional<KafkaSubjectBinding> resolveKafkaSubject(String subject) throws Exception;

    List<KafkaSubjectBinding> listKafkaSubjects() throws Exception;

    /**
     * Hard-delete a Kafka subject binding. No-op when the subject has no binding. Used by the SR
     * {@code DELETE /subjects/{s}?permanent=true} endpoint; soft-delete is modelled as a tombstone
     * in the {@code _sr_config} KV table and does not call into this method.
     */
    void unbindKafkaSubject(String subject) throws Exception;

    /**
     * Hard-delete a single schema version by its internal schema id. Does NOT reclaim the
     * associated Confluent id reservation — Confluent SR semantics require ids remain stable across
     * the lifetime of the registry so that clients holding them keep dereferencing to "not found"
     * rather than colliding with a future registration. No-op when absent.
     */
    void deleteSchemaVersion(String schemaId) throws Exception;

    // --------------------------- Client quotas ------------------------ //

    /**
     * Upsert a single client-quota entry. {@code entityName} may be the empty string to target the
     * default entity. Phase I.3 (Path A) storage-only — this does not install a throttle.
     */
    ClientQuotaEntry upsertClientQuota(
            String entityType, String entityName, String quotaKey, double quotaValue)
            throws Exception;

    /** Delete a client-quota entry. No-op when the entry is absent. */
    void deleteClientQuota(String entityType, String entityName, String quotaKey) throws Exception;

    /** List every client-quota entry matching {@code filter}. */
    List<ClientQuotaEntry> listClientQuotas(ClientQuotaFilter filter) throws Exception;

    // --------------------------- SR config ---------------------------- //

    /**
     * Read the {@code _sr_config} value for {@code key}; {@link Optional#empty()} when unset. Keys
     * follow the {@code global_compatibility} / {@code subject_compatibility:<s>} / {@code
     * global_mode} / {@code subject_mode:<s>} convention.
     */
    Optional<org.apache.fluss.catalog.entities.SrConfigEntry> getSrConfig(String key)
            throws Exception;

    /** Upsert an SR config row. Idempotent on {@code (key, value)}. */
    org.apache.fluss.catalog.entities.SrConfigEntry setSrConfig(String key, String value)
            throws Exception;

    /** Delete an SR config row. No-op when absent. */
    void deleteSrConfig(String key) throws Exception;

    // --------------------------- Principals --------------------------- //

    /**
     * Create a principal record if it doesn't already exist; return the stored entity either way.
     * Projections call this to lazily materialise the principal they're acting as (typically the
     * HTTP caller identity or Fluss's {@link org.apache.fluss.security.acl.FlussPrincipal}).
     */
    PrincipalEntity ensurePrincipal(String name, String type) throws Exception;

    Optional<PrincipalEntity> getPrincipal(String name) throws Exception;

    List<PrincipalEntity> listPrincipals() throws Exception;

    // --------------------------- Grants ------------------------------- //

    /**
     * Grant {@code privilege} to {@code principalName} on the given entity. {@code entityKind} is
     * one of {@link GrantEntity#KIND_CATALOG} / {@code NAMESPACE} / {@code TABLE} / {@code SCHEMA};
     * for {@code CATALOG}, {@code entityId} must be {@link GrantEntity#CATALOG_WILDCARD}.
     * Idempotent on {@code (principal, entityKind, entityId, privilege)}.
     */
    GrantEntity grant(
            String principalName,
            String entityKind,
            String entityId,
            String privilege,
            String grantedBy)
            throws Exception;

    /** Revoke a previously-granted privilege. No-op if the grant is missing. */
    void revoke(String principalName, String entityKind, String entityId, String privilege)
            throws Exception;

    /** Return every grant currently held by {@code principalName}. */
    List<GrantEntity> listGrantsForPrincipal(String principalName) throws Exception;

    /**
     * Authorisation check: does {@code principalName} hold {@code privilege} on {@code (entityKind,
     * entityId)} — directly or via a {@code CATALOG} wildcard grant?
     *
     * <p>Convenience for SR and other projections: namespace/table id resolution lives in the
     * projection; this entry-point takes the resolved kind+id only. Principals that don't exist in
     * {@code _principals} yield {@code false}.
     */
    boolean checkPrivilege(
            String principalName, String entityKind, String entityId, String privilege)
            throws Exception;
}
