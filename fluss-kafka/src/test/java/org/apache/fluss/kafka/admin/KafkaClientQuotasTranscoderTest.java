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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.catalog.entities.ClientQuotaEntry;
import org.apache.fluss.catalog.entities.ClientQuotaFilter;
import org.apache.fluss.catalog.entities.GrantEntity;
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.NamespaceEntity;
import org.apache.fluss.catalog.entities.PrincipalEntity;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;

import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntityData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.EntryData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData.OpData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData.ComponentData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link KafkaClientQuotasTranscoder} end-to-end against an in-memory {@link
 * CatalogService} stub. Avoids the Fluss catalog's bootstrap-readiness handshake so the Phase I.3
 * wire contract is validated without requiring a running cluster — the transcoder's {@link
 * DescribeClientQuotasResponseData} / {@link AlterClientQuotasResponseData} output is the Kafka
 * boundary the tests pin.
 */
class KafkaClientQuotasTranscoderTest {

    @Test
    void alterThenDescribeRoundTripsUserQuota() {
        InMemoryClientQuotasCatalog catalog = new InMemoryClientQuotasCatalog();
        KafkaClientQuotasTranscoder transcoder = new KafkaClientQuotasTranscoder(catalog);

        // Alter: user=alice, producer_byte_rate=1,000,000
        AlterClientQuotasRequestData alter = new AlterClientQuotasRequestData();
        alter.setEntries(
                Collections.singletonList(
                        new EntryData()
                                .setEntity(
                                        Collections.singletonList(
                                                new EntityData()
                                                        .setEntityType("user")
                                                        .setEntityName("alice")))
                                .setOps(
                                        Collections.singletonList(
                                                new OpData()
                                                        .setKey("producer_byte_rate")
                                                        .setValue(1_000_000.0)
                                                        .setRemove(false)))));
        AlterClientQuotasResponseData alterResp = transcoder.alterClientQuotas(alter);
        assertThat(alterResp.entries()).hasSize(1);
        assertThat(alterResp.entries().get(0).errorCode()).isEqualTo(Errors.NONE.code());

        // Describe: filter user=alice
        DescribeClientQuotasRequestData desc = new DescribeClientQuotasRequestData();
        desc.setStrict(false);
        desc.setComponents(
                Collections.singletonList(
                        new ComponentData()
                                .setEntityType("user")
                                .setMatch("alice")
                                .setMatchType((byte) 0))); // 0 = EXACT
        DescribeClientQuotasResponseData descResp = transcoder.describeClientQuotas(desc);
        assertThat(descResp.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(descResp.entries()).hasSize(1);
        assertThat(descResp.entries().get(0).entity()).hasSize(1);
        assertThat(descResp.entries().get(0).entity().get(0).entityType()).isEqualTo("user");
        assertThat(descResp.entries().get(0).entity().get(0).entityName()).isEqualTo("alice");
        assertThat(descResp.entries().get(0).values()).hasSize(1);
        assertThat(descResp.entries().get(0).values().get(0).key()).isEqualTo("producer_byte_rate");
        assertThat(descResp.entries().get(0).values().get(0).value()).isEqualTo(1_000_000.0);
    }

    @Test
    void alterWithRemoveDeletesTheQuota() {
        InMemoryClientQuotasCatalog catalog = new InMemoryClientQuotasCatalog();
        KafkaClientQuotasTranscoder transcoder = new KafkaClientQuotasTranscoder(catalog);

        // Set.
        transcoder.alterClientQuotas(
                alterReq("user", "bob", "consumer_byte_rate", 500_000.0, false));
        assertThat(catalog.rows.values()).hasSize(1);

        // Describe confirms.
        DescribeClientQuotasResponseData before =
                transcoder.describeClientQuotas(descFilterUser("bob"));
        assertThat(before.entries()).hasSize(1);

        // Remove.
        AlterClientQuotasResponseData removeResp =
                transcoder.alterClientQuotas(
                        alterReq("user", "bob", "consumer_byte_rate", 0.0, true));
        assertThat(removeResp.entries().get(0).errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(catalog.rows.values()).isEmpty();

        // Describe confirms gone.
        DescribeClientQuotasResponseData after =
                transcoder.describeClientQuotas(descFilterUser("bob"));
        assertThat(after.entries()).isEmpty();
    }

    @Test
    void defaultEntityIsReturnedAsNullEntityName() {
        InMemoryClientQuotasCatalog catalog = new InMemoryClientQuotasCatalog();
        KafkaClientQuotasTranscoder transcoder = new KafkaClientQuotasTranscoder(catalog);

        // Alter with entityName=null on the wire => default entity (stored as empty string).
        AlterClientQuotasRequestData alter = new AlterClientQuotasRequestData();
        alter.setEntries(
                Collections.singletonList(
                        new EntryData()
                                .setEntity(
                                        Collections.singletonList(
                                                new EntityData()
                                                        .setEntityType("user")
                                                        .setEntityName(null)))
                                .setOps(
                                        Collections.singletonList(
                                                new OpData()
                                                        .setKey("producer_byte_rate")
                                                        .setValue(777.0)
                                                        .setRemove(false)))));
        transcoder.alterClientQuotas(alter);
        assertThat(catalog.rows.values()).hasSize(1);
        assertThat(catalog.rows.values().iterator().next().entityName()).isEmpty();

        // Describe with matchType=DEFAULT.
        DescribeClientQuotasRequestData desc = new DescribeClientQuotasRequestData();
        desc.setStrict(false);
        desc.setComponents(
                Collections.singletonList(
                        new ComponentData()
                                .setEntityType("user")
                                .setMatchType((byte) 1))); // DEFAULT
        DescribeClientQuotasResponseData resp = transcoder.describeClientQuotas(desc);
        assertThat(resp.entries()).hasSize(1);
        // Null entityName on the wire == default entity.
        assertThat(resp.entries().get(0).entity().get(0).entityName()).isNull();
    }

    @Test
    void ipEntityTypeIsAcceptedAndStored() {
        InMemoryClientQuotasCatalog catalog = new InMemoryClientQuotasCatalog();
        KafkaClientQuotasTranscoder transcoder = new KafkaClientQuotasTranscoder(catalog);

        transcoder.alterClientQuotas(
                alterReq("ip", "10.0.0.1", "connection_creation_rate", 50.0, false));
        DescribeClientQuotasResponseData resp =
                transcoder.describeClientQuotas(descFilterExact("ip", "10.0.0.1"));
        assertThat(resp.entries()).hasSize(1);
        assertThat(resp.entries().get(0).entity().get(0).entityType()).isEqualTo("ip");
        assertThat(resp.entries().get(0).entity().get(0).entityName()).isEqualTo("10.0.0.1");
        assertThat(resp.entries().get(0).values().get(0).key())
                .isEqualTo("connection_creation_rate");
        assertThat(resp.entries().get(0).values().get(0).value()).isEqualTo(50.0);
    }

    private static AlterClientQuotasRequestData alterReq(
            String entityType,
            @Nullable String entityName,
            String key,
            double value,
            boolean remove) {
        AlterClientQuotasRequestData r = new AlterClientQuotasRequestData();
        r.setEntries(
                Collections.singletonList(
                        new EntryData()
                                .setEntity(
                                        Collections.singletonList(
                                                new EntityData()
                                                        .setEntityType(entityType)
                                                        .setEntityName(entityName)))
                                .setOps(
                                        Collections.singletonList(
                                                new OpData()
                                                        .setKey(key)
                                                        .setValue(value)
                                                        .setRemove(remove)))));
        return r;
    }

    private static DescribeClientQuotasRequestData descFilterUser(String name) {
        return descFilterExact("user", name);
    }

    private static DescribeClientQuotasRequestData descFilterExact(String type, String name) {
        DescribeClientQuotasRequestData d = new DescribeClientQuotasRequestData();
        d.setStrict(false);
        d.setComponents(
                Collections.singletonList(
                        new ComponentData()
                                .setEntityType(type)
                                .setMatch(name)
                                .setMatchType((byte) 0)));
        return d;
    }

    /** In-memory CatalogService stub — only the three client-quota methods are implemented. */
    private static final class InMemoryClientQuotasCatalog implements CatalogService {

        /** PK = entityType|entityName|quotaKey. */
        final Map<String, ClientQuotaEntry> rows = new HashMap<>();

        private static String key(String type, String name, String qk) {
            return type + "|" + name + "|" + qk;
        }

        @Override
        public ClientQuotaEntry upsertClientQuota(
                String entityType, String entityName, String quotaKey, double quotaValue) {
            ClientQuotaEntry e =
                    new ClientQuotaEntry(
                            entityType,
                            entityName,
                            quotaKey,
                            quotaValue,
                            System.currentTimeMillis());
            rows.put(key(entityType, entityName, quotaKey), e);
            return e;
        }

        @Override
        public void deleteClientQuota(String entityType, String entityName, String quotaKey) {
            rows.remove(key(entityType, entityName, quotaKey));
        }

        @Override
        public List<ClientQuotaEntry> listClientQuotas(ClientQuotaFilter filter) {
            List<ClientQuotaEntry> all = new ArrayList<>(rows.values());
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
            if (filter.strict() && filter.components().size() != 1) {
                return false;
            }
            for (ClientQuotaFilter.Component c : filter.components()) {
                if (!c.entityType().equals(entry.entityType())) {
                    if (filter.strict()) {
                        return false;
                    }
                    continue;
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
                return true;
            }
            return false;
        }

        // --- unused CatalogService methods (not exercised by this test) ---

        @Override
        public NamespaceEntity createNamespace(String p, String n, String d) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<NamespaceEntity> getNamespace(String n) {
            return Optional.empty();
        }

        @Override
        public List<NamespaceEntity> listNamespaces(String p) {
            return Collections.emptyList();
        }

        @Override
        public void dropNamespace(String n, boolean c) {}

        @Override
        public CatalogTableEntity createTable(
                String ns, String n, String f, String bref, String createdBy) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<CatalogTableEntity> getTable(String ns, String n) {
            return Optional.empty();
        }

        @Override
        public List<CatalogTableEntity> listTables(String ns) {
            return Collections.emptyList();
        }

        @Override
        public void dropTable(String ns, String n) {}

        @Override
        public SchemaVersionEntity registerSchema(
                String ns, String tn, String f, String st, String reg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SchemaVersionEntity> getSchemaVersion(String ns, String tn, int v) {
            return Optional.empty();
        }

        @Override
        public Optional<SchemaVersionEntity> getSchemaById(int c) {
            return Optional.empty();
        }

        @Override
        public Optional<SchemaVersionEntity> getSchemaBySchemaId(String id) {
            return Optional.empty();
        }

        @Override
        public Optional<CatalogTableEntity> getTableById(String id) {
            return Optional.empty();
        }

        @Override
        public List<SchemaVersionEntity> listSchemaVersions(String ns, String tn) {
            return Collections.emptyList();
        }

        @Override
        public KafkaSubjectBinding bindKafkaSubject(
                String s, String ns, String tn, String kv, String nm) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<KafkaSubjectBinding> resolveKafkaSubject(String s) {
            return Optional.empty();
        }

        @Override
        public List<KafkaSubjectBinding> listKafkaSubjects() {
            return Collections.emptyList();
        }

        @Override
        public void unbindKafkaSubject(String subject) {}

        @Override
        public void deleteSchemaVersion(String schemaId) {}

        @Override
        public PrincipalEntity ensurePrincipal(String n, String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<PrincipalEntity> getPrincipal(String n) {
            return Optional.empty();
        }

        @Override
        public List<PrincipalEntity> listPrincipals() {
            return Collections.emptyList();
        }

        @Override
        public GrantEntity grant(String p, String k, String id, String pr, String gb) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revoke(String p, String k, String id, String pr) {}

        @Override
        public List<GrantEntity> listGrantsForPrincipal(String p) {
            return Collections.emptyList();
        }

        @Override
        public boolean checkPrivilege(String p, String k, String id, String pr) {
            return false;
        }

        @Override
        public Optional<org.apache.fluss.catalog.entities.SrConfigEntry> getSrConfig(String key) {
            return Optional.empty();
        }

        @Override
        public org.apache.fluss.catalog.entities.SrConfigEntry setSrConfig(
                String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSrConfig(String key) {}

        @Override
        public void deleteReferences(String referrerSchemaId) {}

        @Override
        public void bindReferences(
                String referrerSchemaId,
                java.util.List<org.apache.fluss.catalog.entities.SchemaReference> refs) {}

        @Override
        public java.util.List<org.apache.fluss.catalog.entities.SchemaReference> listReferences(
                String referrerSchemaId) {
            return Collections.emptyList();
        }

        @Override
        public java.util.List<String> listReferencedBy(String referencedSchemaId) {
            return Collections.emptyList();
        }

        @Override
        public org.apache.fluss.catalog.entities.KafkaTxnStateEntity upsertKafkaTxnState(
                org.apache.fluss.catalog.entities.KafkaTxnStateEntity entity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<org.apache.fluss.catalog.entities.KafkaTxnStateEntity> getKafkaTxnState(
                String transactionalId) {
            return Optional.empty();
        }

        @Override
        public List<org.apache.fluss.catalog.entities.KafkaTxnStateEntity> listKafkaTxnStates() {
            return Collections.emptyList();
        }

        @Override
        public void deleteKafkaTxnState(String transactionalId) {}

        @Override
        public org.apache.fluss.catalog.entities.KafkaProducerIdEntity allocateProducerId(
                String transactionalId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<org.apache.fluss.catalog.entities.KafkaProducerIdEntity> getKafkaProducerId(
                long producerId) {
            return Optional.empty();
        }

        @Override
        public List<org.apache.fluss.catalog.entities.KafkaProducerIdEntity>
                listKafkaProducerIds() {
            return Collections.emptyList();
        }
    }
}
