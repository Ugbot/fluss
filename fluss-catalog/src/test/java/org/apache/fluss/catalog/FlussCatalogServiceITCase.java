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

import org.apache.fluss.catalog.entities.CatalogTableEntity;
import org.apache.fluss.catalog.entities.GrantEntity;
import org.apache.fluss.catalog.entities.KafkaSubjectBinding;
import org.apache.fluss.catalog.entities.NamespaceEntity;
import org.apache.fluss.catalog.entities.PrincipalEntity;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end IT for {@link FlussCatalogService}. Spins up a real Fluss cluster and exercises
 * namespace CRUD, multi-format tables, append-only schema history, Kafka subject bindings, and
 * deterministic Confluent id allocation.
 */
class FlussCatalogServiceITCase {

    private static final String FLUSS_LISTENER = "FLUSS";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners(FLUSS_LISTENER + "://localhost:0")
                    .setTabletServerListeners(FLUSS_LISTENER + "://localhost:0")
                    .build();

    private Connection connection;
    private FlussCatalogService catalog;

    @BeforeEach
    void openCatalog() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList(bootstrap()));
        connection = ConnectionFactory.createConnection(conf);
        catalog = new FlussCatalogService(connection);
    }

    @AfterEach
    void closeCatalog() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void namespaceRoundTrip() throws Exception {
        NamespaceEntity ns = catalog.createNamespace(null, "sales_ns" + suffix(), "Sales tables");
        Optional<NamespaceEntity> fetched = catalog.getNamespace(ns.name());
        assertThat(fetched).isPresent();
        assertThat(fetched.get().namespaceId()).isEqualTo(ns.namespaceId());
        assertThat(fetched.get().description()).isEqualTo("Sales tables");

        assertThatThrownBy(() -> catalog.createNamespace(null, ns.name(), null))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("namespace exists");
    }

    @Test
    void nestedNamespaces() throws Exception {
        String top = "top" + suffix();
        NamespaceEntity parent = catalog.createNamespace(null, top, null);
        NamespaceEntity child = catalog.createNamespace(top, "child", null);

        Optional<NamespaceEntity> byFqn = catalog.getNamespace(top + ".child");
        assertThat(byFqn).isPresent();
        assertThat(byFqn.get().namespaceId()).isEqualTo(child.namespaceId());
        assertThat(byFqn.get().parentId()).isEqualTo(parent.namespaceId());

        List<NamespaceEntity> children = catalog.listNamespaces(top);
        assertThat(children).hasSize(1);
        assertThat(children.get(0).name()).isEqualTo("child");
    }

    @Test
    void multiFormatTables() throws Exception {
        String ns = "ns" + suffix();
        catalog.createNamespace(null, ns, null);

        CatalogTableEntity flussTable =
                catalog.createTable(ns, "orders", "FLUSS", "kafka.orders", "alice");
        CatalogTableEntity icebergTable =
                catalog.createTable(ns, "metrics", "ICEBERG", "s3://lake/metrics", "bob");

        List<CatalogTableEntity> tables = catalog.listTables(ns);
        assertThat(tables).hasSize(2);
        assertThat(tables).extracting(CatalogTableEntity::format).contains("FLUSS", "ICEBERG");

        assertThat(catalog.getTable(ns, "orders"))
                .map(CatalogTableEntity::tableId)
                .contains(flussTable.tableId());
        assertThat(catalog.getTable(ns, "metrics"))
                .map(CatalogTableEntity::backingRef)
                .contains("s3://lake/metrics");

        // cascade drop clears both tables.
        catalog.dropNamespace(ns, true);
        assertThat(catalog.getNamespace(ns)).isEmpty();
    }

    @Test
    void schemaHistoryIsAppendOnly() throws Exception {
        String ns = "ns" + suffix();
        catalog.createNamespace(null, ns, null);
        catalog.createTable(ns, "orders", "FLUSS", "kafka.orders", null);

        SchemaVersionEntity v1 = catalog.registerSchema(ns, "orders", "AVRO", "{\"v\":1}", "alice");
        SchemaVersionEntity v2 = catalog.registerSchema(ns, "orders", "AVRO", "{\"v\":2}", "alice");

        assertThat(v1.version()).isEqualTo(1);
        assertThat(v2.version()).isEqualTo(2);
        assertThat(v1.confluentId()).isNotEqualTo(v2.confluentId());

        // idempotent re-register with identical text returns the same row.
        SchemaVersionEntity v1Again =
                catalog.registerSchema(ns, "orders", "AVRO", "{\"v\":1}", "alice");
        assertThat(v1Again.schemaId()).isEqualTo(v1.schemaId());

        List<SchemaVersionEntity> all = catalog.listSchemaVersions(ns, "orders");
        assertThat(all).hasSize(2);
        assertThat(all).extracting(SchemaVersionEntity::version).containsExactly(1, 2);

        // Confluent-id lookup returns the exact same row.
        assertThat(catalog.getSchemaById(v2.confluentId()))
                .map(SchemaVersionEntity::schemaText)
                .contains("{\"v\":2}");
    }

    @Test
    void kafkaSubjectBindingsAreListable() throws Exception {
        String ns = "ns" + suffix();
        catalog.createNamespace(null, ns, null);
        catalog.createTable(ns, "orders", "FLUSS", "kafka.orders", null);

        catalog.bindKafkaSubject("orders-value", ns, "orders", "value", "TopicNameStrategy");
        Optional<KafkaSubjectBinding> resolved = catalog.resolveKafkaSubject("orders-value");
        assertThat(resolved).isPresent();
        assertThat(resolved.get().keyOrValue()).isEqualTo("value");

        List<KafkaSubjectBinding> all = catalog.listKafkaSubjects();
        assertThat(all).extracting(KafkaSubjectBinding::subject).contains("orders-value");
    }

    @Test
    void rbacGrantAndCheck() throws Exception {
        String ns = "ns" + suffix();
        NamespaceEntity nsEntity = catalog.createNamespace(null, ns, null);

        // Unknown principal → no privilege.
        assertThat(
                        catalog.checkPrivilege(
                                "alice",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_WRITE))
                .isFalse();

        // Grant WRITE on the specific namespace — idempotent.
        GrantEntity first =
                catalog.grant(
                        "alice",
                        GrantEntity.KIND_NAMESPACE,
                        nsEntity.namespaceId(),
                        GrantEntity.PRIVILEGE_WRITE,
                        "admin");
        GrantEntity second =
                catalog.grant(
                        "alice",
                        GrantEntity.KIND_NAMESPACE,
                        nsEntity.namespaceId(),
                        GrantEntity.PRIVILEGE_WRITE,
                        "admin");
        assertThat(first.grantId()).isEqualTo(second.grantId());

        assertThat(
                        catalog.checkPrivilege(
                                "alice",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_WRITE))
                .isTrue();
        // Different privilege — denied.
        assertThat(
                        catalog.checkPrivilege(
                                "alice",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_DROP))
                .isFalse();

        // Principal entity materialised.
        Optional<PrincipalEntity> alice = catalog.getPrincipal("alice");
        assertThat(alice).isPresent();
        assertThat(alice.get().name()).isEqualTo("alice");

        // Revoke and re-check.
        catalog.revoke(
                "alice",
                GrantEntity.KIND_NAMESPACE,
                nsEntity.namespaceId(),
                GrantEntity.PRIVILEGE_WRITE);
        assertThat(
                        catalog.checkPrivilege(
                                "alice",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_WRITE))
                .isFalse();
    }

    @Test
    void rbacCatalogWildcardGrant() throws Exception {
        String ns = "ns" + suffix();
        NamespaceEntity nsEntity = catalog.createNamespace(null, ns, null);

        // Catalog-wide READ for bob.
        catalog.grant(
                "bob",
                GrantEntity.KIND_CATALOG,
                GrantEntity.CATALOG_WILDCARD,
                GrantEntity.PRIVILEGE_READ,
                "admin");
        // Applies to any specific entity.
        assertThat(
                        catalog.checkPrivilege(
                                "bob",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_READ))
                .isTrue();
        assertThat(
                        catalog.checkPrivilege(
                                "bob",
                                GrantEntity.KIND_TABLE,
                                "some-random-table-uuid",
                                GrantEntity.PRIVILEGE_READ))
                .isTrue();
        // Still scoped to the READ privilege — WRITE is not granted.
        assertThat(
                        catalog.checkPrivilege(
                                "bob",
                                GrantEntity.KIND_NAMESPACE,
                                nsEntity.namespaceId(),
                                GrantEntity.PRIVILEGE_WRITE))
                .isFalse();

        assertThat(catalog.listGrantsForPrincipal("bob")).hasSize(1);
        assertThat(catalog.listGrantsForPrincipal("nobody")).isEmpty();
    }

    private static String suffix() {
        return "_" + System.nanoTime();
    }

    private static String bootstrap() {
        StringBuilder b = new StringBuilder();
        List<ServerNode> nodes = new ArrayList<>(CLUSTER.getTabletServerNodes(FLUSS_LISTENER));
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }
        return b.toString();
    }
}
