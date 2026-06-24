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

package org.apache.fluss.kafka;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase I.3 — end-to-end {@code DescribeClientQuotas} / {@code AlterClientQuotas} test driving an
 * unmodified {@code kafka-clients} {@link Admin} through the Fluss Kafka bolt-on. The storage
 * backend is the shipped {@code _catalog._client_quotas} PK system table (see {@code
 * FlussCatalogService.upsertClientQuota / listClientQuotas}); this test pins the wire round-trip
 * and durability across a tablet-server restart (Path A — accept-and-store only, no enforcement).
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>Upsert + describe round-trip for a user entity — value echoes back.
 *   <li>Default entity (Kafka wire null name) — upsert with {@code null}, describe via {@code
 *       ofDefaultEntity}, confirm returned entity-name is {@code null} and value survives.
 *   <li>Remove-via-null-value — {@link ClientQuotaAlteration.Op} with {@code value = null} deletes
 *       the row; default-entity row from the prior step is unaffected.
 *   <li>Persistence across tablet-server restart — upsert, restart the tablet server hosting the
 *       catalog Connection, re-describe, the quota is still there.
 *   <li>{@code client-id} entity type — upsert + describe round-trips via {@code ofEntity}.
 * </ul>
 */
class KafkaClientQuotasAdminITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String FLUSS_LISTENER = "FLUSS";
    private static final String KAFKA_DATABASE = "kafka";
    // Single tablet server so the catalog Connection unambiguously targets "this" broker; the
    // restart scenario below relies on restartTabletServer(0) tearing down the only server.
    private static final int NUM_TABLET_SERVERS = 1;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners(FLUSS_LISTENER + "://localhost:0")
                    .setTabletServerListeners(
                            FLUSS_LISTENER + "://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

    private static Admin admin;

    @BeforeAll
    static void createAdmin() {
        admin = KafkaAdminClient.create(adminProps());
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close();
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void alterThenDescribeRoundTripsUserQuota() throws Exception {
        // Random suffix so the test is re-runnable even if a prior test-run left rows behind (the
        // catalog is persisted under the cluster's shared temp dir).
        String user = "alice_" + System.nanoTime();
        ClientQuotaEntity entity =
                new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, user));
        ClientQuotaAlteration.Op op = new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0);
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(entity, Collections.singletonList(op))))
                .all()
                .get();

        ClientQuotaFilter filter =
                ClientQuotaFilter.containsOnly(
                        Collections.singletonList(
                                ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user)));
        Map<ClientQuotaEntity, Map<String, Double>> rows =
                admin.describeClientQuotas(filter).entities().get();
        assertThat(rows).hasSize(1);
        Map.Entry<ClientQuotaEntity, Map<String, Double>> row = rows.entrySet().iterator().next();
        assertThat(row.getKey().entries()).containsExactly(entry(ClientQuotaEntity.USER, user));
        assertThat(row.getValue()).containsExactly(entry("producer_byte_rate", 1024.0));
    }

    @Test
    void defaultUserEntityRoundTrips() throws Exception {
        // Kafka convention: null entityName on the wire == "default entity" bucket. The transcoder
        // normalises it to the empty string on storage; describe with ofDefaultEntity() restores
        // the null on the way out.
        Map<String, String> defaultUser = new HashMap<>();
        defaultUser.put(ClientQuotaEntity.USER, null);
        ClientQuotaEntity entity = new ClientQuotaEntity(defaultUser);
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "producer_byte_rate", 2048.0)))))
                .all()
                .get();

        ClientQuotaFilter filter =
                ClientQuotaFilter.containsOnly(
                        Collections.singletonList(
                                ClientQuotaFilterComponent.ofDefaultEntity(
                                        ClientQuotaEntity.USER)));
        Map<ClientQuotaEntity, Map<String, Double>> rows =
                admin.describeClientQuotas(filter).entities().get();
        assertThat(rows).hasSize(1);
        Map.Entry<ClientQuotaEntity, Map<String, Double>> row = rows.entrySet().iterator().next();
        // The default-entity entry carries (user, null) — not (user, "").
        assertThat(row.getKey().entries()).hasSize(1);
        assertThat(row.getKey().entries().get(ClientQuotaEntity.USER)).isNull();
        assertThat(row.getValue()).containsExactly(entry("producer_byte_rate", 2048.0));

        // Cleanup: remove the default-entity row so later tests don't inherit it.
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "producer_byte_rate", null)))))
                .all()
                .get();
    }

    @Test
    void removeViaNullValueDeletesTheRow() throws Exception {
        String user = "temp_" + System.nanoTime();
        ClientQuotaEntity entity =
                new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, user));
        // Create.
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "consumer_byte_rate", 4096.0)))))
                .all()
                .get();
        // Confirm existence.
        ClientQuotaFilter filter =
                ClientQuotaFilter.containsOnly(
                        Collections.singletonList(
                                ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user)));
        assertThat(admin.describeClientQuotas(filter).entities().get()).hasSize(1);

        // Remove via null value — kafka-clients sets remove=true on the wire when value is null.
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "consumer_byte_rate", null)))))
                .all()
                .get();
        // Confirm gone.
        assertThat(admin.describeClientQuotas(filter).entities().get()).isEmpty();
    }

    @Test
    void clientIdEntityTypeRoundTrips() throws Exception {
        String clientId = "svc_" + System.nanoTime();
        ClientQuotaEntity entity =
                new ClientQuotaEntity(
                        Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, clientId));
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "request_percentage", 25.0)))))
                .all()
                .get();

        ClientQuotaFilter filter =
                ClientQuotaFilter.containsOnly(
                        Collections.singletonList(
                                ClientQuotaFilterComponent.ofEntity(
                                        ClientQuotaEntity.CLIENT_ID, clientId)));
        Map<ClientQuotaEntity, Map<String, Double>> rows =
                admin.describeClientQuotas(filter).entities().get();
        assertThat(rows).hasSize(1);
        Map.Entry<ClientQuotaEntity, Map<String, Double>> row = rows.entrySet().iterator().next();
        assertThat(row.getKey().entries())
                .containsExactly(entry(ClientQuotaEntity.CLIENT_ID, clientId));
        assertThat(row.getValue()).containsExactly(entry("request_percentage", 25.0));
    }

    @Test
    void quotaSurvivesTabletServerRestart() throws Exception {
        // Persistence pin: the catalog writes go to _catalog._client_quotas (a PK system table)
        // so an upsert must survive a tablet-server bounce. We run on a single-node extension,
        // bounce that server, then re-describe against a fresh AdminClient to avoid any cached
        // connection state.
        String user = "bob_" + System.nanoTime();
        ClientQuotaEntity entity =
                new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, user));
        admin.alterClientQuotas(
                        Collections.singletonList(
                                new ClientQuotaAlteration(
                                        entity,
                                        Collections.singletonList(
                                                new ClientQuotaAlteration.Op(
                                                        "producer_byte_rate", 8192.0)))))
                .all()
                .get();

        // Bounce the only tablet server. The coordinator stays up; the tablet server host that
        // serves the Kafka listener is torn down and recreated with the same data dir.
        int victim = CLUSTER.getTabletServerInfos().get(0).id();
        CLUSTER.restartTabletServer(victim, new Configuration());

        // The KAFKA listener's port is re-bound on restart — reconnect AdminClient to the new
        // bootstrap address so we're not holding a stale connection.
        admin.close();
        admin = KafkaAdminClient.create(adminProps());

        ClientQuotaFilter filter =
                ClientQuotaFilter.containsOnly(
                        Collections.singletonList(
                                ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user)));
        Map<ClientQuotaEntity, Map<String, Double>> rows =
                admin.describeClientQuotas(filter).entities().get();
        assertThat(rows).as("quota for %s survives tablet-server restart", user).hasSize(1);
        Map.Entry<ClientQuotaEntity, Map<String, Double>> row = rows.entrySet().iterator().next();
        assertThat(row.getValue()).containsExactly(entry("producer_byte_rate", 8192.0));
    }

    private static Map.Entry<String, Double> entry(String k, Double v) {
        return new java.util.AbstractMap.SimpleEntry<>(k, v);
    }

    private static Map.Entry<String, String> entry(String k, String v) {
        return new java.util.AbstractMap.SimpleEntry<>(k, v);
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static String bootstrap() {
        StringBuilder b = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }
        return b.toString();
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
