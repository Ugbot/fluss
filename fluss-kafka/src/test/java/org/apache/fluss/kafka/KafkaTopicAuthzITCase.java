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
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end proof that the Phase G ACL enforcement chain works. A real kafka-clients AdminClient
 * authenticates via SASL/PLAIN as {@code alice} (non-super-user). Before any grants are seeded,
 * CreateTopics must be refused with {@link ClusterAuthorizationException}; after a direct
 * authorizer {@code addAcls(...)} call grants alice {@code CREATE} on the Kafka-backing database,
 * the exact same call succeeds. This validates: SASL principal extraction (Phase F) → Session
 * construction → {@link org.apache.fluss.server.authorizer.Authorizer} lookup → Kafka wire-format
 * error translation.
 */
@TestMethodOrder(OrderAnnotation.class)
class KafkaTopicAuthzITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "admin-secret";
    private static final String ALICE = "alice";
    private static final String ALICE_PASSWORD = "alice-secret";

    private static final String SERVER_JAAS_INFO =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "user_"
                    + ADMIN
                    + "=\""
                    + ADMIN_PASSWORD
                    + "\" "
                    + "user_"
                    + ALICE
                    + "=\""
                    + ALICE_PASSWORD
                    + "\";";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(aclClusterConf())
                    .build();

    private Admin aliceAdmin;

    @BeforeEach
    void setUp() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
        aliceAdmin = KafkaAdminClient.create(adminPropsFor(ALICE, ALICE_PASSWORD));
    }

    @AfterEach
    void tearDown() {
        if (aliceAdmin != null) {
            aliceAdmin.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    void aliceCannotCreateTopicsWithoutGrant() {
        String topic = "authz_" + System.nanoTime();
        assertThatThrownBy(
                        () ->
                                aliceAdmin
                                        .createTopics(
                                                Collections.singletonList(
                                                        new NewTopic(topic, 1, (short) 1)))
                                        .all()
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOfAny(
                        ClusterAuthorizationException.class, TopicAuthorizationException.class);
    }

    @Test
    @Order(2)
    void aliceCanCreateTopicsAfterGrant() throws Exception {
        String topic = "authz_ok_" + System.nanoTime();
        AclBinding grant =
                new AclBinding(
                        Resource.database(KAFKA_DATABASE),
                        new AccessControlEntry(
                                new FlussPrincipal(ALICE, "User"),
                                "*",
                                OperationType.CREATE,
                                PermissionType.ALLOW));
        seedAcl(grant);
        CLUSTER.waitUntilAuthenticationSync(Collections.singletonList(grant), true);

        aliceAdmin
                .createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        assertThat(aliceAdmin.listTopics().names().get()).contains(topic);
    }

    private void seedAcl(AclBinding binding) {
        Authorizer authorizer = CLUSTER.getCoordinatorServer().getAuthorizer();
        assertThat(authorizer).isNotNull();
        authorizer.addAcls(
                new org.apache.fluss.rpc.netty.server.Session(
                        (short) 0,
                        KAFKA_LISTENER,
                        false,
                        java.net.InetAddress.getLoopbackAddress(),
                        new FlussPrincipal(ADMIN, "User")),
                Collections.singletonList(binding));
    }

    private static Properties adminPropsFor(String user, String password) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\""
                        + user
                        + "\" password=\""
                        + password
                        + "\";");
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

    private static Configuration aclClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), KAFKA_LISTENER + ":sasl");
        conf.setString(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key(), "PLAIN");
        conf.setString("security.sasl.listener.name.kafka.plain.jaas.config", SERVER_JAAS_INFO);
        conf.setString("security.sasl.plain.jaas.config", SERVER_JAAS_INFO);
        // Turn on Fluss's native authorizer so the Kafka handler has something to delegate to.
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        // admin is super-user → bypasses ACL checks when seeding grants; alice is not.
        conf.setString(ConfigOptions.SUPER_USERS.key(), "User:" + ADMIN);
        return conf;
    }
}
