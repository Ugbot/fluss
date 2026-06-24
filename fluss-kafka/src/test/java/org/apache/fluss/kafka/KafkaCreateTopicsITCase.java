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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Kafka CreateTopics / DeleteTopics: driving Fluss table lifecycle through an
 * AdminClient against the KAFKA listener.
 */
class KafkaCreateTopicsITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 3;

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(NUM_TABLET_SERVERS)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(kafkaClusterConf())
                    .build();

    private static Admin admin;

    @BeforeAll
    static void createAdmin() {
        StringBuilder bootstrap = new StringBuilder();
        List<ServerNode> nodes = CLUSTER.getTabletServerNodes(KAFKA_LISTENER);
        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                bootstrap.append(',');
            }
            bootstrap.append(nodes.get(i).host()).append(':').append(nodes.get(i).port());
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap.toString());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        admin = KafkaAdminClient.create(props);
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close();
        }
    }

    @BeforeEach
    void ensureKafkaDatabase() throws Exception {
        // FlussClusterExtension's afterEach drops every non-builtin database between tests; the
        // coordinator re-creates `kafka` only at leader election, so we recreate it explicitly
        // before each test to match real operator setup.
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void createTopicMaterializesKafkaShapedFlussTable() throws Exception {
        String topic = "create_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 4, (short) 1)))
                .all()
                .get();

        try {
            // AdminClient sees it.
            Map<String, TopicListing> listed = admin.listTopics().namesToListings().get();
            assertThat(listed).containsKey(topic);

            // DescribeTopics returns the right partition count.
            Map<String, TopicDescription> described =
                    admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
            assertThat(described.get(topic).partitions()).hasSize(4);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void createTopicRejectsDotNames() {
        String topic = "with.dot";
        assertThatThrownBy(
                        () ->
                                admin.createTopics(
                                                Collections.singletonList(
                                                        new NewTopic(topic, 1, (short) 1)))
                                        .all()
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidTopicException.class);
    }

    @Test
    void createTopicAlreadyExistsReturnsTopicExists() throws Exception {
        String topic = "dupe_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        try {
            assertThatThrownBy(
                            () ->
                                    admin.createTopics(
                                                    Collections.singletonList(
                                                            new NewTopic(topic, 1, (short) 1)))
                                            .all()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicExistsException.class);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void deleteUnknownTopicFails() {
        String topic = "ghost_" + System.nanoTime();
        assertThatThrownBy(() -> admin.deleteTopics(Collections.singletonList(topic)).all().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void deleteRoundTripRemovesTable() throws Exception {
        String topic = "gone_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        admin.deleteTopics(Collections.singletonList(topic)).all().get();

        Map<String, TopicListing> listed = admin.listTopics().namesToListings().get();
        assertThat(listed).doesNotContainKey(topic);
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
