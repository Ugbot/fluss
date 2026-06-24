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
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for Kafka {@code CREATE_PARTITIONS} against the Fluss bolt-on. Fluss tables have
 * a fixed bucket count, so the three outcomes exercised here are:
 *
 * <ul>
 *   <li>No-op when the requested count matches the existing bucket count.
 *   <li>{@link InvalidPartitionsException} (with a descriptive message) when the client asks to
 *       grow or shrink the topic.
 *   <li>{@link UnknownTopicOrPartitionException} for an unknown topic.
 * </ul>
 */
class KafkaCreatePartitionsITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;

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
    void createPartitionsToSameCountIsNoOp() throws Exception {
        String topic = "same_" + uniqueSuffix();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 3, (short) 1)))
                .all()
                .get();
        try {
            Map<String, NewPartitions> req = new HashMap<>();
            req.put(topic, NewPartitions.increaseTo(3));
            admin.createPartitions(req).all().get();

            TopicDescription desc =
                    admin.describeTopics(Collections.singletonList(topic))
                            .allTopicNames()
                            .get()
                            .get(topic);
            assertThat(desc.partitions()).hasSize(3);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void createPartitionsGrowFails() throws Exception {
        String topic = "grow_" + uniqueSuffix();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, (short) 1)))
                .all()
                .get();
        try {
            Map<String, NewPartitions> req = new HashMap<>();
            req.put(topic, NewPartitions.increaseTo(4));
            assertThatThrownBy(() -> admin.createPartitions(req).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(InvalidPartitionsException.class)
                    .hasMessageContaining("fixed bucket count");

            TopicDescription desc =
                    admin.describeTopics(Collections.singletonList(topic))
                            .allTopicNames()
                            .get()
                            .get(topic);
            assertThat(desc.partitions()).hasSize(2);
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void createPartitionsUnknownTopicFails() {
        String topic = "ghost_" + uniqueSuffix();
        Map<String, NewPartitions> req = new HashMap<>();
        req.put(topic, NewPartitions.increaseTo(4));
        assertThatThrownBy(() -> admin.createPartitions(req).all().get())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(UnknownTopicOrPartitionException.class);
    }

    private static String uniqueSuffix() {
        return System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
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
