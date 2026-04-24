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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end ElectLeaders test against the Fluss Kafka bolt-on. Phase I.1 honest-stub contract:
 *
 * <ul>
 *   <li>Fresh topic with 3 replicas — every partition reports {@code ELECTION_NOT_NEEDED} because
 *       Fluss's default placement picks the first replica as leader ({@link
 *       ElectionNotNeededException}).
 *   <li>Forced non-preferred leader via controlled-shutdown migration — transcoder surfaces {@link
 *       PreferredLeaderNotAvailableException} with an explicit "does not yet expose" message, not
 *       silent success. This is the stubbed branch Cruise Control / kafka-leader-election.sh see
 *       until Fluss exposes a real election primitive (design 0011 §3).
 *   <li>Unknown topic — {@code UNKNOWN_TOPIC_OR_PARTITION} (or a short-timeout {@code
 *       TimeoutException} as Kafka AdminClient retries internally on the metadata lookup).
 *   <li>Null topic-partitions ("elect everything") — no-op success, empty map.
 *   <li>Unclean election — {@code INVALID_REQUEST}.
 * </ul>
 */
class KafkaElectLeadersITCase {

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
    void electPreferredLeaderIsNoopOnFreshTopic() throws Exception {
        String topic = "electleaders_" + System.nanoTime();
        int numPartitions = 3;
        admin.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, (short) 3)))
                .all()
                .get();

        try {
            Set<TopicPartition> partitions = new HashSet<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new TopicPartition(topic, i));
            }
            // Wait for every partition's leader to be published through the metadata cache; the
            // coordinator announces CreateTable synchronously but bucket leadership propagates
            // through an asynchronous cache update.
            waitForLeaders(topic, numPartitions);

            // On a fresh topic Fluss's placement makes replicas[0] the current leader, which is
            // the preferred replica, so every partition must report ELECTION_NOT_NEEDED via the
            // per-partition Optional<Throwable> map.
            Map<TopicPartition, Optional<Throwable>> result =
                    admin.electLeaders(ElectionType.PREFERRED, partitions).partitions().get();
            assertThat(result).hasSize(numPartitions);
            for (TopicPartition tp : partitions) {
                Optional<Throwable> err = result.get(tp);
                assertThat(err).as("error for %s", tp).isPresent();
                assertThat(err.get()).isInstanceOf(ElectionNotNeededException.class);
            }
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    @Test
    void electPreferredLeaderOnUnknownTopicFailsFast() {
        // AdminClient issues a METADATA request first; for an unknown topic metadata returns no
        // partition so AdminClient retries internally until its api timeout elapses. Use a
        // short-timeout client to avoid the 30s default-api-timeout wall-clock cost.
        Properties props = adminProps();
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2_000);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
        try (Admin shortAdmin = KafkaAdminClient.create(props)) {
            String ghost = "ghost_" + System.nanoTime();
            TopicPartition tp = new TopicPartition(ghost, 0);
            // AdminClient either surfaces UnknownTopicOrPartition (if metadata returns an empty
            // topic entry) or TimeoutException (if metadata retries until the short api timeout
            // elapses). Either is an acceptable "fail fast" signal for this test.
            assertThatThrownBy(
                            () ->
                                    shortAdmin
                                            .electLeaders(
                                                    ElectionType.PREFERRED,
                                                    Collections.singleton(tp))
                                            .all()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .satisfiesAnyOf(
                            t ->
                                    assertThat(t)
                                            .isInstanceOf(
                                                    org.apache.kafka.common.errors
                                                            .UnknownTopicOrPartitionException
                                                            .class),
                            t ->
                                    assertThat(t)
                                            .isInstanceOf(
                                                    org.apache.kafka.common.errors.TimeoutException
                                                            .class));
        }
    }

    @Test
    void nonPreferredLeaderSurfacesExplicitUnsupportedError() throws Exception {
        // Phase I.1 honest-stub pin: when the current leader is NOT the preferred replica (Fluss
        // convention: replicas[0]), the transcoder has no public election primitive to drive and
        // must surface the explicit PREFERRED_LEADER_NOT_AVAILABLE error with a descriptive
        // message — otherwise Cruise Control / kafka-leader-election.sh silently decide the
        // cluster is already balanced. Build that state by stopping the replica-0 tablet server
        // while the topic is healthy, wait for the controlled-shutdown election to migrate
        // leadership off it, then restart the server so it rejoins as a follower. The preferred
        // replica is now alive but NOT leader — exactly the stubbed branch.
        String topic = "electleaders_stub_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 3)))
                .all()
                .get();
        try {
            // Resolve the bucket id so we can wait for a concrete leader transition.
            TablePath path = TablePath.of(KAFKA_DATABASE, topic);
            long tableId =
                    CLUSTER.newCoordinatorClient()
                            .getTableInfo(RpcMessageTestUtils.newGetTableInfoRequest(path))
                            .get()
                            .getTableId();
            TableBucket tb = new TableBucket(tableId, 0);
            int initialLeader = CLUSTER.waitAndGetLeader(tb);
            // Fluss placement: replica 0 is preferred and becomes the initial leader.
            assertThat(initialLeader).isZero();

            // Force a non-preferred leader via controlled shutdown of tablet server 0. The
            // coordinator's ControlledShutdownLeaderElection migrates leadership to replica 1 or
            // 2; after the server restarts it rejoins ISR as a follower.
            CLUSTER.stopTabletServer(0);
            long deadlineMs = System.currentTimeMillis() + 30_000L;
            int migratedLeader = -1;
            while (System.currentTimeMillis() < deadlineMs) {
                try {
                    int cur = CLUSTER.waitAndGetLeader(tb);
                    if (cur != 0) {
                        migratedLeader = cur;
                        break;
                    }
                } catch (Throwable ignore) {
                    // zk may briefly miss the leader node during transition; loop.
                }
                Thread.sleep(50);
            }
            assertThat(migratedLeader)
                    .as("leader migrated away from preferred replica 0")
                    .isIn(1, 2);
            CLUSTER.startTabletServer(0);
            CLUSTER.waitUntilReplicaExpandToIsr(tb, 0);

            // Now fire electLeaders — transcoder must classify this as
            // PREFERRED_LEADER_NOT_AVAILABLE with the explicit stub-error message.
            TopicPartition tp = new TopicPartition(topic, 0);
            Map<TopicPartition, Optional<Throwable>> result =
                    admin.electLeaders(ElectionType.PREFERRED, Collections.singleton(tp))
                            .partitions()
                            .get();
            Optional<Throwable> err = result.get(tp);
            assertThat(err).isPresent();
            assertThat(err.get())
                    .isInstanceOf(PreferredLeaderNotAvailableException.class)
                    .hasMessageContaining("design 0011");
        } finally {
            try {
                admin.deleteTopics(Collections.singletonList(topic)).all().get();
            } catch (Throwable ignore) {
                // best-effort cleanup; the cluster is shared across tests.
            }
        }
    }

    @Test
    void electLeadersForEmptyPartitionSetSucceeds() throws Exception {
        // kafka-leader-election.sh --all-topic-partitions routes through
        // AdminClient.electLeaders(PREFERRED, null) which translates to an ElectLeaders request
        // with no topicPartitions. Transcoder short-circuits to NONE / empty list; AdminClient
        // surfaces an empty map. Pin that to avoid regressing into a wire-level error that would
        // break Cruise Control's bootstrap.
        Map<TopicPartition, Optional<Throwable>> result =
                admin.electLeaders(ElectionType.PREFERRED, null).partitions().get();
        assertThat(result).isEmpty();
    }

    @Test
    void uncleanElectionIsRejected() throws Exception {
        String topic = "electleaders_unclean_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        try {
            TopicPartition tp = new TopicPartition(topic, 0);
            // Unclean leader election is refused by the transcoder at the top level; AdminClient
            // propagates the wire-level INVALID_REQUEST as InvalidRequestException on .all().
            assertThatThrownBy(
                            () ->
                                    admin.electLeaders(
                                                    ElectionType.UNCLEAN, Collections.singleton(tp))
                                            .all()
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .satisfiesAnyOf(
                            t -> assertThat(t).isInstanceOf(InvalidRequestException.class),
                            t ->
                                    assertThat(t)
                                            .isInstanceOf(
                                                    org.apache.kafka.common.errors
                                                            .ClusterAuthorizationException.class));
        } finally {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        }
    }

    private static void waitForLeaders(String topic, int numPartitions) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<String, TopicDescription> desc =
                        admin.describeTopics(Collections.singletonList(topic))
                                .allTopicNames()
                                .get();
                TopicDescription td = desc.get(topic);
                if (td != null
                        && td.partitions().size() == numPartitions
                        && td.partitions().stream().allMatch(p -> p.leader() != null)) {
                    return;
                }
            } catch (ExecutionException ignored) {
                // topic not yet fully propagated; loop and retry
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException(
                "Topic " + topic + " did not get leaders for all partitions within 30s");
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
        // Force 3-way replication so electLeaders has a non-trivial assignment to inspect.
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}
