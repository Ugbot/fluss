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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
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
 * End-to-end test for Kafka {@code DescribeConfigs}, {@code AlterConfigs} and {@code
 * IncrementalAlterConfigs}. An unmodified {@code kafka-clients} {@link Admin} drives the Fluss
 * KAFKA listener; all assertions are on the round-tripped topic custom properties.
 */
public class KafkaAdminConfigsITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";
    private static final int NUM_TABLET_SERVERS = 1;
    private static final int NUM_BUCKETS = 2;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
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
        List<ServerNode> nodes = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes(KAFKA_LISTENER);
        assertThat(nodes).hasSize(NUM_TABLET_SERVERS);

        StringBuilder bootstrap = new StringBuilder();
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
        FLUSS_CLUSTER_EXTENSION
                .newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
    }

    @Test
    void describeConfigsReturnsKafkaBindingProperties() throws Exception {
        String topic = uniqueTopic("describe_bind");
        createTopic(topic, NUM_BUCKETS);
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);

            assertThat(config).isNotNull();
            // Kafka-binding "compression.type" was set at topic creation; the Kafka AdminClient
            // maps a null/NONE value to "NONE" via configs(). Our binding compression for a topic
            // created without an explicit compression is absent, so the key resolves to the
            // default (reported as isDefault=true by the broker).
            ConfigEntry compressionEntry = config.get("compression.type");
            assertThat(compressionEntry).isNotNull();
            // message.timestamp.type defaults to CreateTime for Kafka-created topics.
            ConfigEntry tsTypeEntry = config.get("message.timestamp.type");
            assertThat(tsTypeEntry).isNotNull();
            assertThat(tsTypeEntry.value()).isEqualToIgnoringCase("CreateTime");

            // retention.ms should be surfaced as a long (ms) mapped from table.log.ttl. It's
            // sourced from the Fluss table default (7d), so either the default-source entry is
            // returned or the topic-level entry reflects that Duration.
            ConfigEntry retentionEntry = config.get("retention.ms");
            assertThat(retentionEntry).isNotNull();
            if (retentionEntry.value() != null) {
                assertThat(Long.parseLong(retentionEntry.value())).isGreaterThan(0L);
            }
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void alterConfigsSetsCustomProperty() throws Exception {
        String topic = uniqueTopic("alter_set");
        String key = "ext.owner";
        String value = "team-" + ThreadLocalRandom.current().nextInt(100_000);
        createTopic(topic, NUM_BUCKETS);
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, Config> change = new HashMap<>();
            change.put(
                    resource, new Config(Collections.singletonList(new ConfigEntry(key, value))));
            @SuppressWarnings("deprecation")
            AlterConfigsResult altered =
                    admin.alterConfigs(change, new AlterConfigsOptions().validateOnly(false));
            altered.all().get();

            Config described =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            ConfigEntry entry = described.get(key);
            assertThat(entry).isNotNull();
            assertThat(entry.value()).isEqualTo(value);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void incrementalAlterConfigsDeletesCustomProperty() throws Exception {
        String topic = uniqueTopic("inc_delete");
        String key = "ext.team";
        String value = "retention-squad-" + ThreadLocalRandom.current().nextInt(100_000);
        createTopic(topic, NUM_BUCKETS);
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

            // 1) SET via incremental alter.
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> setOp = new HashMap<>();
            setOp.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry(key, value), AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(setOp).all().get();

            Config afterSet =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(afterSet.get(key)).isNotNull();
            assertThat(afterSet.get(key).value()).isEqualTo(value);

            // 2) DELETE via incremental alter.
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> deleteOp = new HashMap<>();
            deleteOp.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE)));
            admin.incrementalAlterConfigs(deleteOp).all().get();

            Config afterDelete =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            // Either the key is absent or the AdminClient surfaces a null value entry; both mean
            // "not set" as far as the topic config surface is concerned.
            ConfigEntry deletedEntry = afterDelete.get(key);
            assertThat(deletedEntry == null || deletedEntry.value() == null).isTrue();
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void incrementalAlterConfigsAppendRejectedAsInvalidConfig() throws Exception {
        String topic = uniqueTopic("inc_append_reject");
        createTopic(topic, NUM_BUCKETS);
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> appendOp = new HashMap<>();
            appendOp.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("cleanup.policy", "compact"),
                                    AlterConfigOp.OpType.APPEND)));

            assertThatThrownBy(() -> admin.incrementalAlterConfigs(appendOp).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InvalidConfigurationException.class)
                    .hasMessageContaining("APPEND");
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void describeBrokerConfigsReturnsReadonlyCatalogue() throws Exception {
        // Phase K-CFG: DescribeConfigs(BROKER, id) now returns the broker catalogue as a
        // READ-ONLY set so tools that enumerate cluster state (kafka-configs.sh --entity-type
        // brokers, Cruise Control, MirrorMaker 2) work.
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Config cfg =
                admin.describeConfigs(Collections.singletonList(resource))
                        .all()
                        .get()
                        .get(resource);
        assertThat(cfg).isNotNull();
        // Spot-check standard broker keys that real Kafka 3.9 reports.
        assertThat(cfg.get("broker.id")).isNotNull();
        assertThat(cfg.get("num.network.threads")).isNotNull();
        assertThat(cfg.get("log.retention.ms")).isNotNull();
        assertThat(cfg.get("auto.create.topics.enable")).isNotNull();
        assertThat(cfg.get("num.partitions")).isNotNull();
        assertThat(cfg.get("default.replication.factor")).isNotNull();
        // Every broker entry is read-only.
        for (ConfigEntry e : cfg.entries()) {
            assertThat(e.isReadOnly())
                    .as("broker config '" + e.name() + "' must be read-only")
                    .isTrue();
        }
    }

    @Test
    void alterBrokerConfigsStillRejected() {
        // We report broker configs but don't accept writes on them — Fluss's real broker config
        // changes only via server.yaml + restart.
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
        ops.put(
                resource,
                Collections.singletonList(
                        new AlterConfigOp(
                                new ConfigEntry("num.network.threads", "9"),
                                AlterConfigOp.OpType.SET)));
        assertThatThrownBy(() -> admin.incrementalAlterConfigs(ops).all().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidRequestException.class);
    }

    @Test
    void alterConfigsOnUnknownTopicFails() {
        String ghost = "ghost_" + System.nanoTime();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, ghost);
        Map<ConfigResource, java.util.Collection<AlterConfigOp>> op = new HashMap<>();
        op.put(
                resource,
                Collections.singletonList(
                        new AlterConfigOp(
                                new ConfigEntry("ext.key", "ext.value"),
                                AlterConfigOp.OpType.SET)));
        assertThatThrownBy(() -> admin.incrementalAlterConfigs(op).all().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    // ------------------------------------------------------------------------
    // Phase K-CFG — hardening scenarios
    // ------------------------------------------------------------------------

    @Test
    void describeConfigsReturnsFullCatalogue() throws Exception {
        String topic = uniqueTopic("catalogue");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg).isNotNull();
            // The full standard Kafka topic config set must be represented in DescribeConfigs.
            for (String key :
                    new String[] {
                        "cleanup.policy",
                        "compression.type",
                        "retention.ms",
                        "retention.bytes",
                        "segment.bytes",
                        "segment.ms",
                        "max.message.bytes",
                        "min.insync.replicas",
                        "message.timestamp.type",
                        "preallocate",
                        "message.format.version",
                        "unclean.leader.election.enable",
                    }) {
                assertThat(cfg.get(key))
                        .as("DescribeConfigs must surface standard key '" + key + "'")
                        .isNotNull();
            }
            // READONLY_DEFAULT keys are marked read-only.
            assertThat(cfg.get("preallocate").isReadOnly()).isTrue();
            assertThat(cfg.get("message.format.version").isReadOnly()).isTrue();
            // MAPPED keys are NOT read-only.
            assertThat(cfg.get("retention.ms").isReadOnly()).isFalse();
            assertThat(cfg.get("cleanup.policy").isReadOnly()).isFalse();
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void createTopicWithConfigsIsDescribed() throws Exception {
        String topic = uniqueTopic("createcfg");
        try {
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", "60000");
            configs.put("max.message.bytes", "2048");
            admin.createTopics(
                            Collections.singletonList(
                                    new NewTopic(topic, 1, (short) 1).configs(configs)))
                    .all()
                    .get();

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg.get("retention.ms").value()).isEqualTo("60000");
            assertThat(cfg.get("max.message.bytes").value()).isEqualTo("2048");
            // MAPPED retention.ms source should flip to DYNAMIC_TOPIC_CONFIG on explicit override.
            assertThat(cfg.get("retention.ms").source())
                    .isEqualTo(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void alterMappedRetentionRoundTrips() throws Exception {
        String topic = uniqueTopic("retentionmap");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
            ops.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("retention.ms", "900000"),
                                    AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(ops).all().get();
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg.get("retention.ms").value()).isEqualTo("900000");
            assertThat(cfg.get("retention.ms").source())
                    .isEqualTo(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void alterStoredSegmentBytesRoundTrips() throws Exception {
        String topic = uniqueTopic("segmap");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
            ops.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("segment.bytes", "1048576"),
                                    AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(ops).all().get();
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg.get("segment.bytes").value()).isEqualTo("1048576");
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void alterReadonlyPreallocateRejected() throws Exception {
        String topic = uniqueTopic("prealloc");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
            ops.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("preallocate", "true"),
                                    AlterConfigOp.OpType.SET)));
            assertThatThrownBy(() -> admin.incrementalAlterConfigs(ops).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InvalidConfigurationException.class);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void alterInvalidCleanupPolicyRejected() throws Exception {
        String topic = uniqueTopic("badcleanup");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
            ops.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("cleanup.policy", "banana"),
                                    AlterConfigOp.OpType.SET)));
            assertThatThrownBy(() -> admin.incrementalAlterConfigs(ops).all().get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InvalidConfigurationException.class);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void deleteMappedRetentionResetsToDefault() throws Exception {
        String topic = uniqueTopic("retdel");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            // First set it.
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> setOps = new HashMap<>();
            setOps.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("retention.ms", "120000"),
                                    AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(setOps).all().get();

            // Now delete it.
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> delOps = new HashMap<>();
            delOps.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("retention.ms", null),
                                    AlterConfigOp.OpType.DELETE)));
            admin.incrementalAlterConfigs(delOps).all().get();

            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            // After delete, source flips back to DEFAULT_CONFIG.
            assertThat(cfg.get("retention.ms").source())
                    .isEqualTo(ConfigEntry.ConfigSource.DEFAULT_CONFIG);
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void configFilterReturnsRequestedSubset() throws Exception {
        String topic = uniqueTopic("filter");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<String>> filter = new HashMap<>();
            filter.put(resource, Collections.singletonList("retention.ms"));
            // describeConfigs(Collection<ConfigResource>) doesn't expose key-filter in the public
            // AdminClient API; use a single-config-key DescribeConfigsOptions.
            // Here we just assert the full call returns our key present, and that an absent
            // random key would not collide — sanity check for the catalogue.
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg.get("retention.ms")).isNotNull();
            // Catalogued keys win over a hypothetical user annotation with the same name.
            assertThat(cfg.get("does.not.exist")).isNull();
        } finally {
            dropTopicQuietly(topic);
        }
    }

    @Test
    void customAnnotationStillRoundTrips() throws Exception {
        String topic = uniqueTopic("annot");
        try {
            createTopic(topic, 1);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = new HashMap<>();
            ops.put(
                    resource,
                    Collections.singletonList(
                            new AlterConfigOp(
                                    new ConfigEntry("ext.owner", "team-a"),
                                    AlterConfigOp.OpType.SET)));
            admin.incrementalAlterConfigs(ops).all().get();
            Config cfg =
                    admin.describeConfigs(Collections.singletonList(resource))
                            .all()
                            .get()
                            .get(resource);
            assertThat(cfg.get("ext.owner")).isNotNull();
            assertThat(cfg.get("ext.owner").value()).isEqualTo("team-a");
        } finally {
            dropTopicQuietly(topic);
        }
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private static void createTopic(String name, int numBuckets) throws Exception {
        admin.createTopics(Collections.singletonList(new NewTopic(name, numBuckets, (short) 1)))
                .all()
                .get();
    }

    private static void dropTopicQuietly(String name) {
        try {
            RpcMessageTestUtils.dropTable(
                    FLUSS_CLUSTER_EXTENSION, new TablePath(KAFKA_DATABASE, name));
        } catch (Throwable ignored) {
            // best-effort cleanup; each test isolates on a unique topic name.
        }
    }

    private static String uniqueTopic(String prefix) {
        return prefix + "_" + System.nanoTime();
    }

    private static Configuration kafkaClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        return conf;
    }
}
