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

import org.apache.fluss.catalog.entities.KafkaTxnStateEntity;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.tx.TransactionCoordinator;
import org.apache.fluss.kafka.tx.TransactionCoordinators;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the Phase J.2 transactional producer wire APIs against the Fluss Kafka
 * bolt-on. Drives a real {@link KafkaProducer} through {@code beginTransaction → send →
 * commitTransaction / abortTransaction} and asserts the {@link TransactionCoordinator}'s state row
 * walks the design-doc state machine and ends in {@code Empty} after each cycle. Also exercises
 * {@link Admin#listTransactions} and {@link Admin#describeTransactions} so the observability pair
 * is covered by the same harness.
 *
 * <p>The four scenarios (basic commit, basic abort, sequential init epoch bump, admin
 * observability) run in one {@code @Test} method to avoid the ~30s cluster-warm-up tax the
 * test-cluster extension's afterEach pays whenever the {@code _catalog} database is recreated.
 *
 * <p>Read-side filtering by {@code isolation.level} is intentionally out of scope here — that lands
 * in J.3 once the LSO cursor exists on {@code LogTablet}. These tests only assert wire-level
 * success and coordinator-side state transitions.
 */
class KafkaTransactionalProducerITCase {

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
        // The txn coordinator's bootstrap rehydrate runs at coord-leader election but is
        // best-effort: it swallows catalog-not-yet-reachable errors and starts empty. So the
        // system tables it backs onto are created lazily on first access. Step 1 — wait for the
        // coordinator to register on this JVM. Step 2 — trigger lazy table creation by listing.
        // Step 3 — await bucket-leadership propagation. After step 3, the next INIT_PRODUCER_ID
        // is fast and deterministic.
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!TransactionCoordinators.current().isPresent()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "TransactionCoordinator did not register within 30s of test start");
            }
            Thread.sleep(50L);
        }
        org.apache.fluss.catalog.CatalogService catalog =
                org.apache.fluss.catalog.CatalogServices.current()
                        .orElseThrow(
                                () -> new IllegalStateException("CatalogService not registered"));
        catalog.listKafkaTxnStates();
        catalog.listKafkaProducerIds();
        catalog.listKafkaTxnOffsets("__warmup__");
        CLUSTER.awaitSystemTablesReady(
                Duration.ofSeconds(60),
                "_catalog._kafka_txn_state",
                "_catalog._kafka_producer_ids",
                "_catalog._kafka_txn_offset_buffer");
    }

    /**
     * Omnibus test: walks four scenarios serially against one cluster lifecycle.
     *
     * <ol>
     *   <li><b>basic commit</b> — begin → send → commit; in-memory state goes Empty → Ongoing → … →
     *       Empty, partition set cleared.
     *   <li><b>basic abort</b> — begin → send → abort; same final state.
     *   <li><b>sequential init epoch bump</b> — two producers with the same {@code
     *       transactional.id} sequentially; the second {@code initTransactions()} bumps the
     *       producer epoch on the coordinator's state row.
     *   <li><b>admin observability</b> — {@link Admin#listTransactions} returns the active txn
     *       while it's Ongoing; {@link Admin#describeTransactions} reflects the state row.
     * </ol>
     */
    @Test
    void txnLifecycleAndObservability() throws Exception {
        TransactionCoordinator coord = TransactionCoordinators.current().orElseThrow();
        Random rng = new Random();

        // Scenario 1: basic commit.
        String topicCommit = "txn_commit_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicCommit, 1, (short) 1)))
                .all()
                .get();
        String txnIdCommit = "txn-commit-" + UUID.randomUUID();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdCommit))) {
            producer.initTransactions();
            producer.beginTransaction();
            int recordCount = 1 + rng.nextInt(8);
            byte[] valueBytes = randomBytes(rng, 64);
            for (int i = 0; i < recordCount; i++) {
                producer.send(
                        new ProducerRecord<>(topicCommit, 0, randomBytes(rng, 8), valueBytes));
            }
            producer.flush();
            Optional<KafkaTxnStateEntity> midFlight = coord.getState(txnIdCommit);
            assertThat(midFlight).isPresent();
            assertThat(midFlight.get().state()).isEqualTo(KafkaTxnStateEntity.STATE_ONGOING);
            assertThat(midFlight.get().topicPartitions()).contains(topicCommit + ":0");
            producer.commitTransaction();
            assertThat(coord.getState(txnIdCommit).orElseThrow().state())
                    .isEqualTo(KafkaTxnStateEntity.STATE_EMPTY);
            assertThat(coord.getState(txnIdCommit).orElseThrow().topicPartitions()).isEmpty();
        }

        // Scenario 2: basic abort.
        String topicAbort = "txn_abort_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicAbort, 1, (short) 1)))
                .all()
                .get();
        String txnIdAbort = "txn-abort-" + UUID.randomUUID();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdAbort))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(
                    new ProducerRecord<>(topicAbort, 0, randomBytes(rng, 8), randomBytes(rng, 64)));
            producer.flush();
            assertThat(coord.getState(txnIdAbort).orElseThrow().state())
                    .isEqualTo(KafkaTxnStateEntity.STATE_ONGOING);
            producer.abortTransaction();
            assertThat(coord.getState(txnIdAbort).orElseThrow().state())
                    .isEqualTo(KafkaTxnStateEntity.STATE_EMPTY);
            assertThat(coord.getState(txnIdAbort).orElseThrow().topicPartitions()).isEmpty();
        }

        // Scenario 3: sequential init epoch bump.
        String topicEpoch = "txn_epoch_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicEpoch, 1, (short) 1)))
                .all()
                .get();
        String txnIdEpoch = "txn-epoch-" + UUID.randomUUID();
        short firstEpoch;
        short secondEpoch;
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdEpoch))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(
                    new ProducerRecord<>(topicEpoch, 0, randomBytes(rng, 8), randomBytes(rng, 32)));
            producer.commitTransaction();
            firstEpoch = coord.getState(txnIdEpoch).orElseThrow().producerEpoch();
        }
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdEpoch))) {
            producer.initTransactions();
            secondEpoch = coord.getState(txnIdEpoch).orElseThrow().producerEpoch();
        }
        assertThat(secondEpoch).isGreaterThan(firstEpoch);

        // Scenario 4: AdminClient observability mid-flight.
        String topicAdmin = "txn_admin_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicAdmin, 1, (short) 1)))
                .all()
                .get();
        String txnIdAdmin = "txn-admin-" + UUID.randomUUID();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdAdmin))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(
                    new ProducerRecord<>(topicAdmin, 0, randomBytes(rng, 8), randomBytes(rng, 32)));
            producer.flush();

            Collection<TransactionListing> listings = admin.listTransactions().all().get();
            Optional<TransactionListing> found =
                    listings.stream()
                            .filter(l -> txnIdAdmin.equals(l.transactionalId()))
                            .findFirst();
            assertThat(found).as("listTransactions includes %s", txnIdAdmin).isPresent();
            assertThat(found.get().state()).isEqualTo(TransactionState.ONGOING);

            Map<String, TransactionDescription> describe =
                    admin.describeTransactions(Collections.singletonList(txnIdAdmin)).all().get();
            assertThat(describe).containsKey(txnIdAdmin);
            TransactionDescription desc = describe.get(txnIdAdmin);
            assertThat(desc.state()).isEqualTo(TransactionState.ONGOING);
            assertThat(desc.producerId()).isPositive();
            assertThat(desc.topicPartitions()).isNotEmpty();
            assertThat(desc.topicPartitions().iterator().next().topic()).isEqualTo(topicAdmin);

            producer.commitTransaction();
        }
    }

    private static byte[] randomBytes(Random rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        return props;
    }

    private static Properties transactionalProducerProps(String txnId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        // The @BeforeEach hook awaits ZK-side bucket-leadership for the system tables, but the
        // broker's TransactionCoordinator opens its catalog Connection lazily on first request.
        // The first INIT_PRODUCER_ID through the broker pays that one-time connection cost
        // (~30-60s observed); subsequent calls are fast. 120s window absorbs the cold start
        // without masking real cluster-side regressions.
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
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
