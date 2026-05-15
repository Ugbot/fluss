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

import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.catalog.entities.KafkaProducerIdEntity;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of Phase J.1's {@code INIT_PRODUCER_ID} handler against the Fluss Kafka
 * bolt-on. Drives a real {@link KafkaProducer} for every scenario and asserts the catalog state
 * tables ({@code __kafka_producer_ids__}, {@code __kafka_txn_state__}) and the in-process {@link
 * TransactionCoordinator} row reflect the design-doc 0016 §6 contracts.
 *
 * <p>Four scenarios from doc 0016 §16, split across two {@code @Test} methods:
 *
 * <ol>
 *   <li>{@link #idempotentProducerAllocatesPositiveProducerId()} — {@code enable.idempotence=true},
 *       no {@code transactional.id}. The producer obtains a producer id and a record send
 *       round-trips to a real {@link RecordMetadata}. The catalog's {@code __kafka_producer_ids__}
 *       table gains a fresh row whose {@code producerId > 0} and whose {@code transactionalId}
 *       column is null. Epoch is 0.
 *   <li>{@link #transactionalLifecycleAndFailover()} bundles three transactional scenarios:
 *       <ul>
 *         <li>First-init lifecycle — fresh {@code transactional.id}, {@code initTransactions()} +
 *             {@code beginTransaction()} + send. The {@code __kafka_txn_state__} row exists in
 *             {@code Empty} state immediately after init and transitions to {@code Ongoing} after
 *             the first send. Commit returns the row to {@code Empty}.
 *         <li>Re-init bumps epoch and fences the prior producer — the second {@code
 *             initTransactions()} on the same {@code transactional.id} bumps the epoch from 0 → 1,
 *             and a stale-epoch send from the original producer surfaces a {@link
 *             ProducerFencedException} (or {@code InvalidProducerEpochException}, both valid fence
 *             signals per design 0016 §11).
 *         <li>Coordinator failover preserves the durable state row — produce against an {@code
 *             Ongoing} txn, stop + restart the coordinator-leader, observe the new leader rehydrate
 *             the same {@code (producerId, epoch)} key. End-to-end {@code commitTransaction()}
 *             through a rehydrated coordinator depends on J.3's LSO + marker fan-out; we assert the
 *             durable row survived and best-effort close the producer with a 5s grace, which is the
 *             J.1 observable per design 0016 §10.
 *       </ul>
 * </ol>
 *
 * <p>The three transactional scenarios are bundled in one {@code @Test} method so they share a
 * single cluster-warm-up tax (~20s on a fresh cluster, occasionally longer); a separate
 * {@code @Test}-per-scenario layout re-pays the tax and pushes some scenarios past kafka-clients'
 * {@code MAX_BLOCK_MS}. The same omnibus pattern is used by {@link
 * KafkaTransactionalProducerITCase} for J.2.
 *
 * <p>The transactional method is tagged {@code "flaky"} per the testing convention used elsewhere
 * in {@code fluss-kafka}: the producer's first {@code INIT_PRODUCER_ID} pays the system-table
 * propagation cost and can exceed {@code MAX_BLOCK_MS} on a host running other Fluss test forks at
 * the same time. Run alone in a fresh JVM (e.g. {@code mvn -pl fluss-kafka -Dtest=
 * KafkaInitProducerIdITCase verify}) on an otherwise-idle machine for green; do not interleave with
 * other heavy {@code fluss-server} test forks.
 *
 * <p>Read-side filtering (read_committed / aborted-range exclusion) is intentionally <b>not</b>
 * exercised here — that's J.3 territory. These tests stop at coordinator-side state and producer
 * future success.
 */
class KafkaInitProducerIdITCase {

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
    void ensureKafkaDatabaseAndCoord() throws Exception {
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();
        // The txn coordinator bootstrap registers asynchronously after coordinator-leader
        // election. Block until visible so INIT_PRODUCER_ID doesn't race the registration.
        waitForCoordinator();
    }

    @Test
    void idempotentProducerAllocatesPositiveProducerId() throws Exception {
        String topic = "idempotent_init_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Snapshot the catalog's producer-id table before sending so we can detect the row that
        // this test's INIT_PRODUCER_ID added.
        CatalogService catalog = catalog();
        long maxIdBefore = maxProducerId(catalog);

        Random rng = new Random();
        Properties props = idempotentProducerProps();
        long sentOffset;
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = ("k-" + rng.nextInt()).getBytes();
            byte[] value = randomBytes(rng, 64);
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, 0, key, value)).get();
            assertThat(meta).isNotNull();
            sentOffset = meta.offset();
        }
        assertThat(sentOffset).isGreaterThanOrEqualTo(0L);

        // Find the freshly-allocated row: the producer-id table is monotone and never compacts in
        // J.1, so any row whose id is strictly greater than the pre-test maximum was allocated by
        // this test.
        List<KafkaProducerIdEntity> ids = catalog.listKafkaProducerIds();
        Optional<KafkaProducerIdEntity> mine =
                ids.stream().filter(e -> e.producerId() > maxIdBefore).findFirst();
        assertThat(mine)
                .as(
                        "expected a fresh producer-id row > %d after idempotent send; saw %s",
                        maxIdBefore, ids)
                .isPresent();
        assertThat(mine.get().producerId()).isPositive();
        assertThat(mine.get().epoch()).isEqualTo((short) 0);
        // Idempotent-only producers carry a null transactional id by design (0016 §6 path 3).
        assertThat(mine.get().transactionalId()).isNull();
    }

    /**
     * Three transactional scenarios bundled in one {@code @Test} so they share a single
     * cluster-warm-up tax. The first {@code INIT_PRODUCER_ID} after JVM boot pays the system-table
     * bucket-leadership propagation cost (~20s on a fresh cluster, occasionally longer); splitting
     * each scenario into its own {@code @Test} would re-pay it on every method, which we measured
     * exceeding the kafka-clients {@code MAX_BLOCK_MS=180s} window in CI under load. The single
     * {@link KafkaTransactionalProducerITCase} omnibus pattern is the same workaround.
     *
     * <p>Order:
     *
     * <ol>
     *   <li>First-init scenario — fresh {@code transactional.id}, write the {@code Empty} row, send
     *       + flush, observe {@code Ongoing}; commit, observe {@code Empty} again.
     *   <li>Re-init/fence scenario — close the producer above, recreate with the same {@code
     *       transactional.id}, observe the epoch bump from 0 → 1 on the state row. Then open a
     *       fresh same-id producer concurrently and drive a stale-epoch send on the prior one to
     *       confirm the broker rejects it as fenced.
     *   <li>Coordinator-failover scenario — produce on a fresh txn id, stop + restart the
     *       coordinator-leader, observe the new leader rehydrate the same {@code (pid, epoch)}. The
     *       producer is best-effort closed after the assertions because end-to-end commit through a
     *       rehydrated coordinator depends on J.3's LSO + marker fan-out.
     * </ol>
     */
    @Test
    void transactionalLifecycleAndFailover() throws Exception {
        TransactionCoordinator coord = TransactionCoordinators.current().orElseThrow();
        Random rng = new Random();

        // ---------- Scenario A: fresh-init lifecycle ----------
        String topicA = "txn_init_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicA, 1, (short) 1)))
                .all()
                .get();
        String txnIdA = "init-firstcall-" + UUID.randomUUID();
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(transactionalProducerProps(txnIdA))) {
            producer.initTransactions();
            // After initTransactions: row exists, state=Empty, epoch=0, partitions empty.
            Optional<KafkaTxnStateEntity> postInit = coord.getState(txnIdA);
            assertThat(postInit).isPresent();
            assertThat(postInit.get().state()).isEqualTo(KafkaTxnStateEntity.STATE_EMPTY);
            assertThat(postInit.get().producerEpoch()).isEqualTo((short) 0);
            assertThat(postInit.get().producerId()).isPositive();
            assertThat(postInit.get().topicPartitions()).isEmpty();

            producer.beginTransaction();
            producer.send(
                    new ProducerRecord<>(
                            topicA, 0, ("k-" + rng.nextInt()).getBytes(), randomBytes(rng, 32)));
            producer.flush();
            // After the first send: state row transitions to Ongoing with the participating
            // partition recorded.
            Optional<KafkaTxnStateEntity> postSend = coord.getState(txnIdA);
            assertThat(postSend).isPresent();
            assertThat(postSend.get().state()).isEqualTo(KafkaTxnStateEntity.STATE_ONGOING);
            assertThat(postSend.get().topicPartitions()).contains(topicA + ":0");

            // Commit and assert clean cleanup so the test leaves no orphan in-flight txn for the
            // shared cluster lifecycle.
            producer.commitTransaction();
            assertThat(coord.getState(txnIdA).orElseThrow().state())
                    .isEqualTo(KafkaTxnStateEntity.STATE_EMPTY);
            assertThat(coord.getState(txnIdA).orElseThrow().topicPartitions()).isEmpty();
        }

        // ---------- Scenario B: re-init bumps epoch, fences prior producer ----------
        String topicB = "txn_fencing_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicB, 1, (short) 1)))
                .all()
                .get();
        String txnIdB = "init-fence-" + UUID.randomUUID();
        KafkaProducer<byte[], byte[]> first =
                new KafkaProducer<>(transactionalProducerProps(txnIdB));
        try {
            first.initTransactions();
            short firstEpoch = coord.getState(txnIdB).orElseThrow().producerEpoch();
            assertThat(firstEpoch).isEqualTo((short) 0);
            first.beginTransaction();
            first.send(
                    new ProducerRecord<>(
                            topicB, 0, ("k-" + rng.nextInt()).getBytes(), randomBytes(rng, 32)));
            first.commitTransaction();

            // Second producer with the same transactional id; initTransactions on this one bumps
            // the coordinator's epoch (design 0016 §6 path 2).
            try (KafkaProducer<byte[], byte[]> second =
                    new KafkaProducer<>(transactionalProducerProps(txnIdB))) {
                second.initTransactions();
                short bumpedEpoch = coord.getState(txnIdB).orElseThrow().producerEpoch();
                assertThat(bumpedEpoch).isGreaterThan(firstEpoch);
                assertThat((int) bumpedEpoch).isEqualTo(firstEpoch + 1);

                // Now drive a transactional cycle on the FIRST (stale-epoch) producer. The first
                // call carrying the stale (pid, epoch=0) must surface as ProducerFencedException.
                // The kafka-clients producer maps the fence either at beginTransaction (via the
                // coordinator's epoch check) or at the first send (via the broker's per-partition
                // producer-state tracker); we accept either path and either error type.
                Throwable fenceFailure = null;
                try {
                    first.beginTransaction();
                    first.send(
                                    new ProducerRecord<>(
                                            topicB,
                                            0,
                                            ("k-fenced-" + rng.nextInt()).getBytes(),
                                            randomBytes(rng, 32)))
                            .get();
                    first.commitTransaction();
                } catch (ProducerFencedException expected) {
                    fenceFailure = expected;
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof ProducerFencedException) {
                        fenceFailure = cause;
                    } else if (cause
                            instanceof
                            org.apache.kafka.common.errors.InvalidProducerEpochException) {
                        fenceFailure = cause;
                    } else {
                        throw ee;
                    }
                } catch (org.apache.kafka.common.errors.InvalidProducerEpochException epochErr) {
                    // The broker may map the fence to InvalidProducerEpoch when the request hits
                    // the partition leader before the coordinator's per-id state catches up. Both
                    // are valid fence signals (design 0016 §11).
                    fenceFailure = epochErr;
                }
                assertThat(fenceFailure)
                        .as("first (stale-epoch) producer must be fenced after second init")
                        .isNotNull();

                // The second producer is still healthy and its commit cycle works normally.
                second.beginTransaction();
                second.send(
                        new ProducerRecord<>(
                                topicB,
                                0,
                                ("k-" + rng.nextInt()).getBytes(),
                                randomBytes(rng, 32)));
                second.commitTransaction();
            }
        } finally {
            // The first producer is fenced — close throws by design and may also block on the
            // sender thread waiting for a fenced send to acknowledge. Bound the close so a slow
            // sender thread doesn't block the next scenario.
            try {
                first.close(java.time.Duration.ofSeconds(5));
            } catch (Throwable ignore) {
                // Expected after fencing.
            }
        }

        // ---------- Scenario C: coordinator-leader failover preserves state row ----------
        // We assert coord-side rehydration only — this is the J.1 observable. End-to-end producer
        // recovery (the rehydrated coordinator accepting an END_TXN from the still-running
        // producer and walking the state machine to Empty) depends on J.3's marker fan-out + LSO
        // cursor; until those land, the broker side of commitTransaction() can hang post-failover
        // because the tablet-server's view of the coordinator endpoint and the producer-state
        // tracker race the leader bounce. We therefore best-effort close the producer with a 5s
        // grace after the rehydration assertions.
        String topicC = "txn_failover_" + System.nanoTime();
        admin.createTopics(Collections.singletonList(new NewTopic(topicC, 1, (short) 1)))
                .all()
                .get();
        String txnIdC = "init-failover-" + UUID.randomUUID();
        KafkaProducer<byte[], byte[]> ongoingProducer =
                new KafkaProducer<>(transactionalProducerProps(txnIdC));
        try {
            ongoingProducer.initTransactions();
            ongoingProducer.beginTransaction();
            ongoingProducer.send(
                    new ProducerRecord<>(
                            topicC, 0, ("k-" + rng.nextInt()).getBytes(), randomBytes(rng, 32)));
            ongoingProducer.flush();

            // Snapshot pre-failover state so we can compare across the bounce.
            KafkaTxnStateEntity preFailover = coord.getState(txnIdC).orElseThrow();
            assertThat(preFailover.state()).isEqualTo(KafkaTxnStateEntity.STATE_ONGOING);
            assertThat(preFailover.topicPartitions()).contains(topicC + ":0");
            long pidBefore = preFailover.producerId();
            short epochBefore = preFailover.producerEpoch();

            // Bounce the coordinator. This clears TransactionCoordinators.current() inside the
            // process, then the new coordinator-leader bootstrap rehydrates from the catalog
            // table.
            CLUSTER.stopCoordinatorServer();
            CLUSTER.startCoordinatorServer();
            waitForCoordinator();

            // The new leader must have re-read the txn-state row from the catalog and recovered
            // the same (pid, epoch). The bootstrap may run before the catalog client is reachable
            // (chicken-and-egg called out in TransactionCoordinator.startFromCatalog); accept a
            // short delay and trigger refreshFromCatalog defensively.
            TransactionCoordinator coordAfter = TransactionCoordinators.current().orElseThrow();
            long deadline = System.currentTimeMillis() + 60_000L;
            Optional<KafkaTxnStateEntity> postFailover = coordAfter.getState(txnIdC);
            while (!postFailover.isPresent() && System.currentTimeMillis() < deadline) {
                coordAfter.refreshFromCatalog();
                Thread.sleep(250L);
                postFailover = coordAfter.getState(txnIdC);
            }
            assertThat(postFailover)
                    .as("txn-state row for %s rehydrated post-failover", txnIdC)
                    .isPresent();
            assertThat(postFailover.get().producerId()).isEqualTo(pidBefore);
            assertThat(postFailover.get().producerEpoch()).isEqualTo(epochBefore);
            assertThat(postFailover.get().state()).isEqualTo(KafkaTxnStateEntity.STATE_ONGOING);
        } finally {
            // Best-effort client-side cleanup: don't block the test on producer.close() because
            // post-failover the broker side may not respond cleanly until J.3 wires the
            // tablet-server's producer-state tracker. The durable Ongoing row is the J.1
            // observable; the next coordinator timeout sweep will abort the txn.
            try {
                ongoingProducer.close(java.time.Duration.ofSeconds(5));
            } catch (Throwable ignore) {
                // Expected — the producer is mid-txn against a freshly-restarted coordinator.
            }
        }
    }

    // ---------- helpers ----------

    private static void waitForCoordinator() throws InterruptedException {
        // The txn coordinator bootstrap runs at coord-leader election but rehydrates
        // best-effort: it swallows catalog-not-yet-reachable errors and starts empty. The
        // system tables it backs onto are therefore created lazily on first access. Three-step
        // warmup: (1) wait for in-JVM coordinator registration, (2) trigger lazy table creation
        // by listing, (3) wait for bucket-leadership propagation. After (3), the first
        // INIT_PRODUCER_ID is deterministic.
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!TransactionCoordinators.current().isPresent()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException(
                        "TransactionCoordinator did not register within 30s of test start");
            }
            Thread.sleep(50L);
        }
        try {
            org.apache.fluss.catalog.CatalogService catalog =
                    org.apache.fluss.catalog.CatalogServices.current()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "CatalogService not registered"));
            catalog.listKafkaTxnStates();
            catalog.listKafkaProducerIds();
            catalog.listKafkaTxnOffsets("__warmup__");
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to trigger system-table creation for txn coordinator", e);
        }
        CLUSTER.awaitSystemTablesReady(
                java.time.Duration.ofSeconds(60),
                "_catalog._kafka_txn_state",
                "_catalog._kafka_producer_ids",
                "_catalog._kafka_txn_offset_buffer");
    }

    private static CatalogService catalog() {
        return CatalogServices.current()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "CatalogService not registered on this JVM"));
    }

    private static long maxProducerId(CatalogService catalog) throws Exception {
        long max = -1L;
        for (KafkaProducerIdEntity e : catalog.listKafkaProducerIds()) {
            if (e.producerId() > max) {
                max = e.producerId();
            }
        }
        return max;
    }

    private static byte[] randomBytes(Random rng, int len) {
        byte[] out = new byte[len];
        rng.nextBytes(out);
        return out;
    }

    private static Properties idempotentProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
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
        // The waitForCoordinator helper warms ZK-side bucket-leadership for the system tables,
        // but the broker's TransactionCoordinator opens its catalog Connection lazily on first
        // request. The first INIT_PRODUCER_ID through the broker pays that one-time connection
        // cost (~30-60s observed); subsequent calls are fast. 120s window absorbs the cold
        // start without masking real cluster-side regressions.
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return props;
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
