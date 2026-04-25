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

package org.apache.fluss.kafka.tx;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.entities.KafkaProducerIdEntity;
import org.apache.fluss.catalog.entities.KafkaTxnStateEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide transaction coordinator (design 0016 §3) hosted on the elected coordinator leader.
 * Phase J.1 surface: rehydrate state from {@code __kafka_txn_state__} on startup, allocate / fence
 * producer ids on {@code INIT_PRODUCER_ID}, validate {@code (producerId, epoch)} on incoming
 * requests. The five txn-control APIs are still stubbed — they ship in J.2.
 *
 * <p>Thread-safety: backed by a {@link ConcurrentMap} keyed by {@code transactionalId} for the
 * common-path reads / fences. {@link #initProducerId(String, int)} serialises per-id work under a
 * synchronised block on the cache entry, which is acceptable because (a) {@code INIT_PRODUCER_ID}
 * is rare and (b) the contention window is dominated by the catalog round-trip.
 *
 * <p>The coordinator also keeps a session-local atomic for idempotent-only producers (no {@code
 * transactional.id}). These don't need durable state because Kafka semantics already declare the id
 * ephemeral — a re-init from the same client deserves a fresh id.
 */
@Internal
public final class TransactionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCoordinator.class);

    private final CatalogService catalog;

    /** In-memory mirror of {@code __kafka_txn_state__}. */
    private final ConcurrentMap<String, KafkaTxnStateEntity> states = new ConcurrentHashMap<>();

    /**
     * Per-{@code transactional.id} mutex for the {@code INIT_PRODUCER_ID} path. Cheap because
     * concurrent inits on the same id are rare; we use one object per active id and rely on JVM
     * monitor reentrance for nested calls.
     */
    private final ConcurrentMap<String, Object> idLocks = new ConcurrentHashMap<>();

    /**
     * Idempotent-only producer-id pool. Seeded from the durable allocator on first miss; bumped
     * locally per allocation. Idempotent producers don't share a global id with transactional
     * producers — Kafka's semantics declare both pools the same monotonic counter, so we go through
     * the catalog allocator each time. {@link AtomicLong} kept here purely as a defence against
     * future block-allocation work; for J.1 we route every call through the catalog.
     */
    @SuppressWarnings("unused")
    private final AtomicLong idempotentLocalCounter = new AtomicLong(0L);

    private TransactionCoordinator(CatalogService catalog) {
        this.catalog = catalog;
    }

    /**
     * Build a coordinator and rehydrate its in-memory cache from {@code __kafka_txn_state__}.
     * Issues a single {@code listKafkaTxnStates()} scan; returns the populated coordinator. On any
     * scan error we propagate so the bootstrap that called us can fail leadership acquisition
     * cleanly.
     */
    public static TransactionCoordinator startFromCatalog(CatalogService catalog) throws Exception {
        TransactionCoordinator coord = new TransactionCoordinator(catalog);
        for (KafkaTxnStateEntity entity : catalog.listKafkaTxnStates()) {
            coord.states.put(entity.transactionalId(), entity);
        }
        LOG.info(
                "TransactionCoordinator started; rehydrated {} in-flight transactional id(s) "
                        + "from __kafka_txn_state__.",
                coord.states.size());
        return coord;
    }

    /** Number of transactional ids known to the coordinator (in-memory). */
    public int activeTransactionalIdCount() {
        return states.size();
    }

    /**
     * Snapshot view of the current in-memory state for a given {@code transactional.id}, or {@link
     * Optional#empty()} when the id is unknown.
     */
    public Optional<KafkaTxnStateEntity> getState(String transactionalId) {
        return Optional.ofNullable(states.get(transactionalId));
    }

    /** Outcome of {@link #initProducerId(String, int)}. */
    public static final class InitProducerIdResult {
        private final long producerId;
        private final short producerEpoch;
        private final boolean newlyAllocated;

        InitProducerIdResult(long producerId, short producerEpoch, boolean newlyAllocated) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.newlyAllocated = newlyAllocated;
        }

        public long producerId() {
            return producerId;
        }

        public short producerEpoch() {
            return producerEpoch;
        }

        /**
         * True when this call allocated a fresh producer id; false when an existing row was found
         * and the epoch was bumped.
         */
        public boolean newlyAllocated() {
            return newlyAllocated;
        }
    }

    /**
     * Implement {@code INIT_PRODUCER_ID} for a request that carries a {@code transactional.id}. On
     * first call we allocate a producer id from the catalog and persist a fresh row in {@code
     * Empty} state with epoch 0. On a subsequent call with the same id we bump the epoch — fencing
     * the previous producer — and rewrite the row. Phase J.1 keeps the row in {@code Empty} state
     * across the bump; J.2 will add the {@code Ongoing → PrepareAbort} drive-through described in
     * design 0016 §6 path 2.
     *
     * @param transactionalId non-null Kafka {@code transactional.id} from the request
     * @param timeoutMs the client-supplied {@code transaction.timeout.ms}
     */
    public InitProducerIdResult initProducerId(String transactionalId, int timeoutMs)
            throws Exception {
        if (transactionalId == null || transactionalId.isEmpty()) {
            throw new IllegalArgumentException(
                    "transactionalId is required for transactional initProducerId");
        }
        // Per-id mutex so concurrent INIT_PRODUCER_ID on the same id are linearised. Safe to leave
        // entries in the lock map across the call — they're a fixed-size object each and the id
        // population is bounded by the catalog row count.
        Object lock = idLocks.computeIfAbsent(transactionalId, k -> new Object());
        synchronized (lock) {
            KafkaTxnStateEntity existing = states.get(transactionalId);
            if (existing == null) {
                // Cache miss — but the durable row may exist from a prior leader. Re-read so we
                // don't double-allocate a producer id.
                Optional<KafkaTxnStateEntity> durable = catalog.getKafkaTxnState(transactionalId);
                if (durable.isPresent()) {
                    existing = durable.get();
                    states.put(transactionalId, existing);
                }
            }
            if (existing == null) {
                // Path 1: brand-new transactional id.
                KafkaProducerIdEntity allocated = catalog.allocateProducerId(transactionalId);
                long now = System.currentTimeMillis();
                KafkaTxnStateEntity entity =
                        new KafkaTxnStateEntity(
                                transactionalId,
                                allocated.producerId(),
                                (short) 0,
                                KafkaTxnStateEntity.STATE_EMPTY,
                                Collections.emptySet(),
                                timeoutMs,
                                null,
                                now);
                catalog.upsertKafkaTxnState(entity);
                states.put(transactionalId, entity);
                LOG.info(
                        "Allocated producerId={} epoch=0 for transactionalId='{}' (first init)",
                        allocated.producerId(),
                        transactionalId);
                return new InitProducerIdResult(allocated.producerId(), (short) 0, true);
            }
            // Path 2: re-init — bump epoch on the existing row to fence the prior producer.
            short bumped = (short) (existing.producerEpoch() + 1);
            long now = System.currentTimeMillis();
            KafkaTxnStateEntity replacement =
                    new KafkaTxnStateEntity(
                            transactionalId,
                            existing.producerId(),
                            bumped,
                            // J.1: keep state Empty across the bump. J.2 will land the
                            // Ongoing → PrepareAbort drive-through for in-flight transactions.
                            KafkaTxnStateEntity.STATE_EMPTY,
                            Collections.emptySet(),
                            timeoutMs,
                            null,
                            now);
            catalog.upsertKafkaTxnState(replacement);
            states.put(transactionalId, replacement);
            LOG.info(
                    "Re-init transactionalId='{}': producerId={} epoch {}→{} (fence)",
                    transactionalId,
                    existing.producerId(),
                    existing.producerEpoch(),
                    bumped);
            return new InitProducerIdResult(existing.producerId(), bumped, false);
        }
    }

    /**
     * Allocate a producer id for an idempotent-only producer (no {@code transactional.id}). Returns
     * {@code (producerId, epoch=0)}; no durable txn-state row is written because Kafka declares
     * idempotent-only state ephemeral. The producer id itself is logged in {@code
     * __kafka_producer_ids__} for future cross-referencing by {@code DescribeProducers}.
     */
    public InitProducerIdResult initIdempotentProducerId() throws Exception {
        KafkaProducerIdEntity allocated = catalog.allocateProducerId(null);
        return new InitProducerIdResult(allocated.producerId(), (short) 0, true);
    }

    /**
     * Validate that {@code (producerId, epoch)} carried on a request matches the coordinator's
     * known fencing key for {@code transactionalId}. Used by the txn APIs in J.2 / J.3 — exposed in
     * J.1 so the coordinator surface is complete.
     *
     * @return {@code true} when the request's epoch is the one the coordinator currently honours;
     *     {@code false} when the producer is fenced (caller should map to {@code
     *     INVALID_PRODUCER_EPOCH}).
     */
    public boolean isProducerEpochValid(String transactionalId, long producerId, short epoch) {
        KafkaTxnStateEntity entity = states.get(transactionalId);
        if (entity == null) {
            return false;
        }
        return entity.producerId() == producerId && entity.producerEpoch() == epoch;
    }
}
