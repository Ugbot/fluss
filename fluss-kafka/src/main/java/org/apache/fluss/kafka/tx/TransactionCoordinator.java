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
import org.apache.fluss.catalog.entities.KafkaTxnOffsetBufferEntity;
import org.apache.fluss.catalog.entities.KafkaTxnStateEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide transaction coordinator (design 0016 §3) hosted on the elected coordinator leader.
 *
 * <p>Phase J.1 surface: rehydrate state from {@code __kafka_txn_state__} on startup, allocate /
 * fence producer ids on {@code INIT_PRODUCER_ID}, validate {@code (producerId, epoch)} on incoming
 * requests.
 *
 * <p>Phase J.2 surface: the five transactional wire APIs ({@code ADD_PARTITIONS_TO_TXN}, {@code
 * ADD_OFFSETS_TO_TXN}, {@code END_TXN}, {@code WRITE_TXN_MARKERS}, {@code TXN_OFFSET_COMMIT}) plus
 * the two observability APIs ({@code DESCRIBE_TRANSACTIONS}, {@code LIST_TRANSACTIONS}). State
 * machine transitions match design 0016 §4: Empty → Ongoing on the first ADD_*, Ongoing →
 * PrepareCommit / PrepareAbort on END_TXN, then through the synchronous marker shim to
 * CompleteCommit / CompleteAbort → Empty. Every transition writes {@code __kafka_txn_state__}
 * before any side-effect — durable-before-action.
 *
 * <p>Phase J.2 deferrals (called out in the design and in the per-method Javadoc):
 *
 * <ul>
 *   <li>The {@code WRITE_TXN_MARKERS} fan-out does not yet append a control batch to {@code
 *       LogTablet}. The marker is recorded in {@code __kafka_txn_state__} only; the LogTablet
 *       producer-state tracker and the LSO cursor land in J.3.
 *   <li>The {@code group_ids} field on {@code __kafka_txn_state__} is not yet a column on the row;
 *       group bindings live in memory. They survive in-process restarts of the coordinator (the
 *       state row is enough to know there is an open txn and the marker fan-out is idempotent), but
 *       not full coordinator-leader failovers. J.3 promotes the field to durable.
 *   <li>The {@code TXN_OFFSET_COMMIT} buffer is in-memory ({@link TxnOffsetBuffer}). On {@code
 *       END_TXN(commit)} the buffered offsets are returned to the caller so the broker can flush
 *       them via the existing {@link org.apache.fluss.kafka.group.OffsetStore}; on abort they are
 *       dropped. J.3 promotes the buffer to {@code __kafka_txn_offset_buffer__} for crash
 *       durability.
 * </ul>
 *
 * <p>Thread-safety: backed by a {@link ConcurrentMap} keyed by {@code transactionalId} for the
 * common-path reads / fences. State transitions on a single {@code transactional.id} serialise
 * under the per-id lock obtained from {@link #lockFor(String)} so concurrent ADD_/END_TXN/INIT
 * calls on the same id are linearised.
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
     * Group bindings registered via {@code ADD_OFFSETS_TO_TXN}. Per design 0016 §5 these belong on
     * the state row in the {@code group_ids} column; in J.2 they live in memory only and survive
     * in-process restarts via the next ADD_OFFSETS_TO_TXN replay from the client. J.3 promotes the
     * column.
     */
    private final ConcurrentMap<String, Set<String>> groupBindings = new ConcurrentHashMap<>();

    /**
     * Buffered offsets registered via {@code TXN_OFFSET_COMMIT}, flushed on {@code END_TXN(commit)}
     * and dropped on {@code END_TXN(abort)}. Keyed by {@code (transactionalId, groupId)}.
     */
    private final TxnOffsetBuffer offsetBuffer = new TxnOffsetBuffer();

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

    /**
     * Phase J.3 — marker-fanout sink. The coordinator invokes {@link #append} once per "{@code
     * topic:partition}" entry registered against the closing transaction; the sink resolves the
     * bucket and writes a control batch via {@link
     * org.apache.fluss.server.replica.ReplicaManager#appendTxnMarker}. {@code null} sinks (e.g.
     * during the J.1 / J.2 unit-test wiring) cause the fanout to be a no-op — the state-machine
     * still walks every transition so the coordinator-side correctness is preserved.
     */
    @FunctionalInterface
    public interface MarkerSink {
        void append(String topic, int partition, long producerId, short epoch, boolean commit)
                throws Exception;
    }

    /** Optional fan-out target; lazily registered by the Kafka request-handler bootstrap. */
    @Nullable private volatile MarkerSink markerSink;

    private TransactionCoordinator(CatalogService catalog) {
        this.catalog = catalog;
    }

    /**
     * Phase J.3 — register the {@link MarkerSink} the coordinator should use to fan out {@code
     * WRITE_TXN_MARKERS}. Idempotent; later registrations replace earlier ones (single
     * coordinator-leader, single sink at a time).
     */
    public void setMarkerSink(@Nullable MarkerSink sink) {
        this.markerSink = sink;
    }

    /**
     * Build a coordinator and rehydrate its in-memory cache from {@code __kafka_txn_state__}.
     * Issues a single {@code listKafkaTxnStates()} scan; returns the populated coordinator.
     *
     * <p>The scan is best-effort: the bootstrap runs during leader-election callbacks at a point
     * where the Fluss client {@link org.apache.fluss.client.Connection} cannot yet be opened
     * (because the elected coordinator hasn't finished announcing itself). When the scan throws we
     * log and start the coordinator with an empty in-memory cache; the next ADD_PARTITIONS_TO_TXN
     * or INIT_PRODUCER_ID call rehydrates lazily via {@link #refreshFromCatalog()}.
     */
    public static TransactionCoordinator startFromCatalog(CatalogService catalog) throws Exception {
        TransactionCoordinator coord = new TransactionCoordinator(catalog);
        try {
            for (KafkaTxnStateEntity entity : catalog.listKafkaTxnStates()) {
                coord.states.put(entity.transactionalId(), entity);
            }
            LOG.info(
                    "TransactionCoordinator started; rehydrated {} in-flight transactional id(s) "
                            + "from __kafka_txn_state__.",
                    coord.states.size());
        } catch (Throwable t) {
            // Catalog client not yet ready (chicken-and-egg with leader-election bootstrap). The
            // coordinator stands up empty; lazy refresh on first request fills the cache.
            LOG.info(
                    "TransactionCoordinator started without rehydration (catalog not yet "
                            + "reachable: {}); state will be lazily rebuilt on first request.",
                    t.toString());
        }
        return coord;
    }

    /**
     * Lazy refresh from the catalog. Callable by tooling or by a future failover-recovery hook when
     * the in-memory cache is suspected stale; idempotent and safe to call from multiple threads —
     * the underlying state map uses last-writer-wins on the per-id slot.
     */
    public synchronized void refreshFromCatalog() {
        try {
            for (KafkaTxnStateEntity entity : catalog.listKafkaTxnStates()) {
                states.put(entity.transactionalId(), entity);
            }
        } catch (Throwable t) {
            LOG.warn("Lazy refresh of TransactionCoordinator state failed: {}", t.toString());
        }
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

    /**
     * Snapshot view of every active transactional id known to the coordinator. Used by {@code
     * LIST_TRANSACTIONS}. The returned list is a defensive copy; iteration is safe under concurrent
     * mutation.
     */
    public List<KafkaTxnStateEntity> listAll() {
        return new ArrayList<>(states.values());
    }

    /**
     * Group bindings registered for {@code transactionalId} via {@code ADD_OFFSETS_TO_TXN}. Returns
     * an empty set when no bindings exist.
     */
    public Set<String> groupBindings(String transactionalId) {
        Set<String> bound = groupBindings.get(transactionalId);
        return bound == null ? Collections.emptySet() : new TreeSet<>(bound);
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
        Object lock = lockFor(transactionalId);
        synchronized (lock) {
            // The startup scan rehydrated state when the catalog was reachable; if it wasn't
            // (chicken-and-egg with leader-election), the in-memory cache is the source of truth
            // for the lifetime of this leader. A fresh leader without bootstrap state will
            // allocate a new producer id for any in-flight transactional.id, which the client
            // handles as a fence (the old epoch is rejected on first request).
            KafkaTxnStateEntity existing = states.get(transactionalId);
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
            // If an in-flight transaction is open under the previous epoch, drive it through
            // PrepareAbort → CompleteAbort → Empty before bumping. This is the fencing path from
            // design 0016 §6 path 2: the returning client must observe a clean slate.
            if (KafkaTxnStateEntity.STATE_ONGOING.equals(existing.state())
                    || KafkaTxnStateEntity.STATE_PREPARE_COMMIT.equals(existing.state())
                    || KafkaTxnStateEntity.STATE_PREPARE_ABORT.equals(existing.state())) {
                LOG.info(
                        "Re-init transactionalId='{}' found state={}; driving to abort before "
                                + "epoch bump",
                        transactionalId,
                        existing.state());
                writeState(
                        existing,
                        KafkaTxnStateEntity.STATE_PREPARE_ABORT,
                        existing.topicPartitions(),
                        existing.txnStartTimestampMillis(),
                        now);
                // J.2: the marker fan-out is a no-op on the LogTablet side (J.3 dependency); just
                // walk the state machine to Empty so the new epoch starts clean.
                applyMarkersAndComplete(transactionalId, /* commit */ false);
                existing = states.get(transactionalId);
            }
            KafkaTxnStateEntity replacement =
                    new KafkaTxnStateEntity(
                            transactionalId,
                            existing.producerId(),
                            bumped,
                            KafkaTxnStateEntity.STATE_EMPTY,
                            Collections.emptySet(),
                            timeoutMs,
                            null,
                            now);
            catalog.upsertKafkaTxnState(replacement);
            states.put(transactionalId, replacement);
            // The old epoch's group bindings + offset buffer are gone (the abort path cleared
            // them). Defensive scrub in case we hit Path 2 from Empty without an abort.
            groupBindings.remove(transactionalId);
            offsetBuffer.dropAllFor(transactionalId);
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

    // ---------------------------------------------------------------------
    // Phase J.2 — the five transactional wire APIs.
    // ---------------------------------------------------------------------

    /** Outcome of an epoch validation; used by handlers to convert to Kafka error codes. */
    public enum EpochCheck {
        OK,
        UNKNOWN_TRANSACTIONAL_ID,
        INVALID_PRODUCER_EPOCH
    }

    /**
     * Check {@code (producerId, epoch)} against the stored row; the handler maps the result to
     * either {@code Errors.NONE}, {@code Errors.INVALID_PRODUCER_MAPPING} or {@code
     * Errors.INVALID_PRODUCER_EPOCH}.
     */
    public EpochCheck checkEpoch(String transactionalId, long producerId, short epoch) {
        KafkaTxnStateEntity entity = states.get(transactionalId);
        if (entity == null) {
            return EpochCheck.UNKNOWN_TRANSACTIONAL_ID;
        }
        if (entity.producerId() != producerId || entity.producerEpoch() != epoch) {
            return EpochCheck.INVALID_PRODUCER_EPOCH;
        }
        return EpochCheck.OK;
    }

    /**
     * Implement {@code ADD_PARTITIONS_TO_TXN}. Validates the fencing key, then transitions {@code
     * Empty → Ongoing} (with an initial partition set) or extends the partition set on an
     * already-{@code Ongoing} row. Persists the row before returning.
     *
     * @param transactionalId non-null
     * @param producerId fencing key from the request
     * @param epoch fencing key from the request
     * @param topicPartitions new "topic:partition" entries to register; may overlap the existing
     *     set (idempotent)
     */
    public EpochCheck addPartitionsToTxn(
            String transactionalId, long producerId, short epoch, Set<String> topicPartitions)
            throws Exception {
        Object lock = lockFor(transactionalId);
        synchronized (lock) {
            EpochCheck check = checkEpoch(transactionalId, producerId, epoch);
            if (check != EpochCheck.OK) {
                return check;
            }
            KafkaTxnStateEntity existing = states.get(transactionalId);
            long now = System.currentTimeMillis();
            Set<String> merged = new TreeSet<>(existing.topicPartitions());
            if (topicPartitions != null) {
                merged.addAll(topicPartitions);
            }
            Long startTs =
                    KafkaTxnStateEntity.STATE_EMPTY.equals(existing.state())
                            ? now
                            : existing.txnStartTimestampMillis();
            writeState(existing, KafkaTxnStateEntity.STATE_ONGOING, merged, startTs, now);
            return EpochCheck.OK;
        }
    }

    /**
     * Implement {@code ADD_OFFSETS_TO_TXN}. Validates the fencing key, registers the consumer group
     * binding for this transaction, and transitions {@code Empty → Ongoing} (with an empty
     * partition set) so the in-flight txn carries the binding. The group binding is held in memory
     * (J.2 deferral; see class Javadoc).
     */
    public EpochCheck addOffsetsToTxn(
            String transactionalId, long producerId, short epoch, String groupId) throws Exception {
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("groupId is required for addOffsetsToTxn");
        }
        Object lock = lockFor(transactionalId);
        synchronized (lock) {
            EpochCheck check = checkEpoch(transactionalId, producerId, epoch);
            if (check != EpochCheck.OK) {
                return check;
            }
            KafkaTxnStateEntity existing = states.get(transactionalId);
            long now = System.currentTimeMillis();
            Long startTs =
                    KafkaTxnStateEntity.STATE_EMPTY.equals(existing.state())
                            ? now
                            : existing.txnStartTimestampMillis();
            // Phase J.3: durable group binding via the state row's group_ids column.
            Set<String> mergedGroups = new TreeSet<>(existing.groupIds());
            mergedGroups.add(groupId);
            writeStateWithGroups(
                    existing,
                    KafkaTxnStateEntity.STATE_ONGOING,
                    existing.topicPartitions(),
                    mergedGroups,
                    startTs,
                    now);
            groupBindings
                    .computeIfAbsent(transactionalId, k -> ConcurrentHashMap.newKeySet())
                    .add(groupId);
            return EpochCheck.OK;
        }
    }

    /** Buffered offset entry; flushed atomically with the txn commit. */
    public static final class BufferedOffset {
        private final String groupId;
        private final String topic;
        private final int partition;
        private final long offset;
        private final int leaderEpoch;
        @Nullable private final String metadata;

        public BufferedOffset(
                String groupId,
                String topic,
                int partition,
                long offset,
                int leaderEpoch,
                @Nullable String metadata) {
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.metadata = metadata;
        }

        public String groupId() {
            return groupId;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        public int leaderEpoch() {
            return leaderEpoch;
        }

        @Nullable
        public String metadata() {
            return metadata;
        }
    }

    /**
     * Implement {@code TXN_OFFSET_COMMIT}. Validates the fencing key, then buffers the offsets
     * keyed by {@code (transactionalId, groupId)}. The offsets are not written to {@code
     * __consumer_offsets__} until {@link #endTxn(String, long, short, boolean)} is called with
     * {@code commit=true}.
     */
    public EpochCheck txnOffsetCommit(
            String transactionalId,
            long producerId,
            short epoch,
            String groupId,
            List<BufferedOffset> offsets) {
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("groupId is required for txnOffsetCommit");
        }
        Object lock = lockFor(transactionalId);
        synchronized (lock) {
            EpochCheck check = checkEpoch(transactionalId, producerId, epoch);
            if (check != EpochCheck.OK) {
                return check;
            }
            // Phase J.3 — write through to the durable buffer in addition to the in-memory map.
            // The durable buffer is the source of truth for crash-recovery; the in-memory map is
            // a fast-path read used by the same-leader END_TXN flush. On a coord-leader failover
            // the new leader will re-read the durable rows on demand (listKafkaTxnOffsets).
            offsetBuffer.add(transactionalId, groupId, offsets);
            if (offsets != null) {
                long now = System.currentTimeMillis();
                for (BufferedOffset o : offsets) {
                    try {
                        catalog.upsertKafkaTxnOffset(
                                new KafkaTxnOffsetBufferEntity(
                                        transactionalId,
                                        groupId,
                                        o.topic(),
                                        o.partition(),
                                        o.offset(),
                                        o.leaderEpoch(),
                                        o.metadata(),
                                        now));
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to durably buffer TXN_OFFSET_COMMIT for txn='{}' group='{}'"
                                        + " topic='{}' partition={} offset={}",
                                transactionalId,
                                groupId,
                                o.topic(),
                                o.partition(),
                                o.offset(),
                                e);
                    }
                }
            }
            // Implicitly bind the group: TXN_OFFSET_COMMIT without a prior ADD_OFFSETS_TO_TXN is
            // protocol-illegal but harmless in practice — record the binding so observability
            // reports it correctly.
            groupBindings
                    .computeIfAbsent(transactionalId, k -> ConcurrentHashMap.newKeySet())
                    .add(groupId);
            return EpochCheck.OK;
        }
    }

    /** Outcome of {@link #endTxn}. Carries any buffered offsets the caller must flush. */
    public static final class EndTxnResult {
        private final EpochCheck epoch;
        private final boolean committed;
        private final List<BufferedOffset> bufferedOffsets;

        EndTxnResult(EpochCheck epoch, boolean committed, List<BufferedOffset> bufferedOffsets) {
            this.epoch = epoch;
            this.committed = committed;
            this.bufferedOffsets = bufferedOffsets;
        }

        public EpochCheck epoch() {
            return epoch;
        }

        public boolean committed() {
            return committed;
        }

        /**
         * Offsets to flush to {@code __consumer_offsets__} on a successful commit. Empty on abort
         * and on an epoch-failed call.
         */
        public List<BufferedOffset> bufferedOffsets() {
            return bufferedOffsets;
        }
    }

    /**
     * Implement {@code END_TXN}. Validates the fencing key, persists the {@code PrepareCommit /
     * PrepareAbort} state, walks the synchronous marker shim to {@code CompleteCommit /
     * CompleteAbort → Empty}, and returns the buffered offsets the broker must flush on commit
     * (empty on abort).
     *
     * <p>The marker fan-out to {@link org.apache.fluss.server.log.LogTablet} is a no-op in J.2 —
     * see class Javadoc. The state machine still walks every transition so observability and
     * coordinator failover are correct.
     */
    public EndTxnResult endTxn(String transactionalId, long producerId, short epoch, boolean commit)
            throws Exception {
        Object lock = lockFor(transactionalId);
        synchronized (lock) {
            EpochCheck check = checkEpoch(transactionalId, producerId, epoch);
            if (check != EpochCheck.OK) {
                return new EndTxnResult(check, commit, Collections.emptyList());
            }
            KafkaTxnStateEntity existing = states.get(transactionalId);
            long now = System.currentTimeMillis();
            String prepareState =
                    commit
                            ? KafkaTxnStateEntity.STATE_PREPARE_COMMIT
                            : KafkaTxnStateEntity.STATE_PREPARE_ABORT;
            writeState(
                    existing,
                    prepareState,
                    existing.topicPartitions(),
                    existing.txnStartTimestampMillis(),
                    now);
            List<BufferedOffset> toFlush;
            if (commit) {
                // Read the durable buffer first (catalog is the source of truth across leader
                // failovers); fall back to the in-memory map for the same-leader fast-path.
                List<BufferedOffset> durable = readDurableBuffer(transactionalId);
                List<BufferedOffset> inMem = offsetBuffer.drainAllFor(transactionalId);
                toFlush = durable.isEmpty() ? inMem : durable;
            } else {
                toFlush = Collections.emptyList();
                offsetBuffer.dropAllFor(transactionalId);
            }
            // Drop the durable buffer rows on either commit (after collection) or abort.
            try {
                catalog.deleteKafkaTxnOffsets(transactionalId);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to clean up __kafka_txn_offset_buffer__ for txn='{}': {}",
                        transactionalId,
                        e.toString());
            }
            applyMarkersAndComplete(transactionalId, commit);
            return new EndTxnResult(EpochCheck.OK, commit, toFlush);
        }
    }

    private List<BufferedOffset> readDurableBuffer(String transactionalId) {
        try {
            List<KafkaTxnOffsetBufferEntity> rows = catalog.listKafkaTxnOffsets(transactionalId);
            if (rows.isEmpty()) {
                return Collections.emptyList();
            }
            List<BufferedOffset> out = new ArrayList<>(rows.size());
            for (KafkaTxnOffsetBufferEntity e : rows) {
                out.add(
                        new BufferedOffset(
                                e.groupId(),
                                e.topic(),
                                e.partition(),
                                e.offset(),
                                e.leaderEpoch(),
                                e.metadata()));
            }
            return out;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read durable txn-offset buffer for txn='{}': {}",
                    transactionalId,
                    e.toString());
            return Collections.emptyList();
        }
    }

    /**
     * Apply the synchronous marker shim and walk the final transitions {@code PrepareX → CompleteX
     * → Empty}. The marker fan-out to LogTablet is a no-op in J.2; J.3 wires it up.
     *
     * <p>Caller must hold {@link #lockFor(String)}.
     */
    private void applyMarkersAndComplete(String transactionalId, boolean commit) throws Exception {
        KafkaTxnStateEntity existing = states.get(transactionalId);
        long now = System.currentTimeMillis();
        // Phase J.3 — fan markers out across every participating partition. Failures are logged
        // and skipped; the marker is idempotent on the LogTablet side (open-txn entry removed on
        // first matching control batch) so a retry from a future leader is harmless.
        MarkerSink sink = markerSink;
        if (sink != null) {
            for (String tp : existing.topicPartitions()) {
                int sep = tp.lastIndexOf(':');
                if (sep <= 0) {
                    continue;
                }
                String topic = tp.substring(0, sep);
                int partition;
                try {
                    partition = Integer.parseInt(tp.substring(sep + 1));
                } catch (NumberFormatException nfe) {
                    continue;
                }
                try {
                    sink.append(
                            topic,
                            partition,
                            existing.producerId(),
                            existing.producerEpoch(),
                            commit);
                } catch (Exception e) {
                    LOG.warn(
                            "Marker fan-out for txn='{}' topic='{}' partition={} failed: {}",
                            transactionalId,
                            topic,
                            partition,
                            e.toString());
                }
            }
        }
        String completeState =
                commit
                        ? KafkaTxnStateEntity.STATE_COMPLETE_COMMIT
                        : KafkaTxnStateEntity.STATE_COMPLETE_ABORT;
        writeState(
                existing,
                completeState,
                existing.topicPartitions(),
                existing.txnStartTimestampMillis(),
                now);
        // Final transition: cleanup row → Empty, partitions cleared, group bindings cleared.
        existing = states.get(transactionalId);
        long cleanupAt = System.currentTimeMillis();
        KafkaTxnStateEntity cleared =
                new KafkaTxnStateEntity(
                        transactionalId,
                        existing.producerId(),
                        existing.producerEpoch(),
                        KafkaTxnStateEntity.STATE_EMPTY,
                        Collections.emptySet(),
                        existing.timeoutMs(),
                        null,
                        cleanupAt);
        catalog.upsertKafkaTxnState(cleared);
        states.put(transactionalId, cleared);
        groupBindings.remove(transactionalId);
    }

    /**
     * Locally-served {@code WRITE_TXN_MARKERS} handler. Kafka exposes this RPC so the txn
     * coordinator can fan markers out to participating brokers; in our single-process bolt-on world
     * it is invoked by {@link #endTxn} directly. The wire-API handler exists for protocol
     * completeness and accepts the request from a peer-broker emulation, then no-ops on the log
     * layer (see {@link #applyMarkersAndComplete}). The state row is updated only when the incoming
     * markers reference a transactional id this coordinator owns; otherwise the call is a no-op
     * acknowledged with {@code Errors.NONE}.
     */
    public boolean acknowledgeMarker(
            long producerId, short producerEpoch, boolean transactionResult) {
        // Find the txn id whose state row matches; markers are addressed by (pid, epoch) only.
        for (Map.Entry<String, KafkaTxnStateEntity> e : states.entrySet()) {
            KafkaTxnStateEntity row = e.getValue();
            if (row.producerId() == producerId && row.producerEpoch() == producerEpoch) {
                LOG.debug(
                        "Acknowledged WRITE_TXN_MARKERS for txn='{}' pid={} epoch={} result={}",
                        e.getKey(),
                        producerId,
                        producerEpoch,
                        transactionResult);
                return true;
            }
        }
        return false;
    }

    /**
     * Persist a state-row replacement and update the in-memory cache. Caller must already hold the
     * per-id lock.
     */
    private void writeState(
            KafkaTxnStateEntity previous,
            String newState,
            Set<String> partitions,
            @Nullable Long startTimestampMillis,
            long lastUpdatedMillis)
            throws Exception {
        writeStateWithGroups(
                previous,
                newState,
                partitions,
                previous.groupIds(),
                startTimestampMillis,
                lastUpdatedMillis);
    }

    /** Phase J.3 — write a state row and update its durable {@code group_ids} column. */
    private void writeStateWithGroups(
            KafkaTxnStateEntity previous,
            String newState,
            Set<String> partitions,
            Set<String> groupIds,
            @Nullable Long startTimestampMillis,
            long lastUpdatedMillis)
            throws Exception {
        KafkaTxnStateEntity replacement =
                new KafkaTxnStateEntity(
                        previous.transactionalId(),
                        previous.producerId(),
                        previous.producerEpoch(),
                        newState,
                        partitions,
                        groupIds,
                        previous.timeoutMs(),
                        startTimestampMillis,
                        lastUpdatedMillis);
        catalog.upsertKafkaTxnState(replacement);
        states.put(previous.transactionalId(), replacement);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "txn-state: txn='{}' {}→{} pid={} epoch={} partitions={}",
                    previous.transactionalId(),
                    previous.state(),
                    newState,
                    previous.producerId(),
                    previous.producerEpoch(),
                    partitions);
        }
    }

    private Object lockFor(String transactionalId) {
        return idLocks.computeIfAbsent(transactionalId, k -> new Object());
    }

    /**
     * In-memory buffer for {@code TXN_OFFSET_COMMIT}. Keyed by {@code (transactionalId, groupId)};
     * each entry holds an ordered map from {@code (topic, partition)} to the latest buffered offset
     * (later commits overwrite earlier ones, matching Kafka semantics).
     */
    private static final class TxnOffsetBuffer {

        private final ConcurrentMap<BufferKey, Map<TopicPartitionKey, BufferedOffset>> buffers =
                new ConcurrentHashMap<>();

        void add(String transactionalId, String groupId, List<BufferedOffset> offsets) {
            if (offsets == null || offsets.isEmpty()) {
                return;
            }
            BufferKey k = new BufferKey(transactionalId, groupId);
            Map<TopicPartitionKey, BufferedOffset> bucket =
                    buffers.computeIfAbsent(k, key -> new LinkedHashMap<>());
            synchronized (bucket) {
                for (BufferedOffset o : offsets) {
                    bucket.put(new TopicPartitionKey(o.topic(), o.partition()), o);
                }
            }
        }

        List<BufferedOffset> drainAllFor(String transactionalId) {
            List<BufferedOffset> out = new ArrayList<>();
            buffers.entrySet()
                    .removeIf(
                            e -> {
                                if (!e.getKey().transactionalId.equals(transactionalId)) {
                                    return false;
                                }
                                synchronized (e.getValue()) {
                                    out.addAll(e.getValue().values());
                                }
                                return true;
                            });
            return out;
        }

        void dropAllFor(String transactionalId) {
            buffers.entrySet().removeIf(e -> e.getKey().transactionalId.equals(transactionalId));
        }

        private static final class BufferKey {
            final String transactionalId;
            final String groupId;

            BufferKey(String transactionalId, String groupId) {
                this.transactionalId = transactionalId;
                this.groupId = groupId;
            }

            @Override
            public boolean equals(Object o) {
                if (!(o instanceof BufferKey)) {
                    return false;
                }
                BufferKey that = (BufferKey) o;
                return transactionalId.equals(that.transactionalId) && groupId.equals(that.groupId);
            }

            @Override
            public int hashCode() {
                return transactionalId.hashCode() * 31 + groupId.hashCode();
            }
        }

        private static final class TopicPartitionKey {
            final String topic;
            final int partition;

            TopicPartitionKey(String topic, int partition) {
                this.topic = topic;
                this.partition = partition;
            }

            @Override
            public boolean equals(Object o) {
                if (!(o instanceof TopicPartitionKey)) {
                    return false;
                }
                TopicPartitionKey that = (TopicPartitionKey) o;
                return partition == that.partition && topic.equals(that.topic);
            }

            @Override
            public int hashCode() {
                return topic.hashCode() * 31 + partition;
            }
        }
    }
}
