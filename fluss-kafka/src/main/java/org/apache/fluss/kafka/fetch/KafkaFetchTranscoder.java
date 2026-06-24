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

package org.apache.fluss.kafka.fetch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.catalog.CatalogService;
import org.apache.fluss.catalog.CatalogServices;
import org.apache.fluss.catalog.entities.SchemaVersionEntity;
import org.apache.fluss.kafka.KafkaServerContext;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.kafka.sr.typed.CompiledCodecCache;
import org.apache.fluss.kafka.sr.typed.FormatRegistry;
import org.apache.fluss.kafka.sr.typed.FormatTranslator;
import org.apache.fluss.kafka.sr.typed.RecordCodec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.log.ClientFetchRequest;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Transcodes Kafka {@code FetchRequest}s into Fluss {@link ReplicaManager#fetchLogRecords} calls.
 *
 * <p>Each Fluss row is rendered into a Kafka {@link MemoryRecords} batch (RecordBatch v2,
 * uncompressed) by a {@link KafkaFetchCodec} chosen once per partition (design 0014 §5):
 *
 * <ul>
 *   <li>{@link PassthroughKafkaFetchCodec} for the byte-copy path (default; what every topic gets
 *       when {@code kafka.typed-tables.enabled=false}).
 *   <li>{@link TypedKafkaFetchCodec} for {@code KAFKA_TYPED_AVRO/JSON/PROTOBUF} topics — calls the
 *       compiled {@link RecordCodec} and prepends the 5-byte Kafka SR wire frame.
 * </ul>
 *
 * <p>The driver here keeps the change-type filter ({@link ChangeType#UPDATE_BEFORE} skipped, {@link
 * ChangeType#DELETE} → tombstone) and the {@link MemoryRecordsBuilder} bookkeeping; both codec
 * impls share that loop.
 */
@Internal
public final class KafkaFetchTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFetchTranscoder.class);

    /** Default minimum buffer size for rebuilt Kafka batches. */
    private static final int MIN_BATCH_BUFFER_BYTES = 1024;

    private final KafkaServerContext context;
    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;
    private final KafkaTopicRouteResolver routeResolver;

    /**
     * Cache of TYPED routes resolved for each topic. Only typed routes are stored; topics absent
     * from this map are assumed passthrough until resolved. MUST be shared across all fetch
     * requests on the same server — do not create a new transcoder per request or the cache will be
     * discarded after every fetch.
     */
    private final Map<String, KafkaTopicRoute> routeCache;

    /**
     * One per in-flight topic resolution. {@link #resolveAsync} uses {@link
     * ConcurrentHashMap#computeIfAbsent} to ensure exactly one task is submitted per topic per
     * resolution cycle. Completed futures are removed so that passthrough topics can be re-resolved
     * if they later become typed. Typed topics are never re-resolved once cached.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingRoutes;

    /** Single daemon thread used to drive route-resolution catalog scans off the worker threads. */
    private final ExecutorService routeResolvePool;

    /**
     * Per-topic schema cache shared across all fetch requests. Keyed by table full-name, then by
     * Fluss schemaId. Pre-seeded with the current schema on every pickCodec call so that the first
     * ZK miss per (topic, old-schemaId) is the only blocking read.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Schema>> topicSchemaCache =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Schema>>();

    public KafkaFetchTranscoder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this(
                context,
                catalog,
                replicaManager,
                KafkaTopicRouteResolver.fromCatalogServices(
                        context.typedTablesEnabled(), context.kafkaDatabase()),
                new ConcurrentHashMap<String, KafkaTopicRoute>(),
                new ConcurrentHashMap<String, CompletableFuture<Void>>(),
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("kafka-route-resolver")));
    }

    /** Test-only constructor that takes a custom {@link KafkaTopicRouteResolver}. */
    public KafkaFetchTranscoder(
            KafkaServerContext context,
            KafkaTopicsCatalog catalog,
            ReplicaManager replicaManager,
            KafkaTopicRouteResolver routeResolver) {
        this(
                context,
                catalog,
                replicaManager,
                routeResolver,
                new ConcurrentHashMap<String, KafkaTopicRoute>(),
                new ConcurrentHashMap<String, CompletableFuture<Void>>(),
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("kafka-route-resolver")));
    }

    /** Full constructor used by the shared per-server instance in {@link KafkaRequestHandler}. */
    public KafkaFetchTranscoder(
            KafkaServerContext context,
            KafkaTopicsCatalog catalog,
            ReplicaManager replicaManager,
            KafkaTopicRouteResolver routeResolver,
            Map<String, KafkaTopicRoute> routeCache,
            ConcurrentHashMap<String, CompletableFuture<Void>> pendingRoutes,
            ExecutorService routeResolvePool) {
        this.context = context;
        this.catalog = catalog;
        this.replicaManager = replicaManager;
        this.routeResolver = routeResolver;
        this.routeCache = routeCache;
        this.pendingRoutes = pendingRoutes;
        this.routeResolvePool = routeResolvePool;
    }

    public CompletableFuture<FetchResponseData> fetch(FetchRequestData request) {
        // Collect topics whose route hasn't been resolved yet (not in cache). TYPED routes are
        // cached permanently; absent topics are either passthrough or not yet resolved — both
        // require a catalog scan on the routeResolvePool thread before the first fetch can serve
        // correct data. We chain doFetch() on the resolution futures so no worker thread blocks.
        Set<String> unresolvedTopics = new LinkedHashSet<>();
        for (FetchRequestData.FetchTopic topic : request.topics()) {
            if (!routeCache.containsKey(topic.topic())) {
                unresolvedTopics.add(topic.topic());
            }
        }
        if (unresolvedTopics.isEmpty()) {
            return doFetch(request);
        }
        List<CompletableFuture<Void>> resolvingList = new ArrayList<>(unresolvedTopics.size());
        for (String topic : unresolvedTopics) {
            resolvingList.add(resolveAsync(topic));
        }
        final FetchRequestData capturedRequest = request;
        CompletableFuture<Void> allResolved =
                CompletableFuture.allOf(
                        resolvingList.toArray(new CompletableFuture[resolvingList.size()]));
        return allResolved.thenCompose(ignored -> doFetch(capturedRequest));
    }

    /**
     * Submit an async route-resolution task for {@code topic} and return a future that completes
     * (with void) when the resolution finishes. The resolved route is stored in {@link #routeCache}
     * if typed; passthrough results are NOT cached so re-registration via T.3 is picked up on the
     * next fetch cycle. {@link ConcurrentHashMap#computeIfAbsent} ensures at most one task per
     * topic is in flight at any time.
     */
    private CompletableFuture<Void> resolveAsync(final String topic) {
        return pendingRoutes.computeIfAbsent(
                topic,
                t -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    routeResolvePool.submit(
                            () -> {
                                try {
                                    KafkaTopicRoute route = routeResolver.resolve(t);
                                    if (route.isTyped()) {
                                        routeCache.put(t, route);
                                    }
                                    f.complete(null);
                                } catch (Throwable ex) {
                                    f.complete(null);
                                } finally {
                                    pendingRoutes.remove(t);
                                }
                            });
                    return f;
                });
    }

    private CompletableFuture<FetchResponseData> doFetch(FetchRequestData request) {
        FetchResponseData response = new FetchResponseData();
        response.setThrottleTimeMs(0).setErrorCode(Errors.NONE.code());

        // Phase J.3 — honour isolation_level. Kafka encodes the byte as 0=READ_UNCOMMITTED,
        // 1=READ_COMMITTED. When the caller asks for READ_COMMITTED we clamp the response at
        // min(highWatermark, lastStableOffset) and filter aborted batches in the assembled
        // records. UNCOMMITTED retains the existing HWM-clamped behaviour.
        boolean readCommitted = request.isolationLevel() == (byte) 1;

        // First pass: resolve topics + partitions and assemble the Fluss fetch map. We keep a
        // side-map of (TableBucket -> per-partition response shell) so the Fluss callback can
        // populate each shell directly without recomputing lookups.
        Map<TableBucket, PartitionContext> requested = new LinkedHashMap<>();
        Map<String, List<PartitionContext>> pendingByTopic = new LinkedHashMap<>();

        for (FetchTopic topic : request.topics()) {
            String topicName = topic.topic();
            Optional<KafkaTopicInfo> info;
            try {
                info = catalog.lookup(topicName);
            } catch (Exception e) {
                LOG.error("Catalog lookup failed for fetch of '{}'", topicName, e);
                info = Optional.empty();
            }
            if (!info.isPresent()) {
                pendingByTopic.put(
                        topicName, buildErrorShells(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                continue;
            }

            KafkaTopicInfo topicInfo = info.get();
            TimestampType timestampType =
                    topicInfo.timestampType() == KafkaTopicInfo.TimestampType.LOG_APPEND_TIME
                            ? TimestampType.LOG_APPEND_TIME
                            : TimestampType.CREATE_TIME;
            // Load the topic's actual Fluss TableInfo — compacted topics have a different
            // RowType (record_key is non-null + PK) AND a different CDC log format
            // (PK tables default to ARROW). Passing TableInfo into LogRecordReadContext
            // .createReadContext lets the decoder format-branch on ARROW/INDEXED/COMPACTED.
            TableInfo tableInfo;
            try {
                tableInfo = context.metadataManager().getTable(topicInfo.dataTablePath());
            } catch (Exception e) {
                LOG.error(
                        "Failed to load TableInfo for fetch of '{}'", topicInfo.dataTablePath(), e);
                pendingByTopic.put(topicName, buildErrorShells(topic, Errors.UNKNOWN_SERVER_ERROR));
                continue;
            }

            // Route was resolved before doFetch() ran (either from cache or just resolved
            // by the resolveAsync() phase). Absent means passthrough.
            KafkaTopicRoute route =
                    routeCache.containsKey(topicName)
                            ? routeCache.get(topicName)
                            : KafkaTopicRoute.PASSTHROUGH;
            KafkaFetchCodec codec = pickCodec(route, topicInfo, tableInfo);

            List<PartitionContext> contexts = new ArrayList<>(topic.partitions().size());
            pendingByTopic.put(topicName, contexts);
            for (FetchPartition fp : topic.partitions()) {
                PartitionContext ctx =
                        new PartitionContext(
                                topicInfo,
                                tableInfo,
                                fp.partition(),
                                timestampType,
                                fp.fetchOffset(),
                                codec,
                                readCommitted);
                contexts.add(ctx);
                requested.put(new TableBucket(topicInfo.flussTableId(), fp.partition()), ctx);
                ctx.bucketRead =
                        new ClientFetchRequest.BucketRead(
                                topicInfo.flussTableId(),
                                fp.fetchOffset(),
                                fp.partitionMaxBytes() > 0
                                        ? fp.partitionMaxBytes()
                                        : request.maxBytes());
            }
        }

        Map<TableBucket, ClientFetchRequest.BucketRead> bucketReads = new HashMap<>();
        for (Map.Entry<TableBucket, PartitionContext> e : requested.entrySet()) {
            bucketReads.put(e.getKey(), e.getValue().bucketRead);
        }

        // For read_committed fetches, snapshot the aborted-transaction ranges up front (before the
        // async fetch starts). The callback runs on the Netty I/O thread and must not acquire any
        // LogTablet lock, so we do the synchronized snapshot here on the caller's thread and pass
        // the result in. The snapshot uses Long.MAX_VALUE as the upper bound so we capture all
        // currently-known aborted txns; the encode() path filters to the actual LSO ceiling.
        if (readCommitted) {
            for (Map.Entry<TableBucket, PartitionContext> e : requested.entrySet()) {
                long fo = e.getValue().fetchOffset;
                try {
                    List<LogTablet.AbortedTxn> txns =
                            replicaManager
                                    .getReplicaOrException(e.getKey())
                                    .getLogTablet()
                                    .abortedTxnsInRange(fo, Long.MAX_VALUE);
                    e.getValue().setAbortedTxns(txns);
                } catch (Exception ignored) {
                    // Not the leader or bucket unknown; abortedTxns will remain empty.
                }
            }
        }

        CompletableFuture<FetchResponseData> done = new CompletableFuture<>();
        if (bucketReads.isEmpty()) {
            assembleResponse(response, pendingByTopic);
            done.complete(response);
            return done;
        }

        try {
            replicaManager.fetchLogRecordsForClient(
                    new ClientFetchRequest(request.maxBytes(), bucketReads),
                    results -> {
                        for (Map.Entry<TableBucket, FetchLogResultForBucket> entry :
                                results.entrySet()) {
                            PartitionContext ctx = requested.get(entry.getKey());
                            if (ctx == null) {
                                continue;
                            }
                            ctx.populateFrom(entry.getValue());
                        }
                        assembleResponse(response, pendingByTopic);
                        done.complete(response);
                    });
        } catch (Throwable t) {
            LOG.error("fetchLogRecords threw", t);
            for (PartitionContext ctx : requested.values()) {
                ctx.failWith(Errors.UNKNOWN_SERVER_ERROR, t.getMessage());
            }
            assembleResponse(response, pendingByTopic);
            done.complete(response);
        }
        return done;
    }

    /**
     * Pick the right {@link KafkaFetchCodec} for {@code route}. Falls back to passthrough on any
     * resolution failure (codec compile, missing schema row, etc.) — design 0014 §8 says a typed
     * codec failure during fetch is logged at ERROR and the partition serves passthrough bytes; the
     * consumer will see SR-framed bytes verbatim because passthrough is byte-copy.
     */
    private KafkaFetchCodec pickCodec(
            KafkaTopicRoute route, KafkaTopicInfo topicInfo, TableInfo tableInfo) {
        if (!route.isTyped() || !context.typedTablesEnabled()) {
            return new PassthroughKafkaFetchCodec(tableInfo);
        }
        try {
            TypedCodecBinding binding =
                    resolveTypedCodec(route, topicInfo, tableInfo, context.metrics());
            if (binding == null) {
                LOG.warn(
                        "Typed codec unavailable for topic '{}' (format={}); falling back to"
                                + " passthrough",
                        topicInfo.topic(),
                        route.catalogFormat());
                return new PassthroughKafkaFetchCodec(tableInfo);
            }
            SchemaGetter schemaGetter = metadataSchemaGetter(tableInfo);
            return new TypedKafkaFetchCodec(
                    tableInfo, binding.codec, binding.srSchemaId, context.metrics(), schemaGetter);
        } catch (Throwable t) {
            LOG.error(
                    "Typed-fetch codec resolution failed for topic '{}' (format={}); serving"
                            + " passthrough bytes for this batch",
                    topicInfo.topic(),
                    route.catalogFormat(),
                    t);
            return new PassthroughKafkaFetchCodec(tableInfo);
        }
    }

    private static void assembleResponse(
            FetchResponseData response, Map<String, List<PartitionContext>> pending) {
        for (Map.Entry<String, List<PartitionContext>> entry : pending.entrySet()) {
            FetchableTopicResponse topicResp =
                    new FetchableTopicResponse().setTopic(entry.getKey());
            for (PartitionContext ctx : entry.getValue()) {
                topicResp.partitions().add(ctx.toPartitionData());
            }
            response.responses().add(topicResp);
        }
    }

    private static List<PartitionContext> buildErrorShells(FetchTopic topic, Errors error) {
        List<PartitionContext> shells = new ArrayList<>(topic.partitions().size());
        for (FetchPartition p : topic.partitions()) {
            PartitionContext ctx =
                    new PartitionContext(
                            null,
                            null,
                            p.partition(),
                            TimestampType.CREATE_TIME,
                            p.fetchOffset(),
                            null,
                            false);
            ctx.failWith(error, null);
            shells.add(ctx);
        }
        return shells;
    }

    /** Per-partition accumulator that owns both the input shape and the output response. */
    private static final class PartitionContext {
        private final @Nullable KafkaTopicInfo info;
        private final @Nullable TableInfo tableInfo;
        private final int partitionIndex;
        private final TimestampType timestampType;
        private final long fetchOffset;
        private final @Nullable KafkaFetchCodec codec;
        /** Phase J.3 — clamp the response at LSO + filter aborted batches when set. */
        private final boolean readCommitted;

        private ClientFetchRequest.BucketRead bucketRead;
        private FetchLogResultForBucket result;
        private List<LogTablet.AbortedTxn> abortedTxns = new ArrayList<>();
        private short errorCode = Errors.NONE.code();
        private String errorMessage;

        PartitionContext(
                @Nullable KafkaTopicInfo info,
                @Nullable TableInfo tableInfo,
                int partitionIndex,
                TimestampType timestampType,
                long fetchOffset,
                @Nullable KafkaFetchCodec codec,
                boolean readCommitted) {
            this.info = info;
            this.tableInfo = tableInfo;
            this.partitionIndex = partitionIndex;
            this.timestampType = timestampType;
            this.fetchOffset = fetchOffset;
            this.codec = codec;
            this.readCommitted = readCommitted;
        }

        void populateFrom(FetchLogResultForBucket r) {
            this.result = r;
            if (r.failed()) {
                this.errorCode =
                        org.apache.fluss.kafka.KafkaErrors.toKafka(r.getErrorCode()).code();
                this.errorMessage = r.getErrorMessage();
            }
        }

        void setAbortedTxns(List<LogTablet.AbortedTxn> txns) {
            this.abortedTxns = txns;
        }

        void failWith(Errors err, String msg) {
            this.errorCode = err.code();
            this.errorMessage = msg;
        }

        PartitionData toPartitionData() {
            long highWatermark = result != null ? result.getHighWatermark() : -1L;
            long lastStableOffset =
                    result != null && result.getLastStableOffset() >= 0
                            ? result.getLastStableOffset()
                            : highWatermark;
            PartitionData pd =
                    new PartitionData()
                            .setPartitionIndex(partitionIndex)
                            .setErrorCode(errorCode)
                            .setHighWatermark(highWatermark)
                            .setLastStableOffset(lastStableOffset)
                            .setLogStartOffset(-1L)
                            .setPreferredReadReplica(-1);
            if (errorCode != Errors.NONE.code()) {
                pd.setRecords(MemoryRecords.EMPTY);
                return pd;
            }
            if (result == null || result.records() == null || codec == null) {
                pd.setRecords(MemoryRecords.EMPTY);
                return pd;
            }
            if (readCommitted && !abortedTxns.isEmpty()) {
                List<FetchResponseData.AbortedTransaction> kafkaAborted =
                        new ArrayList<>(abortedTxns.size());
                for (LogTablet.AbortedTxn txn : abortedTxns) {
                    kafkaAborted.add(
                            new FetchResponseData.AbortedTransaction()
                                    .setProducerId(txn.writerId())
                                    .setFirstOffset(txn.firstOffset()));
                }
                pd.setAbortedTransactions(kafkaAborted);
            }
            try {
                long ceiling =
                        readCommitted
                                ? Math.min(
                                        highWatermark < 0 ? Long.MAX_VALUE : highWatermark,
                                        lastStableOffset < 0 ? Long.MAX_VALUE : lastStableOffset)
                                : highWatermark;
                pd.setRecords(
                        encode(
                                result.records(),
                                fetchOffset,
                                timestampType,
                                codec,
                                readCommitted,
                                ceiling,
                                abortedTxns));
            } catch (Exception e) {
                LOG.error(
                        "Fetch transcode failed for topic '{}' partition {}",
                        info != null ? info.topic() : "?",
                        partitionIndex,
                        e);
                pd.setErrorCode(Errors.CORRUPT_MESSAGE.code());
                pd.setRecords(MemoryRecords.EMPTY);
            }
            return pd;
        }
    }

    /**
     * Re-encode Fluss rows as a single Kafka RecordBatch v2 (uncompressed). Offsets are derived
     * from the Fluss {@link LogRecord#logOffset()} so consumer offset math matches the producer's
     * base offset.
     *
     * <p>When {@code readCommitted} is true, batches whose first offset is at-or-after {@code
     * ceiling} are dropped; control batches (commit / abort markers) are also dropped. Data batches
     * whose writer-id falls in an aborted range (from {@code abortedTxns}) are also dropped so
     * aborted records never reach the consumer.
     */
    private static MemoryRecords encode(
            LogRecords flussRecords,
            long baseOffset,
            TimestampType timestampType,
            KafkaFetchCodec codec,
            boolean readCommitted,
            long ceiling,
            List<LogTablet.AbortedTxn> abortedTxns) {
        // Walk once to count bytes; we'll allocate a single buffer slightly larger than needed.
        List<KafkaRecordView> views = new ArrayList<>();
        long firstOffset = -1L;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long estimatedBytes = 512;
        for (LogRecordBatch batch : flussRecords.batches()) {
            // Phase J.3 — read-committed filtering.
            if (readCommitted) {
                if (batch.isControlBatch()) {
                    // Marker batches are bookkeeping-only; never surface to consumers.
                    continue;
                }
                if (batch.baseLogOffset() >= ceiling) {
                    // Past the LSO — exclude. Anything still in-flight stays hidden.
                    continue;
                }
                if (isAborted(batch, abortedTxns)) {
                    // This batch belongs to an aborted transaction; hide it from consumers.
                    continue;
                }
            } else {
                // READ_UNCOMMITTED: still skip control batches (they carry no records and would
                // produce empty Kafka records); show data batches regardless of txn state.
                if (batch.isControlBatch()) {
                    continue;
                }
            }
            LogRecordReadContext readCtx = codec.readContext(batch.schemaId());
            try (CloseableIterator<LogRecord> iter = batch.records(readCtx)) {
                while (iter.hasNext()) {
                    LogRecord record = iter.next();
                    if (firstOffset < 0) {
                        firstOffset = record.logOffset();
                    }
                    // Compacted-topic CDC stream emits UPDATE_BEFORE / UPDATE_AFTER / DELETE
                    // alongside APPEND_ONLY. For Kafka semantics we collapse:
                    //   APPEND_ONLY + UPDATE_AFTER → regular record (current value wins)
                    //   UPDATE_BEFORE              → skip (redundant with its UPDATE_AFTER)
                    //   DELETE                     → emit with null value (Kafka tombstone)
                    ChangeType changeType = record.getChangeType();
                    if (changeType == ChangeType.UPDATE_BEFORE) {
                        continue;
                    }
                    InternalRow row = record.getRow();
                    KafkaRecordView view =
                            codec.rowToKafkaRecord(row, changeType, record.logOffset());
                    if (view == null) {
                        // Codec elected to skip the row (already logged); keep the rest of the
                        // batch healthy.
                        continue;
                    }
                    if (view.timestamp > maxTimestamp) {
                        maxTimestamp = view.timestamp;
                    }
                    views.add(view);
                    estimatedBytes += view.estimatedSize();
                }
            }
        }
        if (views.isEmpty()) {
            return MemoryRecords.EMPTY;
        }
        if (firstOffset < 0) {
            firstOffset = baseOffset;
        }

        ByteBuffer buffer =
                ByteBuffer.allocate((int) Math.max(estimatedBytes, MIN_BATCH_BUFFER_BYTES));
        MemoryRecordsBuilder builder =
                MemoryRecords.builder(
                        buffer,
                        RecordBatch.CURRENT_MAGIC_VALUE,
                        Compression.NONE,
                        timestampType,
                        firstOffset);
        try {
            for (KafkaRecordView view : views) {
                builder.appendWithOffset(
                        view.offset, view.timestamp, view.key, view.value, view.headers);
            }
            return builder.build();
        } finally {
            builder.close();
        }
    }

    private static boolean isAborted(LogRecordBatch batch, List<LogTablet.AbortedTxn> abortedTxns) {
        if (abortedTxns.isEmpty() || !batch.hasWriterId()) {
            return false;
        }
        long writerId = batch.writerId();
        long batchOffset = batch.baseLogOffset();
        for (LogTablet.AbortedTxn txn : abortedTxns) {
            if (txn.writerId() == writerId
                    && batchOffset >= txn.firstOffset()
                    && batchOffset < txn.lastOffsetExclusive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolve the typed codec + Kafka SR schema id for a typed-format topic. Returns {@code null}
     * when the catalog lookup or codec compile fails; the caller falls back to passthrough.
     */
    @Nullable
    public static TypedCodecBinding resolveTypedCodec(
            KafkaTopicRoute route,
            KafkaTopicInfo topicInfo,
            TableInfo tableInfo,
            @Nullable KafkaMetricGroup metrics) {
        Optional<CatalogService> maybeCatalog = CatalogServices.current();
        if (!maybeCatalog.isPresent()) {
            return null;
        }
        CatalogService catalog = maybeCatalog.get();
        // Pick the latest registered subject version for this topic. T.6 ('s extension to
        // RecordNameStrategy) is out of scope here; T.2 assumes TopicNameStrategy: one schema
        // per topic value. The latest subject is the one new producers should be framing
        // against.
        SchemaVersionEntity entity;
        try {
            List<SchemaVersionEntity> versions =
                    catalog.listSchemaVersions(extractKafkaDatabase(tableInfo), topicInfo.topic());
            if (versions.isEmpty()) {
                return null;
            }
            entity = versions.get(versions.size() - 1);
        } catch (Exception e) {
            LOG.warn(
                    "Catalog read failed when resolving typed codec for topic '{}'",
                    topicInfo.topic(),
                    e);
            return null;
        }
        FormatTranslator translator = FormatRegistry.instance().translator(entity.format());
        if (translator == null) {
            LOG.error(
                    "No FormatTranslator registered for format '{}' on topic '{}'",
                    entity.format(),
                    topicInfo.topic());
            return null;
        }
        try {
            CompiledCodecCache cache = FormatRegistry.instance().codecCache();
            int srSchemaId = entity.srSchemaId();
            String formatId = route.formatId() == null ? entity.format() : route.formatId();
            RecordCodec codec =
                    cache.getOrCompile(
                            tableInfo.getTableId(),
                            srSchemaId,
                            () -> compileCodec(formatId, entity, translator, tableInfo, metrics));
            return new TypedCodecBinding(codec, srSchemaId);
        } catch (RuntimeException e) {
            LOG.error(
                    "Codec compile failed for topic '{}' format='{}'",
                    topicInfo.topic(),
                    entity.format(),
                    e);
            return null;
        }
    }

    /**
     * Run the format-specific codec compiler. Wired into {@link CompiledCodecCache#getOrCompile} so
     * a successful compile is cached for the life of the cluster.
     */
    private static RecordCodec compileCodec(
            String formatId,
            SchemaVersionEntity entity,
            FormatTranslator translator,
            TableInfo tableInfo,
            @Nullable KafkaMetricGroup metrics) {
        if (metrics != null) {
            metrics.onCodecCompile();
            // Keep the gauge supplier pinned to the live cache. Idempotent.
            metrics.bindCodecCacheSize(() -> FormatRegistry.instance().codecCache().size());
        }
        org.apache.fluss.types.RowType rowType = translator.translateTo(entity.schemaText());
        short version = (short) Math.min(Short.MAX_VALUE, Math.max(0, entity.version()));
        switch (formatId.toUpperCase(Locale.ROOT)) {
            case "AVRO":
                return org.apache.fluss.kafka.sr.typed.AvroCodecCompiler.compile(
                        new org.apache.avro.Schema.Parser().parse(entity.schemaText()),
                        rowType,
                        version);
            case "JSON":
                return org.apache.fluss.kafka.sr.typed.JsonCodecCompiler.compile(rowType, version);
            case "PROTOBUF":
                return org.apache.fluss.kafka.sr.typed.ProtobufCodecCompiler.compile(
                        rowType, version);
            default:
                throw new IllegalArgumentException(
                        "Unsupported codec format: "
                                + formatId
                                + " (table "
                                + tableInfo.getTablePath()
                                + ")");
        }
    }

    /** Extract the Kafka database from {@code tableInfo} (the table's namespace/database). */
    private static String extractKafkaDatabase(TableInfo tableInfo) {
        return tableInfo.getTablePath().getDatabaseName();
    }

    /**
     * Build a {@link SchemaGetter} backed by the coordinator's schema history for {@code
     * tableInfo}. Used by {@link TypedKafkaFetchCodec} so old-schema batches (written before T.3
     * additive evolution) are deserialized with their original schema and then projected to the
     * current layout via {@link org.apache.fluss.row.ProjectedRow}.
     */
    private SchemaGetter metadataSchemaGetter(final TableInfo tableInfo) {
        final MetadataManager mm = context.metadataManager();
        final TablePath tablePath = tableInfo.getTablePath();
        final Schema currentSchema = tableInfo.getSchema();
        final int currentSchemaId = tableInfo.getSchemaId();
        // Retrieve (or create) the per-topic schema map and seed it with the current schema so
        // the common case (batch.schemaId == current) never touches ZooKeeper.
        final ConcurrentHashMap<Integer, Schema> schemaCache =
                topicSchemaCache.computeIfAbsent(
                        tablePath.toString(), k -> new ConcurrentHashMap<Integer, Schema>());
        schemaCache.put(currentSchemaId, currentSchema);
        return new SchemaGetter() {
            @Override
            public Schema getSchema(int schemaId) {
                Schema cached = schemaCache.get(schemaId);
                if (cached != null) {
                    return cached;
                }
                try {
                    Schema s = mm.getSchemaById(tablePath, schemaId).getSchema();
                    schemaCache.put(schemaId, s);
                    return s;
                } catch (Exception e) {
                    LOG.warn(
                            "Schema history lookup failed for '{}' schemaId={}; using current",
                            tablePath,
                            schemaId,
                            e);
                    return currentSchema;
                }
            }

            @Override
            public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int schemaId) {
                return CompletableFuture.completedFuture(
                        new SchemaInfo(getSchema(schemaId), schemaId));
            }

            @Override
            public SchemaInfo getLatestSchemaInfo() {
                return new SchemaInfo(currentSchema, currentSchemaId);
            }

            @Override
            public void release() {}
        };
    }

    /** Pair returned by {@link #resolveTypedCodec}. */
    public static final class TypedCodecBinding {
        public final RecordCodec codec;
        public final int srSchemaId;

        public TypedCodecBinding(RecordCodec codec, int srSchemaId) {
            this.codec = codec;
            this.srSchemaId = srSchemaId;
        }
    }
}
