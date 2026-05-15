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
import org.apache.fluss.kafka.KafkaServerContext;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.log.ClientFetchRequest;
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

        // Read-committed isolation is out of scope for this FIP (transactions/EOS deferred).
        // Always serve UNCOMMITTED bytes regardless of what the client requested — the
        // kafka-clients consumer treats READ_UNCOMMITTED responses as a degraded-mode fetch
        // without raising an error.
        boolean readCommitted = false;

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

        // Aborted-txn snapshot would happen here for read_committed; omitted in this FIP.

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
     * Pick the {@link KafkaFetchCodec} for {@code route}. In this FIP scope every topic serves
     * passthrough bytes; typed topics (Avro/JSON/Protobuf decoded into typed user columns) land in
     * a follow-up FIP at which point the typed-codec dispatch returns here.
     */
    private KafkaFetchCodec pickCodec(
            KafkaTopicRoute route, KafkaTopicInfo topicInfo, TableInfo tableInfo) {
        return new PassthroughKafkaFetchCodec(tableInfo);
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
        private final boolean readCommitted;

        private ClientFetchRequest.BucketRead bucketRead;
        private FetchLogResultForBucket result;
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

        void failWith(Errors err, String msg) {
            this.errorCode = err.code();
            this.errorMessage = msg;
        }

        PartitionData toPartitionData() {
            long highWatermark = result != null ? result.getHighWatermark() : -1L;
            PartitionData pd =
                    new PartitionData()
                            .setPartitionIndex(partitionIndex)
                            .setErrorCode(errorCode)
                            .setHighWatermark(highWatermark)
                            .setLastStableOffset(highWatermark)
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
            try {
                pd.setRecords(
                        encode(
                                result.records(),
                                fetchOffset,
                                timestampType,
                                codec,
                                readCommitted,
                                highWatermark,
                                java.util.Collections.emptyList()));
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
     * <p>The {@code readCommitted}, {@code ceiling}, and aborted-txn list parameters are unused in
     * this FIP (read-committed isolation is a follow-up); kept on the signature so the encode call
     * site stays stable when transactions are reintroduced.
     */
    private static MemoryRecords encode(
            LogRecords flussRecords,
            long baseOffset,
            TimestampType timestampType,
            KafkaFetchCodec codec,
            boolean readCommitted,
            long ceiling,
            List<Object> abortedTxns) {
        // Walk once to count bytes; we'll allocate a single buffer slightly larger than needed.
        List<KafkaRecordView> views = new ArrayList<>();
        long firstOffset = -1L;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long estimatedBytes = 512;
        for (LogRecordBatch batch : flussRecords.batches()) {
            // Read-committed isolation + transactional control-batch handling is out of scope
            // for this FIP (transactions/EOS are a follow-up). All batches are surfaced.
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

    // isAborted() removed — read-committed isolation is out of scope for this FIP.

    // Typed-codec resolution (Avro/JSON/Protobuf decode into typed columns) and the per-topic
    // schema-history getter live in a follow-up FIP. Passthrough fetch is the only path here.
}
