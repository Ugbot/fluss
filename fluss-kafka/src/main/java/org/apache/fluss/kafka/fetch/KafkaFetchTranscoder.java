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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
 *       compiled {@link RecordCodec} and prepends the 5-byte Confluent frame.
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

    public KafkaFetchTranscoder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this(
                context,
                catalog,
                replicaManager,
                KafkaTopicRouteResolver.fromCatalogServices(
                        context.typedTablesEnabled(), context.kafkaDatabase()));
    }

    /** Test-only constructor that takes a custom {@link KafkaTopicRouteResolver}. */
    public KafkaFetchTranscoder(
            KafkaServerContext context,
            KafkaTopicsCatalog catalog,
            ReplicaManager replicaManager,
            KafkaTopicRouteResolver routeResolver) {
        this.context = context;
        this.catalog = catalog;
        this.replicaManager = replicaManager;
        this.routeResolver = routeResolver;
    }

    public CompletableFuture<FetchResponseData> fetch(FetchRequestData request) {
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

            // Resolve the topic route once, then resolve a codec for that route. Per design
            // 0014 §3 the decision is per-topic-per-call; cache on the PartitionContext.
            KafkaTopicRoute route = routeResolver.resolve(topicName);
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
     * consumer will see Confluent-framed bytes verbatim because passthrough is byte-copy.
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
            return new TypedKafkaFetchCodec(
                    tableInfo, binding.codec, binding.confluentSchemaId, context.metrics());
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
                                ceiling));
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
     * ceiling} are dropped; control batches (commit / abort markers) are also dropped. Aborted data
     * batches before the LSO are filtered out — recognised by the writer-id matching an
     * aborted-range entry in {@link org.apache.fluss.server.log.LogTablet#abortedTxnsInRange(long,
     * long)}, but as that map is not surfaced through the wire entity yet (deferred for follow-on
     * PR), the conservative client-visible filter is "drop control batches; clamp at LSO". The
     * LogTablet still filters at append/marker time so commit-vs-abort semantics are correct
     * end-to-end for producers that don't reuse writer-ids across aborted/committed transactions.
     */
    private static MemoryRecords encode(
            LogRecords flussRecords,
            long baseOffset,
            TimestampType timestampType,
            KafkaFetchCodec codec,
            boolean readCommitted,
            long ceiling) {
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

    /**
     * Resolve the typed codec + Confluent id for a typed-format topic. Returns {@code null} when
     * the catalog lookup or codec compile fails; the caller falls back to passthrough.
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
            int confluentId = entity.confluentId();
            String formatId = route.formatId() == null ? entity.format() : route.formatId();
            RecordCodec codec =
                    cache.getOrCompile(
                            tableInfo.getTableId(),
                            confluentId,
                            () -> compileCodec(formatId, entity, translator, tableInfo, metrics));
            return new TypedCodecBinding(codec, confluentId);
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

    /** Pair returned by {@link #resolveTypedCodec}. */
    public static final class TypedCodecBinding {
        public final RecordCodec codec;
        public final int confluentSchemaId;

        public TypedCodecBinding(RecordCodec codec, int confluentSchemaId) {
            this.codec = codec;
            this.confluentSchemaId = confluentSchemaId;
        }
    }
}
