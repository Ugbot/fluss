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
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.log.ClientFetchRequest;
import org.apache.fluss.server.replica.ReplicaManager;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Transcodes Kafka {@code FetchRequest}s into Fluss {@link ReplicaManager#fetchLogRecords} calls.
 *
 * <p>Each Fluss row in the returned {@link LogRecords} is read as the {@link KafkaDataTable} shape,
 * then packed into a Kafka {@link MemoryRecords} batch (RecordBatch v2, uncompressed). Per topic we
 * respect the catalog-stored timestamp type so the re-encoded batch looks the same on the wire as
 * what the producer sent.
 */
@Internal
public final class KafkaFetchTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFetchTranscoder.class);

    /** Default minimum buffer size for rebuilt Kafka batches. */
    private static final int MIN_BATCH_BUFFER_BYTES = 1024;

    private final KafkaServerContext context;
    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;

    public KafkaFetchTranscoder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.context = context;
        this.catalog = catalog;
        this.replicaManager = replicaManager;
    }

    public CompletableFuture<FetchResponseData> fetch(FetchRequestData request) {
        FetchResponseData response = new FetchResponseData();
        response.setThrottleTimeMs(0).setErrorCode(Errors.NONE.code());

        // First pass: resolve topics + partitions and assemble the Fluss fetch map. We keep a
        // side-map of (TableBucket -> per-partition response shell) so the Fluss callback can
        // populate each shell directly without recomputing lookups.
        Map<TableBucket, PartitionContext> requested = new LinkedHashMap<>();
        Map<String, List<PartitionContext>> pendingByTopic = new LinkedHashMap<>();
        Map<String, Errors> topicLevelErrors = new HashMap<>();

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
                topicLevelErrors.put(topicName, Errors.UNKNOWN_TOPIC_OR_PARTITION);
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
            org.apache.fluss.metadata.TableInfo tableInfo;
            try {
                tableInfo = context.metadataManager().getTable(topicInfo.dataTablePath());
            } catch (Exception e) {
                LOG.error(
                        "Failed to load TableInfo for fetch of '{}'", topicInfo.dataTablePath(), e);
                pendingByTopic.put(topicName, buildErrorShells(topic, Errors.UNKNOWN_SERVER_ERROR));
                continue;
            }

            List<PartitionContext> contexts = new ArrayList<>(topic.partitions().size());
            pendingByTopic.put(topicName, contexts);
            for (FetchPartition fp : topic.partitions()) {
                PartitionContext ctx =
                        new PartitionContext(
                                topicInfo,
                                tableInfo,
                                fp.partition(),
                                timestampType,
                                fp.fetchOffset());
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
                            null, null, p.partition(), TimestampType.CREATE_TIME, p.fetchOffset());
            ctx.failWith(error, null);
            shells.add(ctx);
        }
        return shells;
    }

    /** Per-partition accumulator that owns both the input shape and the output response. */
    private static final class PartitionContext {
        private final KafkaTopicInfo info;
        private final @javax.annotation.Nullable org.apache.fluss.metadata.TableInfo tableInfo;
        private final int partitionIndex;
        private final TimestampType timestampType;
        private final long fetchOffset;
        private ClientFetchRequest.BucketRead bucketRead;
        private FetchLogResultForBucket result;
        private short errorCode = Errors.NONE.code();
        private String errorMessage;

        PartitionContext(
                KafkaTopicInfo info,
                @javax.annotation.Nullable org.apache.fluss.metadata.TableInfo tableInfo,
                int partitionIndex,
                TimestampType timestampType,
                long fetchOffset) {
            this.info = info;
            this.tableInfo = tableInfo;
            this.partitionIndex = partitionIndex;
            this.timestampType = timestampType;
            this.fetchOffset = fetchOffset;
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
            PartitionData pd =
                    new PartitionData()
                            .setPartitionIndex(partitionIndex)
                            .setErrorCode(errorCode)
                            .setHighWatermark(result != null ? result.getHighWatermark() : -1L)
                            .setLastStableOffset(result != null ? result.getHighWatermark() : -1L)
                            .setLogStartOffset(-1L)
                            .setPreferredReadReplica(-1);
            if (errorCode != Errors.NONE.code()) {
                pd.setRecords(MemoryRecords.EMPTY);
                return pd;
            }
            if (result == null || result.records() == null) {
                pd.setRecords(MemoryRecords.EMPTY);
                return pd;
            }
            try {
                pd.setRecords(encode(result.records(), fetchOffset, timestampType, tableInfo));
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
     */
    private static MemoryRecords encode(
            LogRecords flussRecords,
            long baseOffset,
            TimestampType timestampType,
            @javax.annotation.Nullable org.apache.fluss.metadata.TableInfo tableInfo) {
        // Use the topic's real schema when available — compacted topics have record_key as a
        // non-null PK column, AND PK tables default to LogFormat.ARROW for their CDC log.
        // Decoding with the wrong schema / format produces bogus field offsets ("corrupt
        // message" on the Kafka client).
        org.apache.fluss.metadata.Schema effectiveSchema =
                tableInfo != null ? tableInfo.getSchema() : KafkaDataTable.schema();
        org.apache.fluss.metadata.SchemaGetter schemaGetter =
                new org.apache.fluss.metadata.SchemaGetter() {
                    @Override
                    public org.apache.fluss.metadata.Schema getSchema(int schemaId) {
                        return effectiveSchema;
                    }

                    @Override
                    public java.util.concurrent.CompletableFuture<
                                    org.apache.fluss.metadata.SchemaInfo>
                            getSchemaInfoAsync(int schemaId) {
                        return java.util.concurrent.CompletableFuture.completedFuture(
                                new org.apache.fluss.metadata.SchemaInfo(
                                        effectiveSchema, schemaId));
                    }

                    @Override
                    public org.apache.fluss.metadata.SchemaInfo getLatestSchemaInfo() {
                        return new org.apache.fluss.metadata.SchemaInfo(effectiveSchema, 1);
                    }

                    @Override
                    public void release() {}
                };

        // Walk once to count bytes; we'll allocate a single buffer slightly larger than needed.
        List<RowView> views = new ArrayList<>();
        long firstOffset = -1L;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long estimatedBytes = 512;
        for (LogRecordBatch batch : flussRecords.batches()) {
            // LogRecordReadContext.createReadContext format-branches on
            // tableInfo.getTableConfig().getLogFormat() — returns an ARROW-capable,
            // INDEXED-capable, or COMPACTED-capable read context uniformly. Iterating with
            // batch.records(ctx) yields CloseableIterator<LogRecord> for every format;
            // LogRecord.getRow() returns InternalRow our existing row-inspection code already
            // handles. When tableInfo is null (error path before we could load it) fall back
            // to INDEXED + KafkaDataTable.schema() so legacy log-only callers still work.
            org.apache.fluss.record.LogRecordReadContext readCtx;
            if (tableInfo != null) {
                readCtx =
                        org.apache.fluss.record.LogRecordReadContext.createReadContext(
                                tableInfo, /* readFromRemote */ false, null, schemaGetter);
            } else {
                readCtx =
                        org.apache.fluss.record.LogRecordReadContext.createIndexedReadContext(
                                effectiveSchema.getRowType(), batch.schemaId(), schemaGetter);
            }
            try (org.apache.fluss.utils.CloseableIterator<LogRecord> iter =
                    batch.records(readCtx)) {
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
                    org.apache.fluss.record.ChangeType changeType = record.getChangeType();
                    if (changeType == org.apache.fluss.record.ChangeType.UPDATE_BEFORE) {
                        continue;
                    }
                    InternalRow row = record.getRow();
                    boolean isDelete = changeType == org.apache.fluss.record.ChangeType.DELETE;
                    RowView view = RowView.of(row, record.logOffset(), isDelete);
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
            for (RowView view : views) {
                builder.appendWithOffset(
                        view.offset, view.timestamp, view.key, view.value, view.headers);
            }
            return builder.build();
        } finally {
            builder.close();
        }
    }

    /** Decoded view of a single Fluss row in the Kafka-data-table shape. */
    private static final class RowView {
        final long offset;
        final long timestamp;
        final byte[] key;
        final byte[] value;
        final Header[] headers;

        private RowView(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.headers = headers;
        }

        static RowView of(InternalRow row, long logOffset) {
            return of(row, logOffset, false);
        }

        static RowView of(InternalRow row, long logOffset, boolean tombstone) {
            // Columns: 0=record_key (BYTES), 1=payload (BYTES), 2=event_time (TIMESTAMP_LTZ),
            //          3=headers (ARRAY<ROW<name STRING, value BYTES>>).
            byte[] key = row.isNullAt(0) ? null : row.getBytes(0);
            byte[] value = tombstone || row.isNullAt(1) ? null : row.getBytes(1);
            long ts = row.getTimestampLtz(2, 3).toEpochMicros() / 1000L;
            Header[] headers;
            if (row.isNullAt(3)) {
                headers = Record.EMPTY_HEADERS;
            } else {
                InternalArray arr = row.getArray(3);
                headers = new Header[arr.size()];
                for (int i = 0; i < arr.size(); i++) {
                    if (arr.isNullAt(i)) {
                        headers[i] = new RecordHeader((String) null, null);
                        continue;
                    }
                    InternalRow headerRow = arr.getRow(i, 2);
                    String name = headerRow.isNullAt(0) ? null : headerRow.getString(0).toString();
                    byte[] hval = headerRow.isNullAt(1) ? null : headerRow.getBytes(1);
                    headers[i] = new RecordHeader(name, hval);
                }
            }
            return new RowView(logOffset, ts, key, value, headers);
        }

        int estimatedSize() {
            int size = 32; // record overhead approximation
            size += key == null ? 0 : key.length;
            size += value == null ? 0 : value.length;
            if (headers != null) {
                for (Header h : headers) {
                    size += 16;
                    size += h.key() == null ? 0 : h.key().length();
                    size += h.value() == null ? 0 : h.value().length;
                }
            }
            return size;
        }
    }

    /** Alias for the empty-headers sentinel from Kafka's {@code Record} interface. */
    private static final class Record {
        static final Header[] EMPTY_HEADERS = new Header[0];
    }
}
