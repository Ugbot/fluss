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

package org.apache.fluss.kafka.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaServerContext;
import org.apache.fluss.kafka.catalog.KafkaTopicInfo;
import org.apache.fluss.kafka.catalog.KafkaTopicsCatalog;
import org.apache.fluss.kafka.fetch.KafkaFetchTranscoder;
import org.apache.fluss.kafka.fetch.KafkaTopicRoute;
import org.apache.fluss.kafka.fetch.KafkaTopicRouteResolver;
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.kafka.sr.typed.RecordCodec;
import org.apache.fluss.kafka.tx.TransactionCoordinator;
import org.apache.fluss.kafka.tx.TransactionCoordinators;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Transcodes Kafka {@code ProduceRequest} payloads directly into Fluss {@link
 * ReplicaManager#appendRecordsToLog} calls.
 *
 * <p>Each Kafka record is converted into a Fluss {@link IndexedRow} matching the Fluss-native
 * {@link KafkaDataTable} schema. Compressed Kafka batches are transparently decompressed by the
 * Kafka {@code RecordBatch} iterator; the Fluss side stores payload bytes uncompressed so that
 * Fluss SQL can read individual records directly.
 */
@Internal
public final class KafkaProduceTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProduceTranscoder.class);

    private static final int INITIAL_SEGMENT_BYTES = 4096;

    private final KafkaServerContext context;
    private final KafkaTopicsCatalog catalog;
    private final ReplicaManager replicaManager;
    private final KafkaTopicRouteResolver routeResolver;

    public KafkaProduceTranscoder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this(
                context,
                catalog,
                replicaManager,
                KafkaTopicRouteResolver.fromCatalogServices(
                        context.typedTablesEnabled(), context.kafkaDatabase()));
    }

    /** Test-only constructor that takes a custom {@link KafkaTopicRouteResolver}. */
    public KafkaProduceTranscoder(
            KafkaServerContext context,
            KafkaTopicsCatalog catalog,
            ReplicaManager replicaManager,
            KafkaTopicRouteResolver routeResolver) {
        this.context = context;
        this.catalog = catalog;
        this.replicaManager = replicaManager;
        this.routeResolver = routeResolver;
    }

    /** Main entry point. Returns a future for the aggregated Kafka response. */
    public CompletableFuture<ProduceResponseData> produce(ProduceRequestData request) {
        ProduceResponseData response = new ProduceResponseData();
        response.setThrottleTimeMs(0);

        short acks = request.acks();
        int timeoutMs = request.timeoutMs();

        List<CompletableFuture<TopicProduceResponse>> topicFutures = new ArrayList<>();

        for (TopicProduceData topicData : request.topicData()) {
            topicFutures.add(produceTopic(topicData, acks, timeoutMs));
        }

        return CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            for (CompletableFuture<TopicProduceResponse> f : topicFutures) {
                                response.responses().add(f.join());
                            }
                            return response;
                        });
    }

    private CompletableFuture<TopicProduceResponse> produceTopic(
            TopicProduceData topicData, short acks, int timeoutMs) {
        TopicProduceResponse topicResponse = new TopicProduceResponse().setName(topicData.name());

        Optional<KafkaTopicInfo> maybeInfo;
        try {
            maybeInfo = catalog.lookup(topicData.name());
        } catch (Exception e) {
            LOG.error("Catalog lookup failed for produce to '{}'", topicData.name(), e);
            maybeInfo = Optional.empty();
        }

        if (!maybeInfo.isPresent()) {
            for (PartitionProduceData partition : topicData.partitionData()) {
                topicResponse
                        .partitionResponses()
                        .add(
                                new PartitionProduceResponse()
                                        .setIndex(partition.index())
                                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                        .setBaseOffset(-1));
            }
            return CompletableFuture.completedFuture(topicResponse);
        }

        KafkaTopicInfo info = maybeInfo.get();
        TableInfo tableInfo;
        try {
            tableInfo = context.metadataManager().getTable(info.dataTablePath());
        } catch (Exception e) {
            LOG.error("Failed to load TableInfo for '{}'", info.dataTablePath(), e);
            for (PartitionProduceData partition : topicData.partitionData()) {
                topicResponse
                        .partitionResponses()
                        .add(
                                new PartitionProduceResponse()
                                        .setIndex(partition.index())
                                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                                        .setErrorMessage(e.getMessage())
                                        .setBaseOffset(-1));
            }
            return CompletableFuture.completedFuture(topicResponse);
        }

        int schemaId = tableInfo.getSchemaId();
        RowType rowType = tableInfo.getRowType();
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        BinaryWriter.ValueWriter[] valueWriters = new BinaryWriter.ValueWriter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            valueWriters[i] =
                    BinaryWriter.createValueWriter(
                            fieldTypes[i], BinaryRow.BinaryRowFormat.INDEXED);
        }

        // Resolve the topic's typed-vs-passthrough route once per Produce call. When the
        // typed-tables feature flag is off the resolver is wired to alwaysPassthrough() so this
        // is a no-op string compare. Codec resolution is lazy: we only ask for it when the
        // route is typed AND the first record arrives — avoids loading the catalog entry on
        // every batch.
        KafkaTopicRoute route = routeResolver.resolve(topicData.name());
        TypedProduceBinding typedBinding = null;
        if (route.isTyped() && context.typedTablesEnabled()) {
            try {
                KafkaFetchTranscoder.TypedCodecBinding fetchBinding =
                        KafkaFetchTranscoder.resolveTypedCodec(
                                route, info, tableInfo, context.metrics());
                if (fetchBinding == null) {
                    LOG.warn(
                            "Typed Produce codec unavailable for topic '{}' (format={});"
                                    + " falling back to passthrough",
                            info.topic(),
                            route.catalogFormat());
                } else {
                    typedBinding =
                            new TypedProduceBinding(fetchBinding.codec, fetchBinding.srSchemaId);
                }
            } catch (Throwable t) {
                LOG.error(
                        "Typed Produce codec resolution failed for topic '{}'; falling back to"
                                + " passthrough for this batch",
                        info.topic(),
                        t);
            }
        }

        boolean compacted = tableInfo.hasPrimaryKey();
        List<CompletableFuture<PartitionProduceResponse>> partitionFutures = new ArrayList<>();
        for (PartitionProduceData partition : topicData.partitionData()) {
            if (compacted) {
                partitionFutures.add(
                        producePartitionKv(
                                info,
                                tableInfo.getTableId(),
                                schemaId,
                                rowType,
                                valueWriters,
                                partition,
                                acks,
                                timeoutMs,
                                typedBinding));
            } else {
                partitionFutures.add(
                        producePartition(
                                info,
                                tableInfo.getTableId(),
                                schemaId,
                                rowType,
                                valueWriters,
                                partition,
                                acks,
                                timeoutMs,
                                typedBinding));
            }
        }
        return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            for (CompletableFuture<PartitionProduceResponse> f : partitionFutures) {
                                topicResponse.partitionResponses().add(f.join());
                            }
                            return topicResponse;
                        });
    }

    private CompletableFuture<PartitionProduceResponse> producePartition(
            KafkaTopicInfo info,
            long tableId,
            int schemaId,
            RowType rowType,
            BinaryWriter.ValueWriter[] valueWriters,
            PartitionProduceData partition,
            short acks,
            int timeoutMs,
            @javax.annotation.Nullable TypedProduceBinding typedBinding) {
        int partitionIndex = partition.index();
        PartitionProduceResponse response = new PartitionProduceResponse().setIndex(partitionIndex);

        MemoryRecords kafkaRecords = (MemoryRecords) partition.records();
        if (kafkaRecords == null || kafkaRecords.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.NONE.code()).setBaseOffset(-1));
        }

        PartitionProduceResponse fenced = checkEpochFencing(kafkaRecords, response);
        if (fenced != null) {
            return CompletableFuture.completedFuture(fenced);
        }

        MemoryLogRecords flussRecords;
        try {
            flussRecords =
                    buildFlussRecords(
                            kafkaRecords,
                            tableId,
                            partitionIndex,
                            schemaId,
                            rowType,
                            valueWriters,
                            info,
                            typedBinding);
        } catch (InvalidProduceRecordException invalid) {
            // Per design 0014 §8: Kafka SR frame failures map to per-record codes; we narrow
            // the partition-level error code to the specific Errors enum the validator chose
            // (CORRUPT_MESSAGE for short / malformed frames, INVALID_RECORD for magic-byte and
            // schema-id mismatches). Logged at DEBUG since these are expected from non-SR
            // producers misrouted to a typed topic.
            LOG.debug(
                    "Typed Produce frame validation failed for topic '{}' partition {}: {}",
                    info.topic(),
                    partitionIndex,
                    invalid.getMessage());
            return CompletableFuture.completedFuture(
                    response.setErrorCode(invalid.error.code())
                            .setErrorMessage(invalid.getMessage())
                            .setBaseOffset(-1));
        } catch (Exception e) {
            LOG.error(
                    "Transcode failed for topic '{}' partition {}",
                    info.topic(),
                    partitionIndex,
                    e);
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.CORRUPT_MESSAGE.code())
                            .setErrorMessage(e.getMessage())
                            .setBaseOffset(-1));
        }

        if (flussRecords.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.NONE.code()).setBaseOffset(-1));
        }

        CompletableFuture<PartitionProduceResponse> future = new CompletableFuture<>();
        TableBucket bucket = new TableBucket(tableId, partitionIndex);
        try {
            replicaManager.appendRecordsToLog(
                    Math.max(timeoutMs, 1),
                    acks,
                    Collections.singletonMap(bucket, flussRecords),
                    /* userContext */ null,
                    results -> future.complete(translate(results, partitionIndex, response)));
        } catch (Throwable t) {
            LOG.error("appendRecordsToLog threw", t);
            future.complete(
                    response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage(t.getMessage())
                            .setBaseOffset(-1));
        }
        return future;
    }

    /**
     * Produce path for compacted topics (Fluss PK tables): each Kafka record becomes a KvRecord
     * with {@code record_key} as the row key. Null-valued Kafka records become tombstones (row=null
     * → delete the key). Uses {@link ReplicaManager#putRecordsToKv} under {@link
     * org.apache.fluss.rpc.protocol.MergeMode#OVERWRITE} so last-writer-wins by key.
     */
    private CompletableFuture<PartitionProduceResponse> producePartitionKv(
            KafkaTopicInfo info,
            long tableId,
            int schemaId,
            RowType rowType,
            BinaryWriter.ValueWriter[] valueWriters,
            PartitionProduceData partition,
            short acks,
            int timeoutMs,
            @javax.annotation.Nullable TypedProduceBinding typedBinding) {
        int partitionIndex = partition.index();
        PartitionProduceResponse response = new PartitionProduceResponse().setIndex(partitionIndex);

        MemoryRecords kafkaRecords = (MemoryRecords) partition.records();
        if (kafkaRecords == null || kafkaRecords.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.NONE.code()).setBaseOffset(-1));
        }

        PartitionProduceResponse fenced = checkEpochFencing(kafkaRecords, response);
        if (fenced != null) {
            return CompletableFuture.completedFuture(fenced);
        }

        org.apache.fluss.record.KvRecordBatch kvBatch;
        try {
            kvBatch =
                    buildFlussKvRecords(
                            kafkaRecords,
                            tableId,
                            partitionIndex,
                            schemaId,
                            rowType,
                            valueWriters,
                            typedBinding);
        } catch (InvalidProduceRecordException invalid) {
            LOG.debug(
                    "Typed Produce frame validation failed for compacted topic '{}' partition"
                            + " {}: {}",
                    info.topic(),
                    partitionIndex,
                    invalid.getMessage());
            return CompletableFuture.completedFuture(
                    response.setErrorCode(invalid.error.code())
                            .setErrorMessage(invalid.getMessage())
                            .setBaseOffset(-1));
        } catch (Exception e) {
            LOG.error(
                    "Transcode failed for compacted topic '{}' partition {}",
                    info.topic(),
                    partitionIndex,
                    e);
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.CORRUPT_MESSAGE.code())
                            .setErrorMessage(e.getMessage())
                            .setBaseOffset(-1));
        }

        CompletableFuture<PartitionProduceResponse> future = new CompletableFuture<>();
        org.apache.fluss.metadata.TableBucket bucket =
                new org.apache.fluss.metadata.TableBucket(tableId, partitionIndex);
        try {
            replicaManager.putRecordsToKv(
                    Math.max(timeoutMs, 1),
                    acks,
                    Collections.singletonMap(bucket, kvBatch),
                    /* targetColumns */ null,
                    org.apache.fluss.rpc.protocol.MergeMode.OVERWRITE,
                    /* apiVersion */ (short) 0,
                    results -> future.complete(translateKv(results, partitionIndex, response)));
        } catch (Throwable t) {
            LOG.error("putRecordsToKv threw", t);
            future.complete(
                    response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage(t.getMessage())
                            .setBaseOffset(-1));
        }
        return future;
    }

    /**
     * Scans the first producer-aware batch in {@code kafkaRecords} and, when a {@link
     * TransactionCoordinator} is running, checks the epoch against the coordinator's view.
     *
     * @return a pre-filled {@link PartitionProduceResponse} with {@code INVALID_PRODUCER_EPOCH} if
     *     the producer is fenced; {@code null} if the batch is either not producer-aware, no
     *     coordinator is running, or the epoch is current.
     */
    @javax.annotation.Nullable
    private static PartitionProduceResponse checkEpochFencing(
            MemoryRecords kafkaRecords, PartitionProduceResponse response) {
        for (RecordBatch batch : kafkaRecords.batches()) {
            if (!batch.hasProducerId()) {
                continue;
            }
            long producerId = batch.producerId();
            short producerEpoch = batch.producerEpoch();
            Optional<TransactionCoordinator> coord = TransactionCoordinators.current();
            if (!coord.isPresent()) {
                return null;
            }
            TransactionCoordinator.EpochCheck check =
                    coord.get().checkProducerEpoch(producerId, producerEpoch);
            if (check == TransactionCoordinator.EpochCheck.INVALID_PRODUCER_EPOCH) {
                return response.setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code())
                        .setErrorMessage("Fenced producer epoch")
                        .setBaseOffset(-1);
            }
            return null; // only check the first producer-aware batch per partition
        }
        return null;
    }

    /**
     * Build a Fluss {@link org.apache.fluss.record.KvRecordBatch} from a Kafka {@link
     * MemoryRecords}. The {@code record_key} column in the row mirrors the separate {@code key}
     * byte array the KV batch builder expects — Fluss's row-format constraint is satisfied by using
     * {@link org.apache.fluss.metadata.KvFormat#INDEXED} on the table descriptor (see {@link
     * org.apache.fluss.kafka.catalog.KafkaTableFactory#buildDescriptor(String, int,
     * KafkaTopicInfo.TimestampType, KafkaTopicInfo.Compression, org.apache.kafka.common.Uuid,
     * boolean)}).
     */
    private org.apache.fluss.record.KvRecordBatch buildFlussKvRecords(
            MemoryRecords kafkaRecords,
            long tableId,
            int partitionIdx,
            int schemaId,
            RowType rowType,
            BinaryWriter.ValueWriter[] valueWriters,
            @javax.annotation.Nullable TypedProduceBinding typedBinding)
            throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(INITIAL_SEGMENT_BYTES);
        org.apache.fluss.record.KvRecordBatchBuilder builder =
                org.apache.fluss.record.KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        outputView,
                        org.apache.fluss.metadata.KvFormat.INDEXED);

        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        IndexedRowWriter rowWriter = new IndexedRowWriter(rowType);
        IndexedRow row = new IndexedRow(fieldTypes);
        KafkaMetricGroup metrics = context.metrics();
        KafkaWriterSeqCache seqCache = context.writerSeqCache();

        long writerId = -1L;
        int flussSeq = -1;

        for (RecordBatch batch : kafkaRecords.batches()) {
            if (batch.hasProducerId() && writerId == -1L) {
                writerId = batch.producerId();
                flussSeq =
                        seqCache.nextSeq(
                                batch.producerId(), tableId, partitionIdx, batch.baseSequence());
            }
            for (Record kafkaRecord : batch) {
                byte[] key =
                        kafkaRecord.hasKey() ? byteBufferToBytes(kafkaRecord.key()) : new byte[0];
                if (kafkaRecord.hasValue()) {
                    writeRow(kafkaRecord, rowWriter, valueWriters, typedBinding);
                    if (metrics != null && typedBinding != null) {
                        metrics.onTypedProduce(1);
                    }
                    row.pointTo(rowWriter.segment(), 0, rowWriter.position());
                    builder.append(key, row);
                } else {
                    // Kafka tombstone (null value on a compacted topic) → delete the key.
                    builder.append(key, null);
                }
            }
        }

        if (writerId >= 0) {
            builder.setWriterState(writerId, flussSeq);
        }

        org.apache.fluss.record.bytesview.BytesView built = builder.build();
        builder.close();
        return org.apache.fluss.record.DefaultKvRecordBatch.pointToByteBuffer(
                built.getByteBuf().nioBuffer());
    }

    private PartitionProduceResponse translateKv(
            List<org.apache.fluss.rpc.entity.PutKvResultForBucket> results,
            int partitionIndex,
            PartitionProduceResponse response) {
        for (org.apache.fluss.rpc.entity.PutKvResultForBucket result : results) {
            if (result.getBucketId() != partitionIndex) {
                continue;
            }
            if (result.failed()) {
                String msg =
                        result.getErrorMessage() != null
                                ? result.getErrorMessage()
                                : result.getError().toString();
                return response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(msg)
                        .setBaseOffset(-1);
            }
            return response.setErrorCode(Errors.NONE.code()).setBaseOffset(0);
        }
        return response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                .setErrorMessage("no KV result for partition " + partitionIndex)
                .setBaseOffset(-1);
    }

    /**
     * Build a Fluss {@link MemoryLogRecords} from a Kafka {@link MemoryRecords}.
     *
     * <p>Each Kafka {@link RecordBatch} becomes its own Fluss batch. Kafka sequences are per-record
     * (the next batch's {@code baseSequence} advances by {@code numRecords}), whereas Fluss
     * validates {@code nextBatchSeq == lastBatchSeq + 1}. To bridge that gap, each Fluss batch
     * receives a synthetic per-batch counter from {@link KafkaWriterSeqCache} rather than the raw
     * Kafka {@code baseSequence}. The counter resets to 0 when the Kafka {@code baseSequence} is 0
     * (fresh producer or epoch reset), which is also the signal used by {@code
     * WriterAppendInfo.inSequence} to accept new-epoch appends.
     *
     * <p>All Fluss batches for this partition are concatenated into a single {@link
     * MemoryLogRecords} so that {@code appendRecordsToLog} validates them atomically in one call,
     * with each sub-batch's sequence checked sequentially by {@code analyzeAndValidateWriterState}.
     */
    private MemoryLogRecords buildFlussRecords(
            MemoryRecords kafkaRecords,
            long tableId,
            int partitionIdx,
            int schemaId,
            RowType rowType,
            BinaryWriter.ValueWriter[] valueWriters,
            KafkaTopicInfo info,
            @javax.annotation.Nullable TypedProduceBinding typedBinding)
            throws Exception {
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        IndexedRowWriter rowWriter = new IndexedRowWriter(rowType);
        IndexedRow row = new IndexedRow(fieldTypes);
        KafkaMetricGroup metrics = context.metrics();
        KafkaWriterSeqCache seqCache = context.writerSeqCache();

        List<BytesView> builtBatches = new ArrayList<>();

        for (RecordBatch batch : kafkaRecords.batches()) {
            UnmanagedPagedOutputView outputView =
                    new UnmanagedPagedOutputView(INITIAL_SEGMENT_BYTES);
            MemoryLogRecordsIndexedBuilder builder =
                    MemoryLogRecordsIndexedBuilder.builder(
                            schemaId, Integer.MAX_VALUE, outputView, /* appendOnly */ true);

            for (Record kafkaRecord : batch) {
                writeRow(kafkaRecord, rowWriter, valueWriters, typedBinding);
                if (metrics != null && typedBinding != null) {
                    metrics.onTypedProduce(1);
                }
                row.pointTo(rowWriter.segment(), 0, rowWriter.position());
                builder.append(ChangeType.APPEND_ONLY, row);
            }

            if (batch.hasProducerId()) {
                int flussSeq =
                        seqCache.nextSeq(
                                batch.producerId(), tableId, partitionIdx, batch.baseSequence());
                builder.setWriterState(batch.producerId(), flussSeq);
            }

            BytesView built = builder.build();
            builder.close();
            builtBatches.add(built);
        }

        return combineBatches(builtBatches);
    }

    /** Combines a list of {@link BytesView} objects into a single {@link MemoryLogRecords}. */
    private static MemoryLogRecords combineBatches(List<BytesView> batches) {
        if (batches.isEmpty()) {
            return MemoryLogRecords.EMPTY;
        }
        if (batches.size() == 1) {
            return MemoryLogRecords.pointToBytesView(batches.get(0));
        }
        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf[] bufs =
                new org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf[batches.size()];
        for (int i = 0; i < batches.size(); i++) {
            bufs[i] = batches.get(i).getByteBuf();
        }
        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf combined =
                Unpooled.wrappedBuffer(bufs);
        return MemoryLogRecords.pointToByteBuffer(combined.nioBuffer());
    }

    private void writeRow(
            Record kafkaRecord,
            IndexedRowWriter rowWriter,
            BinaryWriter.ValueWriter[] valueWriters,
            @javax.annotation.Nullable TypedProduceBinding typedBinding) {
        byte[] valueBytes = kafkaRecord.hasValue() ? byteBufferToBytes(kafkaRecord.value()) : null;

        if (typedBinding != null && valueBytes != null) {
            // Typed produce path (T.4): decode the SR-encoded body into the N user columns,
            // then write the full typed row in layout [key, user_col_0..N-1, event_time, headers].
            byte[] body = stripKafkaSrFrame(valueBytes, typedBinding.srSchemaId);
            typedBinding.codec.decodeInto(
                    Unpooled.wrappedBuffer(body), 0, body.length, typedBinding.userColWriter);
            typedBinding.userColRow.pointTo(
                    typedBinding.userColWriter.segment(), 0, typedBinding.userColWriter.position());

            int numUserCols = typedBinding.userColGetters.length;
            rowWriter.reset();
            valueWriters[0].writeValue(
                    rowWriter,
                    0,
                    kafkaRecord.hasKey() ? byteBufferToBytes(kafkaRecord.key()) : null);
            for (int i = 0; i < numUserCols; i++) {
                valueWriters[i + 1].writeValue(
                        rowWriter,
                        i + 1,
                        typedBinding.userColGetters[i].getFieldOrNull(typedBinding.userColRow));
            }
            long ts = kafkaRecord.timestamp();
            if (ts == RecordBatch.NO_TIMESTAMP) {
                ts = System.currentTimeMillis();
            }
            valueWriters[numUserCols + 1].writeValue(
                    rowWriter, numUserCols + 1, TimestampLtz.fromEpochMillis(ts));
            Header[] headers = kafkaRecord.headers();
            if (headers == null || headers.length == 0) {
                valueWriters[numUserCols + 2].writeValue(rowWriter, numUserCols + 2, null);
            } else {
                Object[] rows = new Object[headers.length];
                for (int i = 0; i < headers.length; i++) {
                    Header h = headers[i];
                    rows[i] =
                            GenericRow.of(
                                    h.key() == null ? null : BinaryString.fromString(h.key()),
                                    h.value());
                }
                valueWriters[numUserCols + 2].writeValue(
                        rowWriter, numUserCols + 2, new GenericArray(rows));
            }
        } else {
            // Passthrough path: 4-column layout [key, value_bytes, event_time, headers].
            rowWriter.reset();
            valueWriters[0].writeValue(
                    rowWriter,
                    0,
                    kafkaRecord.hasKey() ? byteBufferToBytes(kafkaRecord.key()) : null);
            valueWriters[1].writeValue(rowWriter, 1, valueBytes);
            long ts = kafkaRecord.timestamp();
            if (ts == RecordBatch.NO_TIMESTAMP) {
                ts = System.currentTimeMillis();
            }
            valueWriters[2].writeValue(rowWriter, 2, TimestampLtz.fromEpochMillis(ts));
            Header[] headers = kafkaRecord.headers();
            if (headers == null || headers.length == 0) {
                valueWriters[3].writeValue(rowWriter, 3, null);
            } else {
                Object[] rows = new Object[headers.length];
                for (int i = 0; i < headers.length; i++) {
                    Header h = headers[i];
                    rows[i] =
                            GenericRow.of(
                                    h.key() == null ? null : BinaryString.fromString(h.key()),
                                    h.value());
                }
                valueWriters[3].writeValue(rowWriter, 3, new GenericArray(rows));
            }
        }
    }

    /**
     * Strip the 5-byte Kafka SR wire frame ({@code [0x00][int32 schemaId]}) and return the body.
     * Throws {@link InvalidProduceRecordException} when the frame is malformed (length &lt; 5,
     * magic byte ≠ 0x00) or when the framed schema id doesn't match the codec the topic was
     * resolved against — per design 0014 §8 the caller turns these into per-record {@link
     * Errors#CORRUPT_MESSAGE} / {@link Errors#INVALID_RECORD} responses.
     *
     * <p>Note that T.2 strips and validates the frame even when the row format hasn't yet been
     * altered to typed columns (T.3); the body bytes are written into the payload column. Once T.3
     * alters the layout the {@code typedBinding.codec.decodeInto(buf, offset, length, rowWriter)}
     * call replaces the body byte-write — the frame strip + validation logic stays here.
     */
    private static byte[] stripKafkaSrFrame(byte[] framed, int expectedSchemaId) {
        if (framed.length < 5) {
            throw new InvalidProduceRecordException(
                    Errors.CORRUPT_MESSAGE,
                    "Kafka SR frame shorter than 5 bytes: length=" + framed.length);
        }
        if (framed[0] != 0x00) {
            throw new InvalidProduceRecordException(
                    Errors.INVALID_RECORD,
                    "Kafka SR magic byte expected 0x00, got 0x"
                            + String.format("%02X", framed[0] & 0xFF));
        }
        int frameId =
                ((framed[1] & 0xFF) << 24)
                        | ((framed[2] & 0xFF) << 16)
                        | ((framed[3] & 0xFF) << 8)
                        | (framed[4] & 0xFF);
        if (frameId != expectedSchemaId) {
            throw new InvalidProduceRecordException(
                    Errors.INVALID_RECORD,
                    "Schema id mismatch: framed=" + frameId + " expected=" + expectedSchemaId);
        }
        byte[] body = new byte[framed.length - 5];
        System.arraycopy(framed, 5, body, 0, body.length);
        return body;
    }

    /** Codec + Kafka SR schema id pair carried through the produce path for typed topics. */
    static final class TypedProduceBinding {
        final RecordCodec codec;
        final int srSchemaId;
        /** Pre-allocated writer for decoding user columns from the SR-encoded body. */
        final IndexedRowWriter userColWriter;
        /** Pre-allocated row view over {@link #userColWriter}'s segment after each decode. */
        final IndexedRow userColRow;
        /** Per-column field getters for extracting values from {@link #userColRow}. */
        final InternalRow.FieldGetter[] userColGetters;

        TypedProduceBinding(RecordCodec codec, int srSchemaId) {
            this.codec = codec;
            this.srSchemaId = srSchemaId;
            RowType userRowType = codec.rowType();
            this.userColWriter = new IndexedRowWriter(userRowType);
            this.userColRow = new IndexedRow(userRowType.getChildren().toArray(new DataType[0]));
            int numUserCols = userRowType.getFieldCount();
            this.userColGetters = new InternalRow.FieldGetter[numUserCols];
            for (int i = 0; i < numUserCols; i++) {
                userColGetters[i] = InternalRow.createFieldGetter(userRowType.getTypeAt(i), i);
            }
        }
    }

    /** Per-record produce failure that should map to a Kafka error code (no batch failure). */
    static final class InvalidProduceRecordException extends RuntimeException {
        final Errors error;

        InvalidProduceRecordException(Errors error, String message) {
            super(message);
            this.error = error;
        }
    }

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }

    private PartitionProduceResponse translate(
            List<ProduceLogResultForBucket> results,
            int partitionIndex,
            PartitionProduceResponse response) {
        for (ProduceLogResultForBucket result : results) {
            if (result.getBucketId() != partitionIndex) {
                continue;
            }
            if (result.failed()) {
                String msg =
                        result.getErrorMessage() != null
                                ? result.getErrorMessage()
                                : "Fluss errorCode=" + result.getErrorCode();
                LOG.warn(
                        "Fluss appendRecordsToLog error for bucket {}: code={} msg={}",
                        partitionIndex,
                        result.getErrorCode(),
                        msg);
                return response.setErrorCode(mapFlussError(result.getErrorCode()))
                        .setErrorMessage(msg)
                        .setBaseOffset(-1);
            }
            return response.setErrorCode(Errors.NONE.code()).setBaseOffset(result.getBaseOffset());
        }
        return response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                .setErrorMessage("Fluss did not return a result for bucket " + partitionIndex)
                .setBaseOffset(-1);
    }

    /** Map a Fluss error code to the closest Kafka error code. */
    private static short mapFlussError(int flussErrorCode) {
        return org.apache.fluss.kafka.KafkaErrors.toKafka(flussErrorCode).code();
    }
}
