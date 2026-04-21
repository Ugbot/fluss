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
import org.apache.fluss.kafka.metadata.KafkaDataTable;
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
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.server.replica.ReplicaManager;
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

    public KafkaProduceTranscoder(
            KafkaServerContext context, KafkaTopicsCatalog catalog, ReplicaManager replicaManager) {
        this.context = context;
        this.catalog = catalog;
        this.replicaManager = replicaManager;
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

        List<CompletableFuture<PartitionProduceResponse>> partitionFutures = new ArrayList<>();
        for (PartitionProduceData partition : topicData.partitionData()) {
            partitionFutures.add(
                    producePartition(
                            info,
                            tableInfo.getTableId(),
                            schemaId,
                            rowType,
                            valueWriters,
                            partition,
                            acks,
                            timeoutMs));
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
            int timeoutMs) {
        int partitionIndex = partition.index();
        PartitionProduceResponse response = new PartitionProduceResponse().setIndex(partitionIndex);

        MemoryRecords kafkaRecords = (MemoryRecords) partition.records();
        if (kafkaRecords == null || kafkaRecords.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(
                    response.setErrorCode(Errors.NONE.code()).setBaseOffset(-1));
        }

        MemoryLogRecords flussRecords;
        try {
            flussRecords = buildFlussRecords(kafkaRecords, schemaId, rowType, valueWriters, info);
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

    /** Build a Fluss {@link MemoryLogRecords} from a Kafka {@link MemoryRecords}. */
    private MemoryLogRecords buildFlussRecords(
            MemoryRecords kafkaRecords,
            int schemaId,
            RowType rowType,
            BinaryWriter.ValueWriter[] valueWriters,
            KafkaTopicInfo info)
            throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(INITIAL_SEGMENT_BYTES);
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        schemaId, Integer.MAX_VALUE, outputView, /* appendOnly */ true);

        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        IndexedRowWriter rowWriter = new IndexedRowWriter(rowType);
        IndexedRow row = new IndexedRow(fieldTypes);

        long writerId = -1L;
        int baseSequence = -1;

        for (RecordBatch batch : kafkaRecords.batches()) {
            if (batch.hasProducerId()) {
                writerId = batch.producerId();
                baseSequence = batch.baseSequence();
            }
            for (Record kafkaRecord : batch) {
                writeRow(kafkaRecord, rowWriter, valueWriters);
                row.pointTo(rowWriter.segment(), 0, rowWriter.position());
                builder.append(ChangeType.APPEND_ONLY, row);
            }
        }

        if (writerId >= 0) {
            builder.setWriterState(writerId, baseSequence);
        }

        BytesView built = builder.build();
        builder.close();
        return MemoryLogRecords.pointToBytesView(built);
    }

    private void writeRow(
            Record kafkaRecord,
            IndexedRowWriter rowWriter,
            BinaryWriter.ValueWriter[] valueWriters) {
        rowWriter.reset();

        // Delegating through the nullable ValueWriter wrapper keeps the sequential IndexedRow
        // writer's variable-length-pointer list consistent with its null bitmap.
        valueWriters[0].writeValue(
                rowWriter, 0, kafkaRecord.hasKey() ? byteBufferToBytes(kafkaRecord.key()) : null);
        valueWriters[1].writeValue(
                rowWriter,
                1,
                kafkaRecord.hasValue() ? byteBufferToBytes(kafkaRecord.value()) : null);

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
