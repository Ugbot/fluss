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

package org.apache.fluss.lake.tiering.reader;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.ArrowScanRecords;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.lake.batch.ArrowRecordBatch;
import org.apache.fluss.lake.tiering.split.TieringLogSplit;
import org.apache.fluss.lake.tiering.split.TieringSnapshotSplit;
import org.apache.fluss.lake.tiering.split.TieringSplit;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsRecordBatchWrite;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The engine-agnostic core that reads a Fluss table (snapshot and/or log splits) and writes the
 * data to a lake via the {@link LakeTieringFactory} SPI.
 *
 * <p>This is the extraction of the Flink connector's {@code TieringSplitReader}: it contains all of
 * the same private tiering logic (the Arrow batch fast path, the row path, the snapshot/PK path and
 * the offset/timestamp bookkeeping), but without the Flink {@code SplitReader} / {@code
 * RecordsWithSplitIds} iteration protocol. Instead of producing a {@code RecordsWithSplitIds} for
 * the Flink fetcher to drain, it exposes {@link #tierTable(TablePath, long, List)} which drives a
 * single table to completion and returns the finished {@link BucketWriteResult}s directly.
 *
 * @param <WriteResult> the lake write result type produced by the {@link LakeWriter}
 */
public class TieringTableReader<WriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringTableReader.class);

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10_000L);

    // unknown bucket timestamp for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_TIMESTAMP = -1;

    // unknown bucket offset for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_OFFSET = -1;

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;

    private final Duration pollTimeout;

    // the id for the pending tables to be tiered
    private final Queue<Long> pendingTieringTables;
    // the table_id to the pending splits
    private final Map<Long, Set<TieringSplit>> pendingTieringSplits;

    private final Set<Long> reachTieringMaxDurationTables;

    private final Map<TableBucket, LakeWriter<WriteResult>> lakeWriters;
    private final Connection connection;

    @Nullable private Long currentTableId;
    @Nullable private TablePath currentTablePath;
    @Nullable private LogScanner currentLogScanner;
    @Nullable private Table currentTable;

    private final Queue<TieringSnapshotSplit> currentPendingSnapshotSplits;
    @Nullable private BoundedSplitReader currentSnapshotSplitReader;
    @Nullable private TieringSnapshotSplit currentSnapshotSplit;
    @Nullable private Integer currentTableNumberOfSplits;
    // whether the current table uses the Arrow record batch path for tiering
    @Nullable private Boolean currentTableUseRecordBatchPath;

    // map from table bucket to split id
    private final Map<TableBucket, TieringSplit> currentTableSplitsByBucket;
    private final Map<TableBucket, Long> currentTableStoppingOffsets;

    private final Map<TableBucket, LogOffsetAndTimestamp> currentTableTieredOffsetAndTimestamp;

    private final Set<TieringSplit> currentEmptySplits;

    private final TieringReaderMetrics tieringMetrics;
    private final boolean unshadedArrowAvailable;

    public TieringTableReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            TieringReaderMetrics tieringMetrics) {
        this(connection, lakeTieringFactory, userClassLoader, DEFAULT_POLL_TIMEOUT, tieringMetrics);
    }

    @VisibleForTesting
    public TieringTableReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            ClassLoader userClassLoader,
            Duration pollTimeout,
            TieringReaderMetrics tieringMetrics) {
        this.lakeTieringFactory = lakeTieringFactory;
        // owned by the daemon (TieringService)
        this.connection = connection;
        this.pendingTieringTables = new ArrayDeque<>();
        this.pendingTieringSplits = new HashMap<>();
        this.currentTableStoppingOffsets = new HashMap<>();
        this.currentTableTieredOffsetAndTimestamp = new HashMap<>();
        this.currentEmptySplits = new HashSet<>();
        this.currentTableSplitsByBucket = new HashMap<>();
        this.lakeWriters = new HashMap<>();
        this.currentPendingSnapshotSplits = new ArrayDeque<>();
        this.reachTieringMaxDurationTables = new HashSet<>();
        this.pollTimeout = pollTimeout;
        this.tieringMetrics = tieringMetrics;
        this.unshadedArrowAvailable = checkUnshadedArrowAvailable(userClassLoader);
    }

    /**
     * Drives a single table to completion: adds the given splits, then repeatedly polls until all
     * of the table's splits are finished, accumulating and returning the finished {@link
     * BucketWriteResult}s.
     *
     * @param tablePath the table to tier (used for logging / sanity)
     * @param tableId the id of the table to tier
     * @param splits the splits (snapshot and/or log) of the table to tier
     * @return the finished write results for every bucket of the table
     * @throws IOException if an I/O error occurs while reading or writing
     */
    public List<BucketWriteResult<WriteResult>> tierTable(
            TablePath tablePath, long tableId, List<TieringSplit> splits) throws IOException {
        addSplits(splits);
        List<BucketWriteResult<WriteResult>> finished = new ArrayList<>();
        // loop until there is nothing left to do for the current table:
        //   - no pending empty splits,
        //   - no open snapshot reader,
        //   - no open log scanner,
        //   - no pending snapshot splits / no pending tables.
        while (hasPendingWork()) {
            finished.addAll(pollOnce());
        }
        LOG.info(
                "Finished tiering table {} (id {}) with {} write result(s).",
                tablePath,
                tableId,
                finished.size());
        return finished;
    }

    private boolean hasPendingWork() {
        return !currentEmptySplits.isEmpty()
                || currentSnapshotSplitReader != null
                || currentLogScanner != null
                || !currentPendingSnapshotSplits.isEmpty()
                || !currentTableSplitsByBucket.isEmpty()
                || !pendingTieringTables.isEmpty();
    }

    /**
     * Performs a single tiering step, equivalent to one Flink {@code fetch()} cycle. Returns the
     * {@link BucketWriteResult}s for any splits that finished during this step (possibly empty).
     */
    private List<BucketWriteResult<WriteResult>> pollOnce() throws IOException {
        // check empty splits
        if (!currentEmptySplits.isEmpty()) {
            LOG.info("Empty split(s) {} finished.", currentEmptySplits);
            List<BucketWriteResult<WriteResult>> records = forEmptySplits(currentEmptySplits);
            currentEmptySplits.forEach(
                    split -> currentTableSplitsByBucket.remove(split.getTableBucket()));
            mayFinishCurrentTable();
            currentEmptySplits.clear();
            return records;
        }
        checkSplitOrStartNext();

        // may read snapshot firstly
        if (currentSnapshotSplitReader != null) {
            // for snapshot split, we don't force to complete it
            // since we rely on the log offset for the snapshot to
            // do next tiering, if force to complete, we can't get the log offset
            CloseableIterator<RecordAndPos> recordIterator = currentSnapshotSplitReader.readBatch();
            if (recordIterator == null) {
                LOG.info("Split {} is finished", currentSnapshotSplit.splitId());
                return finishCurrentSnapshotSplit();
            } else {
                return forSnapshotSplitRecords(
                        currentSnapshotSplit.getTableBucket(), recordIterator);
            }
        } else {
            if (currentLogScanner != null) {
                // force to complete records
                if (reachTieringMaxDurationTables.contains(currentTableId)) {
                    return forceCompleteTieringLogRecords();
                }
                if (useRecordBatchPath()) {
                    try (ArrowScanRecords arrowScanRecords =
                            ((LogScannerImpl) currentLogScanner).pollRecordBatch(pollTimeout)) {
                        return processLogRecords(
                                arrowScanRecords.buckets(),
                                arrowScanRecords::records,
                                this::handleArrowBatchRecords,
                                arrowScanRecords::consumedUpToOffset);
                    }
                } else {
                    ScanRecords scanRecords = currentLogScanner.poll(pollTimeout);
                    return processLogRecords(
                            scanRecords.buckets(),
                            scanRecords::records,
                            this::handleLogRecords,
                            scanRecords::consumedUpToOffset);
                }
            } else {
                return emptyWriteResults();
            }
        }
    }

    /**
     * Adds the given splits to the reader. This is the engine-agnostic equivalent of the Flink
     * connector's {@code handleSplitsChanges(SplitsAddition)} body.
     */
    @VisibleForTesting
    void addSplits(List<TieringSplit> splits) {
        for (TieringSplit split : splits) {
            LOG.info("add split {}", split.splitId());
            if (split.shouldSkipCurrentRound()) {
                // if the split is forced to ignore,
                // mark it as empty
                LOG.info(
                        "ignore split {} since the split is set to skip the current round of tiering.",
                        split.splitId());
                currentEmptySplits.add(split);
                continue;
            }
            long tableId = split.getTableBucket().getTableId();
            // the split belongs to the current table
            if (currentTableId != null && currentTableId == tableId) {
                addSplitToCurrentTable(split);
            } else {
                Set<TieringSplit> alreadyPendingSplits = pendingTieringSplits.get(tableId);
                if (alreadyPendingSplits != null) {
                    // add to the already pending splits
                    alreadyPendingSplits.add(split);
                } else {
                    Set<TieringSplit> pendingSplits = new HashSet<>();
                    pendingSplits.add(split);
                    pendingTieringSplits.put(tableId, pendingSplits);
                    pendingTieringTables.add(tableId);
                }
            }
        }
    }

    private void addSplitToCurrentTable(TieringSplit split) {
        this.currentTableSplitsByBucket.put(split.getTableBucket(), split);
        if (split.isTieringSnapshotSplit()) {
            this.currentPendingSnapshotSplits.add((TieringSnapshotSplit) split);
        } else if (split.isTieringLogSplit()) {
            subscribeLog((TieringLogSplit) split);
        }
    }

    private void checkSplitOrStartNext() {
        if (currentSnapshotSplitReader != null) {
            return;
        }

        // may poll next snapshot split to read
        TieringSnapshotSplit nextSnapshotSplit = currentPendingSnapshotSplits.poll();
        if (nextSnapshotSplit != null) {
            Table table = getOrMoveToTable(nextSnapshotSplit);
            currentSnapshotSplit = nextSnapshotSplit;
            currentSnapshotSplitReader =
                    new BoundedSplitReader(
                            table.newScan()
                                    .createBatchScanner(
                                            currentSnapshotSplit.getTableBucket(),
                                            currentSnapshotSplit.getSnapshotId()),
                            0);
            return;
        }

        // use current log scanner to read
        if (currentLogScanner != null) {
            return;
        }

        // may poll next table to read
        Long pendingTableId = pendingTieringTables.poll();
        if (pendingTableId == null) {
            return;
        }

        Set<TieringSplit> pendingSplits = pendingTieringSplits.remove(pendingTableId);
        for (TieringSplit split : pendingSplits) {
            getOrMoveToTable(split);
            addSplitToCurrentTable(split);
        }
    }

    private Table getOrMoveToTable(TieringSplit split) {
        if (currentTable == null) {
            TablePath tablePath = split.getTablePath();
            currentTable = connection.getTable(tablePath);
            currentTablePath = tablePath;
            currentTableId = split.getTableBucket().getTableId();
            currentTableNumberOfSplits = split.getNumberOfSplits();
            TableInfo currentTableInfo = checkNotNull(currentTable).getTableInfo();
            // check currentTable's id for the table path is same with table id of the tiering
            // split, if not, it means the tiering split is for a previous dropped table. let's fail
            // directly
            // todo: we should skip and notify enumerator that the table id is not tiering now
            // instead of fail directly
            checkArgument(
                    currentTableInfo.getTableId() == split.getTableBucket().getTableId(),
                    "The current table id %s for table path %s is different from the table id %s in TieringSplit split.",
                    currentTableInfo.getTableId(),
                    tablePath,
                    split.getTableBucket().getTableId());
            LOG.info("Start to tier table {} with table id {}.", currentTablePath, currentTableId);
        }
        return currentTable;
    }

    private void mayCreateLogScanner() {
        if (currentLogScanner == null) {
            currentLogScanner = checkNotNull(currentTable).newScan().createLogScanner();
        }
    }

    private List<BucketWriteResult<WriteResult>> forceCompleteTieringLogRecords()
            throws IOException {
        List<BucketWriteResult<WriteResult>> writeResults = new ArrayList<>();

        // force finish all splits
        Iterator<Map.Entry<TableBucket, TieringSplit>> currentTieringSplitsIterator =
                currentTableSplitsByBucket.entrySet().iterator();
        while (currentTieringSplitsIterator.hasNext()) {
            Map.Entry<TableBucket, TieringSplit> entry = currentTieringSplitsIterator.next();
            TableBucket bucket = entry.getKey();
            TieringSplit split = entry.getValue();
            if (split != null && split.isTieringLogSplit()) {
                // get the current offset, timestamp that tiered so far
                LogOffsetAndTimestamp logOffsetAndTimestamp =
                        currentTableTieredOffsetAndTimestamp.get(bucket);
                long logEndOffset =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_OFFSET
                                // logEndOffset is equal to offset tiered + 1
                                : logOffsetAndTimestamp.logOffset + 1;
                long timestamp =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_TIMESTAMP
                                : logOffsetAndTimestamp.timestamp;
                BucketWriteResult<WriteResult> bucketWriteResult =
                        completeLakeWriter(
                                bucket, split.getPartitionName(), logEndOffset, timestamp);

                if (logEndOffset == UNKNOWN_BUCKET_OFFSET) {
                    // when the log end offset is unknown, the write result must be
                    // null, otherwise, we should throw exception directly to avoid data
                    // inconsistent
                    checkState(
                            bucketWriteResult.writeResult() == null,
                            "bucketWriteResult must be null when log end offset is unknown when tiering "
                                    + split);
                }

                writeResults.add(bucketWriteResult);
                LOG.info(
                        "Split {} is forced to be finished due to tiering reach max duration, "
                                + "write result {}, logEndOffset {}, timestamp {}",
                        split.splitId(),
                        bucketWriteResult,
                        logEndOffset,
                        timestamp);
                currentTieringSplitsIterator.remove();
            }
        }
        reachTieringMaxDurationTables.remove(this.currentTableId);
        mayFinishCurrentTable();
        return writeResults;
    }

    /**
     * Determines whether the current table should use the Arrow record batch path for tiering. The
     * batch path is used when the table is an ARROW format append-only (log) table and the lake
     * writer supports batch writing.
     */
    private boolean useRecordBatchPath() {
        if (currentTableUseRecordBatchPath != null) {
            return currentTableUseRecordBatchPath;
        }
        TableInfo tableInfo = checkNotNull(currentTable).getTableInfo();

        currentTableUseRecordBatchPath =
                unshadedArrowAvailable
                        && !tableInfo.hasPrimaryKey()
                        && tableInfo.getTableConfig().getLogFormat() == LogFormat.ARROW
                        && tableInfo.getTableConfig().getDataLakeFormat().orElse(null)
                                == DataLakeFormat.PAIMON;
        return currentTableUseRecordBatchPath;
    }

    /**
     * Generic template method for processing tiering log records. Encapsulates the shared workflow
     * of bucket traversal, stopping offset checks, LakeWriter management, offset/timestamp
     * tracking, split completion, and table completion.
     *
     * @param buckets the set of buckets that have records
     * @param recordsExtractor function to extract records for a given bucket
     * @param handler callback for processing records within a single bucket
     * @param consumedUpToOffsetExtractor function to extract the consumed-up-to offset for a given
     *     bucket. The offset is used for progress tracking and split completion even when records
     *     are empty.
     * @param <R> the record type
     * @return the finished write results
     * @throws IOException if an I/O error occurs during processing
     */
    private <R> List<BucketWriteResult<WriteResult>> processLogRecords(
            Set<TableBucket> buckets,
            Function<TableBucket, List<R>> recordsExtractor,
            BucketRecordsHandler<R> handler,
            Function<TableBucket, Long> consumedUpToOffsetExtractor)
            throws IOException {
        List<BucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        boolean anyFinished = false;

        // Iterate every polled bucket, including those that only advanced their offset.
        for (TableBucket bucket : buckets) {
            Long stoppingOffset = currentTableStoppingOffsets.get(bucket);
            if (stoppingOffset == null) {
                continue;
            }

            List<R> records = recordsExtractor.apply(bucket);

            // consumedUpToOffset is an exclusive upper bound: all offsets before it have been
            // consumed by the scanner in this poll round. It may advance even when records is
            // empty, e.g. when FIRST_ROW filters duplicate upserts into empty WAL batches.
            Long consumedUpToOffset = consumedUpToOffsetExtractor.apply(bucket);
            checkState(
                    consumedUpToOffset != null,
                    "Missing consumed-up-to offset for polled bucket %s.",
                    bucket);

            // Write records to the lake; returns the last written timestamp,
            // or UNKNOWN_BUCKET_TIMESTAMP if no records were actually written.
            long lastWrittenTimestamp =
                    handler.handleRecords(
                            records,
                            () -> {
                                TieringSplit split =
                                        checkNotNull(currentTableSplitsByBucket.get(bucket));
                                return getOrCreateLakeWriter(
                                        bucket,
                                        split.getPartitionName(),
                                        split.getSplitIndex(),
                                        split.getTieringRoundTimestamp());
                            },
                            stoppingOffset);

            // The split owns offsets before stoppingOffset only. If the scanner consumed past
            // the split boundary, cap the tiered progress at stoppingOffset so the next split
            // still owns later data.
            long tieredLogEndOffset = Math.min(consumedUpToOffset, stoppingOffset);
            long tieredTimestamp;
            if (lastWrittenTimestamp >= 0) {
                tieredTimestamp = lastWrittenTimestamp;
            } else {
                LogOffsetAndTimestamp latest = currentTableTieredOffsetAndTimestamp.get(bucket);
                tieredTimestamp = latest != null ? latest.timestamp : UNKNOWN_BUCKET_TIMESTAMP;
            }
            currentTableTieredOffsetAndTimestamp.put(
                    bucket, new LogOffsetAndTimestamp(tieredLogEndOffset - 1, tieredTimestamp));

            // The split owns offsets below stoppingOffset. If the scanner has not consumed up
            // to that exclusive bound yet, keep the split active.
            if (consumedUpToOffset < stoppingOffset) {
                continue;
            }

            // Split completion: unsubscribe, remove split, complete lake writer.
            currentTableStoppingOffsets.remove(bucket);
            if (bucket.getPartitionId() != null) {
                currentLogScanner.unsubscribe(bucket.getPartitionId(), bucket.getBucket());
            } else {
                // todo: should unsubscribe the log split if unsubscribe bucket for
                // un-partitioned table is supported
            }
            TieringSplit currentTieringSplit = currentTableSplitsByBucket.remove(bucket);
            String currentSplitId = currentTieringSplit.splitId();
            writeResults.add(
                    completeLakeWriter(
                            bucket,
                            currentTieringSplit.getPartitionName(),
                            stoppingOffset,
                            tieredTimestamp));
            anyFinished = true;
            LOG.info(
                    "Finish tier bucket {} for table {}, split: {}.",
                    bucket,
                    currentTablePath,
                    currentSplitId);
        }

        if (anyFinished) {
            mayFinishCurrentTable();
        }

        return writeResults;
    }

    /**
     * Handles row-based ScanRecord writing for the log path.
     *
     * @return the timestamp of the last written record, or -1 if no records were written
     */
    private long handleLogRecords(
            List<ScanRecord> records,
            SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
            long stoppingOffset)
            throws IOException {
        long lastWrittenTimestamp = UNKNOWN_BUCKET_TIMESTAMP;
        LakeWriter<?> lakeWriter = null;
        for (ScanRecord record : records) {
            if (record.logOffset() < stoppingOffset) {
                if (lakeWriter == null) {
                    lakeWriter = lakeWriterSupplier.get();
                }
                lakeWriter.write(record);
                lastWrittenTimestamp = record.timestamp();
                if (record.getSizeInBytes() > 0) {
                    tieringMetrics.recordBytesRead(record.getSizeInBytes());
                }
            }
        }
        return lastWrittenTimestamp;
    }

    /**
     * Handles Arrow batch writing for the record batch path.
     *
     * @return the timestamp of the last written batch, or -1 if no batches were written
     */
    private long handleArrowBatchRecords(
            List<ArrowBatchData> batches,
            SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
            long stoppingOffset)
            throws IOException {
        SupportsRecordBatchWrite batchWriter = null;
        long lastWrittenTimestamp = UNKNOWN_BUCKET_TIMESTAMP;
        for (ArrowBatchData batch : batches) {
            long batchBaseOffset = batch.getBaseLogOffset();
            long batchRecordCount = batch.getRecordCount();
            long batchTimestamp = batch.getTimestamp();
            if (batchBaseOffset >= stoppingOffset) {
                batch.close();
                continue;
            }

            long writableRowCount = stoppingOffset - batchBaseOffset;
            int writableRows = (int) Math.min(batchRecordCount, writableRowCount);
            if (writableRows <= 0) {
                batch.close();
                continue;
            }

            if (batchWriter == null) {
                LakeWriter<?> lakeWriter = lakeWriterSupplier.get();
                if (!(lakeWriter instanceof SupportsRecordBatchWrite)) {
                    throw new IOException(
                            "LakeWriter does not support RecordBatch writes: "
                                    + lakeWriter.getClass().getName());
                }
                batchWriter = (SupportsRecordBatchWrite) lakeWriter;
            }

            ArrowBatchData batchToWrite = batch;
            if (writableRows < batchRecordCount) {
                batchToWrite = batch.truncateAndTransferOwnership(writableRows);
            }
            long batchSizeInBytes = batchToWrite.getSizeInBytes();
            try (ArrowRecordBatch arrowRecordBatch = new ArrowRecordBatch(batchToWrite)) {
                batchWriter.write(arrowRecordBatch);
            }
            if (batchSizeInBytes > 0) {
                tieringMetrics.recordBytesRead(batchSizeInBytes);
            }
            lastWrittenTimestamp = batchTimestamp;
        }
        return lastWrittenTimestamp;
    }

    private LakeWriter<WriteResult> getOrCreateLakeWriter(
            TableBucket bucket,
            @Nullable String partitionName,
            int splitIndex,
            long tieringRoundTimestamp)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.get(bucket);
        if (lakeWriter == null) {
            lakeWriter =
                    lakeTieringFactory.createLakeWriter(
                            new WriterInitContextImpl(
                                    currentTablePath,
                                    bucket,
                                    partitionName,
                                    currentTable.getTableInfo(),
                                    splitIndex,
                                    tieringRoundTimestamp));
            lakeWriters.put(bucket, lakeWriter);
        }
        return lakeWriter;
    }

    private BucketWriteResult<WriteResult> completeLakeWriter(
            TableBucket bucket,
            @Nullable String partitionName,
            long logEndOffset,
            long maxTimestamp)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.remove(bucket);
        WriteResult writeResult = null;
        if (lakeWriter != null) {
            writeResult = lakeWriter.complete();
            lakeWriter.close();
        }
        return toBucketWriteResult(
                currentTablePath,
                bucket,
                partitionName,
                writeResult,
                logEndOffset,
                maxTimestamp,
                checkNotNull(currentTableNumberOfSplits));
    }

    private List<BucketWriteResult<WriteResult>> forEmptySplits(Set<TieringSplit> emptySplits) {
        List<BucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        for (TieringSplit tieringSplit : emptySplits) {
            TableBucket tableBucket = tieringSplit.getTableBucket();
            writeResults.add(
                    toBucketWriteResult(
                            tieringSplit.getTablePath(),
                            tableBucket,
                            tieringSplit.getPartitionName(),
                            null,
                            UNKNOWN_BUCKET_OFFSET,
                            UNKNOWN_BUCKET_TIMESTAMP,
                            tieringSplit.getNumberOfSplits()));
        }
        return writeResults;
    }

    private void mayFinishCurrentTable() throws IOException {
        // no any pending splits for the table, just finish the table
        if (currentTableSplitsByBucket.isEmpty()) {
            finishCurrentTable();
        }
    }

    private List<BucketWriteResult<WriteResult>> finishCurrentSnapshotSplit() throws IOException {
        TableBucket tableBucket = currentSnapshotSplit.getTableBucket();
        long logEndOffset = currentSnapshotSplit.getLogOffsetOfSnapshot();
        String splitId = currentTableSplitsByBucket.remove(tableBucket).splitId();
        BucketWriteResult<WriteResult> writeResult =
                completeLakeWriter(
                        tableBucket,
                        currentSnapshotSplit.getPartitionName(),
                        logEndOffset,
                        UNKNOWN_BUCKET_TIMESTAMP);
        LOG.info(
                "Finish tier bucket {} for table {}, split: {}.",
                tableBucket,
                currentTablePath,
                splitId);
        closeCurrentSnapshotSplit();
        mayFinishCurrentTable();
        List<BucketWriteResult<WriteResult>> writeResults = new ArrayList<>(1);
        writeResults.add(writeResult);
        return writeResults;
    }

    private List<BucketWriteResult<WriteResult>> forSnapshotSplitRecords(
            TableBucket bucket, CloseableIterator<RecordAndPos> recordIterator) throws IOException {
        LakeWriter<WriteResult> lakeWriter = null;
        while (recordIterator.hasNext()) {
            ScanRecord scanRecord = recordIterator.next().record();
            if (lakeWriter == null) {
                lakeWriter =
                        getOrCreateLakeWriter(
                                bucket,
                                checkNotNull(currentSnapshotSplit).getPartitionName(),
                                currentSnapshotSplit.getSplitIndex(),
                                currentSnapshotSplit.getTieringRoundTimestamp());
            }
            lakeWriter.write(scanRecord);
            if (scanRecord.getSizeInBytes() > 0) {
                tieringMetrics.recordBytesRead(scanRecord.getSizeInBytes());
            }
        }
        recordIterator.close();
        return emptyWriteResults();
    }

    private List<BucketWriteResult<WriteResult>> emptyWriteResults() {
        return new ArrayList<>(0);
    }

    private void closeCurrentSnapshotSplit() throws IOException {
        try {
            currentSnapshotSplitReader.close();
        } catch (Exception e) {
            throw new IOException("Fail to close current snapshot split reader.", e);
        }
        currentSnapshotSplitReader = null;
        currentSnapshotSplit = null;
    }

    private void finishCurrentTable() throws IOException {
        try {
            if (currentLogScanner != null) {
                currentLogScanner.close();
                currentLogScanner = null;
            }

            if (currentSnapshotSplitReader != null) {
                currentSnapshotSplitReader.close();
                currentSnapshotSplitReader = null;
            }

            if (currentTable != null) {
                currentTable.close();
                currentTable = null;
            }
        } catch (Exception e) {
            throw new IOException("Fail to finish current table.", e);
        }
        reachTieringMaxDurationTables.remove(currentTableId);
        // before switch to a new table, mark all as empty or null
        currentTableId = null;
        currentTablePath = null;
        currentTableNumberOfSplits = null;
        currentTableUseRecordBatchPath = null;
        currentPendingSnapshotSplits.clear();
        currentTableStoppingOffsets.clear();
        currentTableTieredOffsetAndTimestamp.clear();
        currentTableSplitsByBucket.clear();
    }

    /**
     * Handle a table reach max tiering duration. This will mark the current table as reaching max
     * duration, and it will be force completed in the next poll cycle. It is called directly by the
     * daemon's force-finish timer.
     */
    public void handleTableReachTieringMaxDuration(long tableId) {
        LOG.info(
                "handleTableReachTieringMaxDuration, currentTableId: {}, pendingTieringSplits: {}",
                currentTableId,
                pendingTieringSplits);
        if ((currentTableId != null && currentTableId.equals(tableId))
                || pendingTieringSplits.containsKey(tableId)) {
            LOG.info("Table {} reach tiering max duration, will force to complete.", tableId);
            reachTieringMaxDurationTables.add(tableId);
        }
    }

    /** Wakes up the current log scanner so a blocking poll returns promptly. */
    public void wakeUp() {
        if (currentLogScanner != null) {
            currentLogScanner.wakeup();
        }
    }

    /**
     * Closes the reader, releasing the current log scanner and table (the connection is owned by
     * the daemon).
     */
    public void close() throws Exception {
        if (currentLogScanner != null) {
            currentLogScanner.close();
        }
        if (currentTable != null) {
            currentTable.close();
        }

        // don't need to close connection, will be closed by the daemon (TieringService)
    }

    private void subscribeLog(TieringLogSplit logSplit) {
        // assign bucket offset dynamically
        TableBucket tableBucket = logSplit.getTableBucket();
        long stoppingOffset = logSplit.getStoppingOffset();
        long startingOffset = logSplit.getStartingOffset();
        if (startingOffset >= stoppingOffset || stoppingOffset <= 0) {
            currentEmptySplits.add(logSplit);
            return;
        } else {
            currentTableStoppingOffsets.put(tableBucket, stoppingOffset);
        }

        mayCreateLogScanner();
        Long partitionId = tableBucket.getPartitionId();
        int bucket = tableBucket.getBucket();
        checkNotNull(currentLogScanner, "current log scanner shouldn't be null.");
        if (partitionId != null) {
            currentLogScanner.subscribe(partitionId, bucket, startingOffset);
        } else {
            // If no partition id, subscribe by bucket only.
            currentLogScanner.subscribe(bucket, startingOffset);
        }
        LOG.info(
                "Subscribe to read log for split {} from starting offset {} to end offset {}.",
                logSplit.splitId(),
                startingOffset,
                stoppingOffset);
    }

    private BucketWriteResult<WriteResult> toBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long endLogOffset,
            long maxTimestamp,
            int numberOfSplits) {
        return new BucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                endLogOffset,
                maxTimestamp,
                numberOfSplits);
    }

    /**
     * Callback interface for processing records within a single bucket. Encapsulates the
     * differences in write strategy between the row-based (ScanRecord) and Arrow batch
     * (ArrowBatchData) paths.
     *
     * @param <R> the record type (ScanRecord or ArrowBatchData)
     */
    @FunctionalInterface
    private interface BucketRecordsHandler<R> {

        /**
         * Processes the records for a bucket and writes them to the lake.
         *
         * @param records the records for this bucket
         * @param lakeWriterSupplier supplier for lazily creating the lake writer
         * @param stoppingOffset the stopping offset for this bucket
         * @return the timestamp of the last written record, or -1 if no records were written
         * @throws IOException if an I/O error occurs during writing
         */
        long handleRecords(
                List<R> records,
                SupplierWithException<LakeWriter<?>, IOException> lakeWriterSupplier,
                long stoppingOffset)
                throws IOException;
    }

    private static boolean checkUnshadedArrowAvailable(ClassLoader classLoader) {
        try {
            Class.forName("org.apache.arrow.vector.VectorSchemaRoot", false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static final class LogOffsetAndTimestamp {

        private final long logOffset;
        private final long timestamp;

        public LogOffsetAndTimestamp(long logOffset, long timestamp) {
            this.logOffset = logOffset;
            this.timestamp = timestamp;
        }
    }
}
