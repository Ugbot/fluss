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

package org.apache.fluss.lake.tiering.committer;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.TieringStats;
import org.apache.fluss.lake.tiering.reader.BucketWriteResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Engine-agnostic core that aggregates the per-bucket {@code WriteResult}s collected for a single
 * table into a single {@code Committable}, commits it to the lake via {@link LakeCommitter}, and
 * then records the committed lake snapshot back to the Fluss cluster via {@link
 * FlussTableLakeSnapshotCommitter}.
 *
 * <p>This is the extraction of {@code TieringCommitOperator.commitWriteResults(...)} from the Flink
 * connector. It depends only on the lake SPI ({@link LakeTieringFactory}, {@link LakeCommitter})
 * and the Fluss client ({@link Admin}, {@link FlussTableLakeSnapshotCommitter}); it contains no
 * Flink types so it can be driven by any tiering engine.
 *
 * <p>The caller is responsible for collecting all {@link BucketWriteResult}s of a round of tiering
 * for a table (i.e. once {@link BucketWriteResult#numberOfWriteResults()} results have arrived) and
 * passing them to {@link #commit(long, TablePath, List)}.
 *
 * @param <WriteResult> the per-bucket write result type produced by the lake writer
 * @param <Committable> the per-table committable type consumed by the lake committer
 */
public class TableTieringCommitter<WriteResult, Committable> {

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final Admin admin;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;

    public TableTieringCommitter(
            Configuration flussConfig,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory,
            Admin admin,
            FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter) {
        this.flussConfig = checkNotNull(flussConfig, "flussConfig must not be null.");
        this.lakeTieringConfig =
                checkNotNull(lakeTieringConfig, "lakeTieringConfig must not be null.");
        this.lakeTieringFactory =
                checkNotNull(lakeTieringFactory, "lakeTieringFactory must not be null.");
        this.admin = checkNotNull(admin, "admin must not be null.");
        this.flussTableLakeSnapshotCommitter =
                checkNotNull(
                        flussTableLakeSnapshotCommitter,
                        "flussTableLakeSnapshotCommitter must not be null.");
    }

    /**
     * Commits the collected write results for one table to the lake and then to Fluss.
     *
     * <p>Always returns a non-null {@link CommitResult}. When all buckets produced no data (empty
     * commit), {@link CommitResult#committable()} is {@code null} and {@link CommitResult#stats()}
     * is {@code null}.
     *
     * @param tableId the id of the table being committed
     * @param tablePath the path of the table being committed
     * @param bucketWriteResults all the bucket write results collected for this round of tiering
     * @return the result of the commit round, holding the lake committable and the tiering stats
     * @throws Exception if the commit to the lake or to Fluss fails
     */
    public CommitResult<Committable> commit(
            long tableId,
            TablePath tablePath,
            List<BucketWriteResult<WriteResult>> bucketWriteResults)
            throws Exception {
        // filter down to buckets that actually produced data
        List<BucketWriteResult<WriteResult>> nonEmptyResults =
                bucketWriteResults.stream()
                        .filter(r -> r.writeResult() != null)
                        .collect(Collectors.toList());

        // all buckets were empty — nothing to commit to the lake
        if (nonEmptyResults.isEmpty()) {
            return new CommitResult<>(null, null);
        }

        // Check if the table was dropped and recreated during tiering.
        // If the current table id differs from the committable's table id, fail this commit
        // to avoid dirty commit to a newly created table.
        TableInfo currentTableInfo = admin.getTableInfo(tablePath).get();
        if (currentTableInfo.getTableId() != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "The current table id %s for table path %s is different from the table id %s in the committable. "
                                    + "This usually happens when a table was dropped and recreated during tiering. "
                                    + "Aborting commit to prevent dirty commit.",
                            currentTableInfo.getTableId(), tablePath, tableId));
        }

        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new CommitterInitContextImpl(
                                tablePath, currentTableInfo, lakeTieringConfig, flussConfig))) {
            List<WriteResult> writeResults =
                    nonEmptyResults.stream()
                            .map(BucketWriteResult::writeResult)
                            .collect(Collectors.toList());

            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            Map<TableBucket, Long> logMaxTieredTimestamps = new HashMap<>();
            for (BucketWriteResult<WriteResult> writeResult : nonEmptyResults) {
                TableBucket tableBucket = writeResult.tableBucket();
                logEndOffsets.put(tableBucket, writeResult.logEndOffset());
                logMaxTieredTimestamps.put(tableBucket, writeResult.maxTimestamp());
            }

            // to committable
            Committable committable = lakeCommitter.toCommittable(writeResults);
            // before commit to lake, check fluss not missing any lake snapshot committed by fluss
            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
            checkFlussNotMissingLakeSnapshot(
                    tablePath,
                    tableId,
                    lakeCommitter,
                    committable,
                    flussCurrentLakeSnapshot == null
                            ? null
                            : flussCurrentLakeSnapshot.getSnapshotId());

            // get the lake bucket offsets file storing the log end offsets
            String lakeBucketTieredOffsetsFile =
                    flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                            tableId, tablePath, logEndOffsets);

            // record the lake snapshot bucket offsets file to snapshot property
            Map<String, String> snapshotProperties =
                    Collections.singletonMap(
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeBucketTieredOffsetsFile);
            LakeCommitResult lakeCommitResult =
                    lakeCommitter.commit(committable, snapshotProperties);
            // commit to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    tablePath,
                    lakeCommitResult,
                    lakeBucketTieredOffsetsFile,
                    logEndOffsets,
                    logMaxTieredTimestamps);
            return new CommitResult<>(committable, lakeCommitResult.getTieringStats());
        }
    }

    @Nullable
    private LakeSnapshot getLatestLakeSnapshot(TablePath tablePath) throws Exception {
        LakeSnapshot flussCurrentLakeSnapshot;
        try {
            flussCurrentLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                // do-nothing
                flussCurrentLakeSnapshot = null;
            } else {
                throw e;
            }
        }
        return flussCurrentLakeSnapshot;
    }

    private void checkFlussNotMissingLakeSnapshot(
            TablePath tablePath,
            long tableId,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            Long flussCurrentLakeSnapshot)
            throws Exception {
        // get Fluss missing lake snapshot in Lake
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot);

        // fluss's known snapshot is less than lake snapshot committed by fluss
        // fail this commit since the data is read from the log end-offset of a invalid fluss
        // known lake snapshot, which means the data already has been committed to lake,
        // not to commit to lake to avoid data duplicated
        if (missingCommittedSnapshot != null) {
            String lakeSnapshotOffsetPath =
                    missingCommittedSnapshot
                            .getSnapshotProperties()
                            .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);

            // should only will happen in v0.7 which won't put offsets info
            // to properties
            if (lakeSnapshotOffsetPath == null) {
                throw new IllegalStateException(
                        String.format(
                                "Can't find %s field from snapshot property.",
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY));
            }

            // the fluss-offsets will be a json string if it's tiered by v0.8,
            // since this code path should be rare, we do not consider backward compatibility
            // and throw IllegalStateException directly
            String trimmedPath = lakeSnapshotOffsetPath.trim();
            if (trimmedPath.contains("{")) {
                throw new IllegalStateException(
                        String.format(
                                "The %s field in snapshot property is a JSON string (tiered by v0.8), "
                                        + "which is not supported to restore. Snapshot ID: %d, Table: {tablePath=%s, tableId=%d}.",
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                missingCommittedSnapshot.getLakeSnapshotId(),
                                tablePath,
                                tableId));
            }

            // commit this missing snapshot to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    missingCommittedSnapshot.getLakeSnapshotId(),
                    lakeSnapshotOffsetPath,
                    // don't care readable snapshot and offsets,
                    null,
                    // use empty log offsets, log max timestamp, since we can't know that
                    // in last tiering, it doesn't matter for they are just used to
                    // report metrics
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    LakeCommitResult.KEEP_ALL_PREVIOUS);
            // abort this committable to delete the written files
            lakeCommitter.abort(committable);
            throw new IllegalStateException(
                    String.format(
                            "The current Fluss's lake snapshot %d is less than"
                                    + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                    + " missing snapshot: %s.",
                            flussCurrentLakeSnapshot,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId,
                            missingCommittedSnapshot));
        }
    }

    /**
     * The result of one table's commit round, holding the lake committable (nullable for empty
     * commits where no data was written) and the associated tiering statistics.
     *
     * @param <Committable> the per-table committable type produced by the lake committer
     */
    public static final class CommitResult<Committable> {
        /** The lake committable, or {@code null} if nothing was written in this round. */
        @Nullable private final Committable committable;

        /** Per-table tiering statistics collected during this round, or {@code null} if empty. */
        @Nullable private final TieringStats stats;

        public CommitResult(@Nullable Committable committable, @Nullable TieringStats stats) {
            this.committable = committable;
            this.stats = stats;
        }

        /** Returns the lake committable, or {@code null} if nothing was written in this round. */
        @Nullable
        public Committable committable() {
            return committable;
        }

        /** Returns the per-table tiering stats, or {@code null} for an empty commit. */
        @Nullable
        public TieringStats stats() {
            return stats;
        }
    }
}
