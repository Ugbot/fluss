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

package org.apache.fluss.jmh;

import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatchFormat;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ProjectionPushdownCache;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;

/**
 * Benchmark for the fetch projection-pushdown hot path: {@link FileLogProjection#project} reading a
 * wide Arrow-format batch from a {@link FileLogRecords} file and emitting only a subset of columns
 * directly as zero-copy {@code BytesView} slices, without ever decoding the row values.
 *
 * <p>The number of projected columns is swept via {@code @Param} ({@code projectedColumns}) against
 * a fixed-width source schema of {@link #TOTAL_COLUMNS} columns. This isolates how the cost of a
 * projection scales with the size of the selected column subset (project few vs many columns from a
 * wider batch). Because the projection metadata for a given (tableId, schemaId, selectedFields)
 * triple is cached in the shared {@link ProjectionPushdownCache}, repeated invocations exercise the
 * steady-state path that the TabletServer hits per fetch request.
 *
 * <p>The source file is built once per trial: {@link #ROW_COUNT} randomized rows (deterministic
 * seed, so each run is reproducible) are encoded into a single Arrow batch and flushed to a temp
 * file. Each benchmark invocation re-runs {@code project()} over that file with {@code maxBytes =
 * Integer.MAX_VALUE} so the full batch is always projected.
 */
@State(Scope.Thread)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class FileLogProjectionBenchmark {

    private static final int SCHEMA_ID = 1;
    private static final long TABLE_ID = 1L;
    private static final long DETERMINISTIC_SEED = 0x5DEECE66DL;
    /** Width of the source schema: alternating INT and STRING columns. */
    private static final int TOTAL_COLUMNS = 32;
    /** Number of rows encoded into the single source Arrow batch. */
    private static final int ROW_COUNT = 2000;

    /** Number of leading columns to project out of {@link #TOTAL_COLUMNS}. */
    @Param({"1", "4", "8", "16", "32"})
    private int projectedColumns;

    private File logFile;
    private FileLogRecords fileLogRecords;
    private TestingSchemaGetter schemaGetter;
    private FileLogProjection projection;
    private int[] selectedFieldPositions;
    private int fileSizeInBytes;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Schema schema = buildWideSchema(TOTAL_COLUMNS);
        RowType rowType = schema.getRowType();
        this.schemaGetter = new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));

        List<Object[]> rows = generateRows(ROW_COUNT, TOTAL_COLUMNS);
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.createRecordsWithoutBaseLogOffset(
                        rowType,
                        SCHEMA_ID,
                        0L,
                        System.currentTimeMillis(),
                        LogRecordBatchFormat.LOG_MAGIC_VALUE_V2,
                        rows,
                        LogFormat.ARROW);

        File tempDir = Files.createTempDirectory("fluss-jmh-projection").toFile();
        this.logFile = new File(tempDir, UUID.randomUUID() + ".log");
        this.fileLogRecords = FileLogRecords.open(logFile);
        this.fileLogRecords.append(memoryLogRecords);
        this.fileLogRecords.flush();
        this.fileSizeInBytes = fileLogRecords.sizeInBytes();

        // Project the leading `projectedColumns` columns. Projection indexes must be ascending and
        // within bounds, both of which hold here since projectedColumns in [1, TOTAL_COLUMNS].
        this.selectedFieldPositions = new int[projectedColumns];
        for (int i = 0; i < projectedColumns; i++) {
            selectedFieldPositions[i] = i;
        }

        this.projection = new FileLogProjection(new ProjectionPushdownCache());
        this.projection.setCurrentProjection(
                TABLE_ID, schemaGetter, DEFAULT_COMPRESSION, selectedFieldPositions);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (fileLogRecords != null) {
            fileLogRecords.close();
        }
        if (logFile != null) {
            File parent = logFile.getParentFile();
            Files.deleteIfExists(logFile.toPath());
            if (parent != null) {
                Files.deleteIfExists(parent.toPath());
            }
        }
    }

    /**
     * Project the source batch to the selected column subset. Exercises Arrow IPC metadata parsing,
     * buffer/field-node selection and zero-copy {@code BytesView} assembly via the cached projection
     * info.
     */
    @Benchmark
    public void project(Blackhole bh) throws Exception {
        bh.consume(
                projection.project(
                        fileLogRecords.channel(), 0, fileSizeInBytes, Integer.MAX_VALUE));
    }

    private static Schema buildWideSchema(int columns) {
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < columns; i++) {
            if ((i & 1) == 0) {
                builder.column("c" + i, DataTypes.INT());
            } else {
                builder.column("c" + i, DataTypes.STRING());
            }
        }
        return builder.build();
    }

    private static List<Object[]> generateRows(int rowCount, int columns) {
        Random random = new Random(DETERMINISTIC_SEED);
        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            Object[] row = new Object[columns];
            for (int c = 0; c < columns; c++) {
                if ((c & 1) == 0) {
                    row[c] = random.nextInt();
                } else {
                    row[c] = randomString(random, 8 + random.nextInt(24));
                }
            }
            rows.add(row);
        }
        return rows;
    }

    private static String randomString(Random random, int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            // printable ASCII range [33, 126].
            chars[i] = (char) (33 + random.nextInt(94));
        }
        return new String(chars);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*" + FileLogProjectionBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
