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

import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatchFormat;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataField;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;

/**
 * Benchmark for the produce hot path: building an Arrow-format {@link
 * org.apache.fluss.record.MemoryLogRecords} batch from rows via {@link
 * MemoryLogRecordsArrowBuilder}.
 *
 * <p>Each invocation simulates what the writer does when flushing a batch: obtain an {@link
 * ArrowWriter} from the pool, create a builder over a fresh paged output view, append every
 * pre-generated row through the Arrow writer (and change-type vector), then {@link
 * MemoryLogRecordsArrowBuilder#build()} the serialized batch. The Arrow writer is recycled back to
 * the pool on {@code build()}, so the pool is reused across invocations exactly as in production.
 *
 * <p>Row counts are swept via {@code @Param}. Rows are randomized with a deterministic seed so each
 * run is reproducible. The buffer is sized large enough to hold all rows of the largest sweep so
 * the builder never reports full mid-append.
 */
@State(Scope.Thread)
@Warmup(iterations = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class MemoryLogRecordsArrowBuildBenchmark {

    private static final int DEFAULT_SCHEMA_ID = 1;
    private static final long TABLE_ID = 1L;
    private static final long DETERMINISTIC_SEED = 0x5DEECE66DL;
    // page size large enough to hold the biggest sweep without the builder reporting full.
    private static final int PAGE_SIZE_IN_BYTES = 8 * 1024 * 1024;

    @Param({"10", "100", "1000", "10000"})
    private int rowCount;

    private final RowType rowType =
            new RowType(
                    Arrays.asList(
                            new DataField("id", DataTypes.INT()),
                            new DataField("name", DataTypes.STRING()),
                            new DataField("amount", DataTypes.BIGINT()),
                            new DataField("ratio", DataTypes.DOUBLE())));

    private BufferAllocator allocator;
    private ArrowWriterPool writerPool;
    private List<InternalRow> rows;

    @Setup(Level.Trial)
    public void setup() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.writerPool = new ArrowWriterPool(allocator);
        this.rows = generateRows(rowCount);
    }

    @TearDown(Level.Trial)
    public void teardown() {
        writerPool.close();
        allocator.close();
    }

    /**
     * Build a single Arrow log-records batch from all pre-generated rows. This exercises Arrow
     * vector writes, the change-type vector, batch-header CRC computation and zero-copy
     * serialization into the paged output view.
     */
    @Benchmark
    public void buildArrowBatch(Blackhole bh) throws Exception {
        ArrowWriter writer =
                writerPool.getOrCreateWriter(
                        TABLE_ID, DEFAULT_SCHEMA_ID, PAGE_SIZE_IN_BYTES, rowType, NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        0L,
                        LogRecordBatchFormat.LOG_MAGIC_VALUE_V2,
                        DEFAULT_SCHEMA_ID,
                        writer,
                        new ManagedPagedOutputView(
                                new TestingMemorySegmentPool(PAGE_SIZE_IN_BYTES)));
        for (int i = 0; i < rows.size(); i++) {
            builder.append(ChangeType.APPEND_ONLY, rows.get(i));
        }
        builder.setWriterState(TABLE_ID, 0);
        // close() builds the batch and recycles the Arrow writer back to the pool.
        builder.close();
        bh.consume(builder.build());
    }

    private List<InternalRow> generateRows(int count) {
        Random random = new Random(DETERMINISTIC_SEED);
        List<InternalRow> generated = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            GenericRow row = new GenericRow(4);
            row.setField(0, random.nextInt());
            row.setField(1, BinaryString.fromString(randomString(random, 8 + random.nextInt(24))));
            row.setField(2, random.nextLong());
            row.setField(3, random.nextDouble());
            generated.add(row);
        }
        return generated;
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
                                ".*"
                                        + MemoryLogRecordsArrowBuildBenchmark.class
                                                .getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(opt).run();
    }
}
