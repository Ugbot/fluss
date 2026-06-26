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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Baseline microbenchmark for typed field access on {@link IndexedRow}, a concrete {@link
 * org.apache.fluss.row.BinaryRow} implementation backed by {@link
 * org.apache.fluss.memory.MemorySegment}. {@code IndexedRow} (and the {@link
 * org.apache.fluss.row.BinarySegmentUtils} it delegates to for variable-length fields) sits on the
 * read hot path: every record decoded from a fetched log batch is materialized as a binary row, and
 * downstream operators pull {@code int}/{@code long}/{@code String} fields out of it field by
 * field.
 *
 * <p>Purpose: this is the reference number for the planned migration of {@code MemorySegment} (and
 * therefore of {@code BinaryRow} field access through {@code BinarySegmentUtils}) off {@code
 * sun.misc.Unsafe} onto the Java 25 Foreign Function &amp; Memory API. Field decode is the
 * realistic consumer pattern, so this benchmark complements {@link MemorySegmentBenchmark} (raw
 * primitive accessors) by measuring the row-decode layer that callers actually use. Re-run after
 * that migration: it must not regress.
 *
 * <p>The row layout cycles {@code INT}, {@code BIGINT}, {@code STRING} across a {@code fieldCount}
 * sweep so the mix of fixed-width (in-place segment read) and variable-length (offset/length
 * indirection through {@code BinarySegmentUtils}) access scales with the row width. Payload is
 * seeded-random per project policy. {@code getInt}/{@code getLong} read directly from the backing
 * segment; {@code getString} returns a {@link BinaryString} pointing into the segment, so the
 * string benchmark also forces a materialization to {@code String} to exercise the copy-out path.
 *
 * <p>Run: {@code mvn -pl fluss-jmh test-compile} then execute {@code main}, or {@code java ...
 * org.apache.fluss.jmh.BinaryRowAccessBenchmark}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class BinaryRowAccessBenchmark {

    /** Deterministic seed so payload is repeatable across runs. */
    private static final long SEED = 0xB1A2C3D4L;

    /** Field-count sweep: narrow rows up to wide rows that span many cache lines. */
    @Param({"4", "16", "64", "256"})
    private int fieldCount;

    private DataType[] dataTypes;
    private IndexedRow row;

    private int[] intPositions;
    private int[] longPositions;
    private int[] stringPositions;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(SEED);

        dataTypes = new DataType[fieldCount];
        int intCount = 0;
        int longCount = 0;
        int stringCount = 0;
        for (int i = 0; i < fieldCount; i++) {
            switch (i % 3) {
                case 0:
                    dataTypes[i] = DataTypes.INT();
                    intCount++;
                    break;
                case 1:
                    dataTypes[i] = DataTypes.BIGINT();
                    longCount++;
                    break;
                default:
                    dataTypes[i] = DataTypes.STRING();
                    stringCount++;
                    break;
            }
        }

        intPositions = new int[intCount];
        longPositions = new int[longCount];
        stringPositions = new int[stringCount];

        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        int intIdx = 0;
        int longIdx = 0;
        int stringIdx = 0;
        for (int i = 0; i < fieldCount; i++) {
            switch (i % 3) {
                case 0:
                    writer.writeInt(random.nextInt());
                    intPositions[intIdx++] = i;
                    break;
                case 1:
                    writer.writeLong(random.nextLong());
                    longPositions[longIdx++] = i;
                    break;
                default:
                    // Variable-length payload of varying width to exercise the offset/length
                    // indirection rather than a single hot length.
                    writer.writeString(BinaryString.fromString(randomString(random)));
                    stringPositions[stringIdx++] = i;
                    break;
            }
        }

        row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());
    }

    private static String randomString(Random random) {
        int length = 4 + random.nextInt(28);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    @Benchmark
    public long readAllInts() {
        long sum = 0;
        for (int pos : intPositions) {
            sum += row.getInt(pos);
        }
        return sum;
    }

    @Benchmark
    public long readAllLongs() {
        long sum = 0;
        for (int pos : longPositions) {
            sum += row.getLong(pos);
        }
        return sum;
    }

    @Benchmark
    public void readAllStrings(Blackhole bh) {
        for (int pos : stringPositions) {
            // Force materialization to String to exercise the BinarySegmentUtils copy-out path,
            // not just the zero-copy BinaryString view.
            bh.consume(row.getString(pos).toString());
        }
    }

    @Benchmark
    public void readAllFields(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            switch (i % 3) {
                case 0:
                    bh.consume(row.getInt(i));
                    break;
                case 1:
                    bh.consume(row.getLong(i));
                    break;
                default:
                    bh.consume(row.getString(i));
                    break;
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + BinaryRowAccessBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
