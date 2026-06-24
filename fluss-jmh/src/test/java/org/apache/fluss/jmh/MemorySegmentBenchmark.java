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

import org.apache.fluss.memory.MemorySegment;

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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Baseline microbenchmark for the {@link MemorySegment} hot-path primitive accessors, which sit
 * underneath the Arrow log codec, record (de)serialization and the Kafka typed fetch path.
 *
 * <p>Purpose: this is the reference number for the planned migration of {@code MemorySegment} off
 * {@code sun.misc.Unsafe} onto the Java 25 Foreign Function &amp; Memory API ({@code
 * java.lang.foreign.MemorySegment}/{@code Arena}). Re-run after that migration: a naive FFM port
 * can be slower than Unsafe without confined arenas, so this benchmark must not regress.
 *
 * <p>Mechanical-sympathy lens: compares on-heap vs off-heap backing and sequential int/long
 * accessors vs bulk byte copy. Sequential access is the realistic codec pattern.
 *
 * <p>Run: {@code mvn -pl fluss-jmh test-compile} then execute {@code main}, or {@code java ...
 * org.apache.fluss.jmh.MemorySegmentBenchmark}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class MemorySegmentBenchmark {

    /**
     * 64 KiB — comfortably larger than L1, exercises L2 and the accessor path, not the allocator.
     */
    private static final int SIZE = 64 * 1024;

    private static final int INT_COUNT = SIZE / Integer.BYTES;
    private static final int LONG_COUNT = SIZE / Long.BYTES;

    @Param({"heap", "offheap"})
    private String backing;

    private MemorySegment segment;
    private byte[] src;
    private byte[] dst;

    @Setup(Level.Trial)
    public void setup() {
        segment =
                "heap".equals(backing)
                        ? MemorySegment.allocateHeapMemory(SIZE)
                        : MemorySegment.allocateOffHeapMemory(SIZE);

        // Randomized payload per project policy (deterministic seed for repeatable runs).
        Random random = new Random(0xF155L);
        src = new byte[SIZE];
        dst = new byte[SIZE];
        random.nextBytes(src);

        // Pre-fill the segment so the read benchmarks observe real values.
        for (int i = 0; i < INT_COUNT; i++) {
            segment.putInt(i * Integer.BYTES, random.nextInt());
        }
    }

    @Benchmark
    public long sequentialReadInt() {
        long sum = 0;
        for (int i = 0; i < INT_COUNT; i++) {
            sum += segment.getInt(i * Integer.BYTES);
        }
        return sum;
    }

    @Benchmark
    public long sequentialReadLong() {
        long sum = 0;
        for (int i = 0; i < LONG_COUNT; i++) {
            sum += segment.getLong(i * Long.BYTES);
        }
        return sum;
    }

    @Benchmark
    public MemorySegment sequentialWriteInt() {
        for (int i = 0; i < INT_COUNT; i++) {
            segment.putInt(i * Integer.BYTES, i * 0x9E3779B1);
        }
        return segment;
    }

    @Benchmark
    public MemorySegment sequentialWriteLong() {
        for (int i = 0; i < LONG_COUNT; i++) {
            segment.putLong(i * Long.BYTES, i * 0x9E3779B97F4A7C15L);
        }
        return segment;
    }

    @Benchmark
    public byte[] bulkPutThenGet() {
        segment.put(0, src);
        segment.get(0, dst);
        return dst;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + MemorySegmentBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
