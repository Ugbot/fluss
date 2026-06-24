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

import org.apache.fluss.utils.crc.Crc32C;
import org.apache.fluss.utils.crc.PureJavaCrc32C;

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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.Checksum;

/**
 * Microbenchmark for the record-batch CRC32C (Castagnoli) hot path used by the Fluss log format.
 *
 * <p>Every {@code DefaultLogRecordBatch} writes and validates a CRC over the batch body (see {@code
 * DefaultLogRecordBatch#computeChecksum()} which calls {@link Crc32C#compute(ByteBuffer, int, int)}
 * starting at the schema-id offset). The same path is exercised on the write side when a batch is
 * sealed and on the read side when a batch is validated, so it sits directly on the produce/fetch
 * critical path — including the Kafka-compatible produce/fetch surface that reuses this batch
 * format.
 *
 * <p>This benchmark sweeps realistic batch-body payload sizes and compares:
 *
 * <ul>
 *   <li>{@code flussArray} — {@link Crc32C#compute(byte[], int, int)}, the array entry point.
 *   <li>{@code flussHeapBuffer} — {@link Crc32C#compute(ByteBuffer, int, int)} over a heap buffer,
 *       the array-backed shape the batch validation path takes.
 *   <li>{@code flussDirectBuffer} — the same over a direct (off-heap) buffer, the shape network
 *       receive buffers take.
 *   <li>{@code pureJavaArray} — the {@link PureJavaCrc32C} software fallback used when the
 *       intrinsified {@code java.util.zip.CRC32C} is unavailable (pre-Java-9 source level).
 * </ul>
 *
 * <p>On Java 9+ {@link Crc32C} delegates to {@code java.util.zip.CRC32C}, which the JIT lowers to
 * the SSE4.2 {@code CRC32} instruction; the {@code pureJava*} variants quantify the cost of the
 * software fallback so the intrinsic win is measurable. Payloads are randomized with a fixed seed
 * for repeatable runs (project policy: no hardcoded sample data).
 *
 * <p>Run: {@code mvn -pl fluss-jmh test-compile} then execute {@code main}, or {@code java ...
 * org.apache.fluss.jmh.CrcChecksumBenchmark}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class CrcChecksumBenchmark {

    /** Seed fixed so the randomized payload is identical across runs and across variants. */
    private static final long SEED = 0xC0FFEEL;

    /**
     * Batch-body sizes in bytes. 64 spans the small control-record / single-tiny-row case, 1 KiB
     * and 16 KiB the common append-log row batches, and 256 KiB a large Arrow batch that spills out
     * of L2.
     */
    @Param({"64", "1024", "16384", "262144"})
    private int payloadSize;

    private byte[] array;
    private ByteBuffer heapBuffer;
    private ByteBuffer directBuffer;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(SEED);
        array = new byte[payloadSize];
        random.nextBytes(array);

        heapBuffer = ByteBuffer.allocate(payloadSize);
        heapBuffer.put(array);
        heapBuffer.position(0);

        directBuffer = ByteBuffer.allocateDirect(payloadSize);
        directBuffer.put(array);
        directBuffer.position(0);
    }

    @Benchmark
    public long flussArray() {
        return Crc32C.compute(array, 0, payloadSize);
    }

    @Benchmark
    public long flussHeapBuffer() {
        return Crc32C.compute(heapBuffer, 0, payloadSize);
    }

    @Benchmark
    public long flussDirectBuffer() {
        return Crc32C.compute(directBuffer, 0, payloadSize);
    }

    @Benchmark
    public long pureJavaArray() {
        Checksum crc = new PureJavaCrc32C();
        crc.update(array, 0, payloadSize);
        return crc.getValue();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + CrcChecksumBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
