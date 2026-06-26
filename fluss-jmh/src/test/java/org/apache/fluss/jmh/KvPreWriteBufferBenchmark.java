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

import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Value;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for the in-memory {@link KvPreWriteBuffer} put/get hot path on the KV write side.
 *
 * <p>On every upsert, the tablet server first writes the key-value pair into the pre-write buffer
 * ({@link KvPreWriteBuffer#insert}/{@link KvPreWriteBuffer#update}) and, to generate the CDC change
 * log, looks up the previous value for the same key ({@link KvPreWriteBuffer#get}) before the WAL
 * is persisted. Both operations are backed by a {@code HashMap<Key, KvEntry>}: each put performs a
 * {@code Map.compute} (a hash + probe to thread the previous-entry pointer) and each get performs a
 * separate {@code Map.get}. The audit flagged this per-put map lookup as the dominant cost of the
 * buffer, so this benchmark isolates it.
 *
 * <p>The benchmark drives a realistic put-then-get pair per logical key over a sweep of
 * distinct-key counts ({@code entryCount}). Each invocation:
 *
 * <ul>
 *   <li>{@code putThenGet} — for every key, {@code insert} the key (the {@code Map.compute} write
 *       path that also walks any existing entry) immediately followed by {@code get} of the same
 *       key (the {@code Map.get} CDC-lookup path), mirroring the real produce-side ordering. The
 *       buffer is reset each invocation so log sequence numbers stay strictly increasing as the
 *       buffer requires and the map grows from empty to {@code entryCount} live entries.
 *   <li>{@code getExisting} — {@code get} every key from a buffer pre-populated with {@code
 *       entryCount} entries, isolating the pure {@code Map.get} lookup cost (hash + {@code Key}
 *       equals, which compares the cached Murmur hash then the key bytes) with no write or resize
 *       interference.
 * </ul>
 *
 * <p>Keys and values are randomized with a fixed seed (project policy: no hardcoded sample data) so
 * the key set, byte contents, and therefore the hash distribution are identical across runs and
 * across the two variants. {@code keySize}/{@code valueSize} are held at representative small-row
 * sizes; sweeping {@code entryCount} across {@code HashMap} resize boundaries surfaces the load
 * factor / resize behaviour of the buffer.
 *
 * <p>The buffer's constructor requires a {@link KvBatchWriter} and a {@link
 * TabletServerMetricGroup}, but neither is touched by the {@code insert}/{@code update}/{@code get}
 * paths under test (the batch writer is only used by {@code flush}, and the counters only by {@code
 * truncateTo}). The batch writer is therefore a no-op stand-in and the metric group is built
 * against {@link NOPMetricRegistry}; no flush or truncate is exercised, so the substitution does
 * not affect the measured path.
 *
 * <p>Run: {@code mvn -pl fluss-jmh test-compile} then execute {@code main}, or {@code java ...
 * org.apache.fluss.jmh.KvPreWriteBufferBenchmark}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5)
@Fork(value = 1)
public class KvPreWriteBufferBenchmark {

    /** Seed fixed so the randomized keys/values are identical across runs and across variants. */
    private static final long SEED = 0xCAFEBABEL;

    /** A no-op batch writer; the put/get path under test never invokes it (only flush does). */
    private static final KvBatchWriter NOOP_BATCH_WRITER =
            new KvBatchWriter() {
                @Override
                public void put(byte[] key, byte[] value) throws IOException {}

                @Override
                public void delete(byte[] key) throws IOException {}

                @Override
                public void flush() throws IOException {}

                @Override
                public void close() throws Exception {}
            };

    /**
     * Number of distinct keys driven per invocation. The values straddle {@code HashMap} resize
     * thresholds (default capacity 16, load factor 0.75) so the sweep covers the small-buffer case
     * through buffers large enough to spill out of cache and exercise repeated resizing.
     */
    @Param({"1024", "8192", "65536", "262144"})
    private int entryCount;

    /** Key length in bytes — a representative primary-key encoding width. */
    @Param({"16"})
    private int keySize;

    /** Value length in bytes — a representative small-row payload width. */
    @Param({"128"})
    private int valueSize;

    private Key[] keys;
    private Value[] values;
    private TabletServerMetricGroup metricGroup;

    /** A buffer pre-populated with all keys, reused read-only by {@code getExisting}. */
    private KvPreWriteBuffer populatedBuffer;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(SEED);
        keys = new Key[entryCount];
        values = new Value[entryCount];
        for (int i = 0; i < entryCount; i++) {
            byte[] keyBytes = new byte[keySize];
            random.nextBytes(keyBytes);
            keys[i] = Key.of(keyBytes);

            byte[] valueBytes = new byte[valueSize];
            random.nextBytes(valueBytes);
            values[i] = Value.of(valueBytes);
        }

        metricGroup =
                new TabletServerMetricGroup(NOPMetricRegistry.INSTANCE, "fluss", "rack", "host", 0);

        populatedBuffer = new KvPreWriteBuffer(NOOP_BATCH_WRITER, metricGroup);
        for (int i = 0; i < entryCount; i++) {
            populatedBuffer.update(keys[i], values[i].get(), i);
        }
    }

    /**
     * Put each key then immediately get it back, mirroring the produce-side ordering where every
     * upsert writes the buffer and then reads the previous value to build the CDC change log.
     * Builds the map from empty to {@code entryCount} live entries within the invocation.
     */
    @Benchmark
    @OperationsPerInvocation(1)
    public void putThenGet(Blackhole blackhole) {
        KvPreWriteBuffer buffer = new KvPreWriteBuffer(NOOP_BATCH_WRITER, metricGroup);
        for (int i = 0; i < entryCount; i++) {
            buffer.update(keys[i], values[i].get(), i);
            blackhole.consume(buffer.get(keys[i]));
        }
    }

    /**
     * Get every key from the pre-populated buffer, isolating the pure {@code Map.get} lookup cost
     * with no write or resize interference.
     */
    @Benchmark
    @OperationsPerInvocation(1)
    public void getExisting(Blackhole blackhole) {
        for (int i = 0; i < entryCount; i++) {
            blackhole.consume(populatedBuffer.get(keys[i]));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + KvPreWriteBufferBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
