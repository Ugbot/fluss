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

package org.apache.fluss.jmh.kafka;

import org.apache.fluss.kafka.sr.typed.CompiledCodecCache;
import org.apache.fluss.kafka.sr.typed.RecordCodec;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Stresses {@link CompiledCodecCache#get} under 64-thread contention against 1000 distinct schema
 * ids, to confirm the {@code ConcurrentHashMap.computeIfAbsent} hot path does not regress into lock
 * contention once all entries are warmed.
 *
 * <p><b>Gate (plan §25.4):</b> throughput per thread MUST NOT collapse under contention. Review by
 * comparing {@code ops/us/thread} at {@code threads=64} against {@code threads=1} — the ratio
 * should stay above ~0.5 for a read-dominant workload with 1k keys.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@Threads(64)
@State(Scope.Benchmark)
public class CodecCacheContentionBench {

    private static final int NUM_SCHEMAS = 1_000;
    private static final long TABLE_ID = 42L;

    private CompiledCodecCache cache;
    private NoopCodec[] codecs;

    @Setup
    public void setUp() {
        cache = new CompiledCodecCache();
        codecs = new NoopCodec[NUM_SCHEMAS];
        RowType rowType = DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT()));
        // Pre-warm: every schema id has a codec cached. This is the steady-state case —
        // the bench measures lookup latency, not compilation.
        for (int i = 0; i < NUM_SCHEMAS; i++) {
            codecs[i] = new NoopCodec(rowType, (short) i);
            final NoopCodec fixed = codecs[i];
            long key = CompiledCodecCache.packKey(TABLE_ID, i);
            cache.get(key, () -> fixed);
        }
    }

    @Benchmark
    public RecordCodec warmGet() {
        int schemaId = ThreadLocalRandom.current().nextInt(NUM_SCHEMAS);
        long key = CompiledCodecCache.packKey(TABLE_ID, schemaId);
        return cache.get(
                key,
                () -> {
                    // Should never fire once the cache is warm; if it does, the map lost an entry.
                    throw new IllegalStateException("cache miss for warmed key");
                });
    }

    /** Zero-work codec — contention bench isolates the map, not the decode path. */
    private static final class NoopCodec implements RecordCodec {
        private final RowType rowType;
        private final short version;

        NoopCodec(RowType rowType, short version) {
            this.rowType = rowType;
            this.version = version;
        }

        @Override
        public int decodeInto(ByteBuf src, int offset, int length, IndexedRowWriter dst) {
            return 0;
        }

        @Override
        public int encodeInto(BinaryRow src, ByteBuf dst) {
            return 0;
        }

        @Override
        public short schemaVersion() {
            return version;
        }

        @Override
        public RowType rowType() {
            return rowType;
        }
    }
}
