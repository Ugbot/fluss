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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CompiledCodecCache}: key packing, cache hit / miss, and the critical
 * contract that concurrent {@code get(...)} on the same key compiles exactly once.
 */
class CompiledCodecCacheTest {

    private static final RowType DUMMY_ROW = DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT()));

    @Test
    void packAndUnpackKey() {
        // Fluss table ids are int-width in practice; the cache packs them into the top 32 bits.
        long tableId = 0x0000_0000_7FFF_FFFFL;
        int schemaId = 0x1234_5678;
        long key = CompiledCodecCache.packKey(tableId, schemaId);
        assertThat(CompiledCodecCache.tableId(key)).isEqualTo(tableId);
        assertThat(CompiledCodecCache.schemaId(key)).isEqualTo(schemaId);

        // Negative schema id space (unused in Kafka SR but valid 32-bit int) round-trips too.
        long key2 = CompiledCodecCache.packKey(42L, -1);
        assertThat(CompiledCodecCache.tableId(key2)).isEqualTo(42L);
        assertThat(CompiledCodecCache.schemaId(key2)).isEqualTo(-1);
    }

    @Test
    void missThenHit() {
        CompiledCodecCache cache = new CompiledCodecCache();
        long key = CompiledCodecCache.packKey(1L, 42);
        assertThat(cache.contains(key)).isFalse();

        AtomicInteger calls = new AtomicInteger();
        RecordCodec first =
                cache.get(
                        key,
                        () -> {
                            calls.incrementAndGet();
                            return new DummyCodec(DUMMY_ROW, (short) 42);
                        });
        assertThat(first).isNotNull();
        assertThat(cache.contains(key)).isTrue();
        assertThat(cache.size()).isEqualTo(1);

        RecordCodec second =
                cache.get(
                        key,
                        () -> {
                            calls.incrementAndGet();
                            return new DummyCodec(DUMMY_ROW, (short) 99);
                        });
        assertThat(second).isSameAs(first);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void distinctKeysCoexist() {
        CompiledCodecCache cache = new CompiledCodecCache();
        RecordCodec a =
                cache.get(
                        CompiledCodecCache.packKey(1L, 1),
                        () -> new DummyCodec(DUMMY_ROW, (short) 1));
        RecordCodec b =
                cache.get(
                        CompiledCodecCache.packKey(1L, 2),
                        () -> new DummyCodec(DUMMY_ROW, (short) 2));
        RecordCodec c =
                cache.get(
                        CompiledCodecCache.packKey(2L, 1),
                        () -> new DummyCodec(DUMMY_ROW, (short) 3));
        assertThat(a).isNotSameAs(b).isNotSameAs(c);
        assertThat(cache.size()).isEqualTo(3);
    }

    @Test
    void concurrentGetInvokesSupplierExactlyOnce() throws Exception {
        CompiledCodecCache cache = new CompiledCodecCache();
        long key = CompiledCodecCache.packKey(7L, 13);

        int threads = 64;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger supplierInvocations = new AtomicInteger();
            List<Future<RecordCodec>> futures = new ArrayList<>(threads);
            Random rnd = new Random(0xFEED);
            // Seed a slow supplier so losing threads have time to race.
            int delayNanos = 50_000 + rnd.nextInt(50_000);

            for (int i = 0; i < threads; i++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return cache.get(
                                            key,
                                            () -> {
                                                supplierInvocations.incrementAndGet();
                                                parkNanos(delayNanos);
                                                return new DummyCodec(DUMMY_ROW, (short) 1);
                                            });
                                }));
            }
            start.countDown();

            RecordCodec first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<RecordCodec> f : futures) {
                assertThat(f.get(10, TimeUnit.SECONDS)).isSameAs(first);
            }
            assertThat(supplierInvocations.get()).isEqualTo(1);
            assertThat(cache.size()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void nullSupplierResultIsRejected() {
        CompiledCodecCache cache = new CompiledCodecCache();
        long key = CompiledCodecCache.packKey(3L, 3);
        assertThatThrownBy(() -> cache.get(key, () -> null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("null");
    }

    @Test
    void clearResetsState() {
        CompiledCodecCache cache = new CompiledCodecCache();
        cache.get(CompiledCodecCache.packKey(1L, 1), () -> new DummyCodec(DUMMY_ROW, (short) 1));
        assertThat(cache.size()).isEqualTo(1);
        cache.clear();
        assertThat(cache.size()).isEqualTo(0);
    }

    private static void parkNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline) {
            Thread.yield();
        }
    }

    /** Minimal codec stand-in — the cache is codec-shape-agnostic. */
    private static final class DummyCodec implements RecordCodec {
        private final RowType rowType;
        private final short version;

        DummyCodec(RowType rowType, short version) {
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
