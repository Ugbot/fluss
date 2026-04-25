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

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Process-wide cache of compiled {@link RecordCodec}s, keyed by a packed {@code long} composed of
 * {@code (tableId &lt;&lt; 32) | schemaId}.
 *
 * <p>Plan §22.4: codecs are compiled on first sighting of a {@code (tableId, schemaId)} pair and
 * reused for the life of the cluster. The hot path ({@link #get(long, Supplier)}) uses {@link
 * ConcurrentMap#computeIfAbsent} so concurrent lookups for the same key compile exactly once — the
 * suppliers passed by losing threads are never invoked.
 *
 * <p>Keys are packed to keep the hash map small and to avoid allocating composite key objects on
 * every produce / fetch. The top 32 bits are the {@code tableId}; the bottom 32 bits are the {@code
 * schemaId}. A single cache instance is intended per broker.
 *
 * @since 0.9
 */
@Internal
@ThreadSafe
public final class CompiledCodecCache {

    private final ConcurrentMap<Long, RecordCodec> codecs = new ConcurrentHashMap<>();

    /** Pack a {@code (tableId, schemaId)} pair into the cache key space. */
    public static long packKey(long tableId, int schemaId) {
        return (tableId << 32) | (schemaId & 0xFFFFFFFFL);
    }

    /** Extract the {@code tableId} half of a packed key. */
    public static long tableId(long packedKey) {
        return packedKey >> 32;
    }

    /** Extract the {@code schemaId} half of a packed key. */
    public static int schemaId(long packedKey) {
        return (int) (packedKey & 0xFFFFFFFFL);
    }

    /**
     * Fetch an existing codec or compile one via {@code supplier} if none is cached. Concurrent
     * calls with the same {@code packedKey} result in exactly one invocation of {@code supplier};
     * the losing callers receive the winning thread's result.
     *
     * @param packedKey key produced by {@link #packKey(long, int)}
     * @param supplier codec factory; invoked at most once per key
     * @return the cached or freshly compiled codec (never {@code null})
     */
    public RecordCodec get(long packedKey, Supplier<RecordCodec> supplier) {
        RecordCodec hit = codecs.get(packedKey);
        if (hit != null) {
            return hit;
        }
        return codecs.computeIfAbsent(
                packedKey,
                k -> {
                    RecordCodec c = supplier.get();
                    if (c == null) {
                        throw new IllegalStateException(
                                "Codec supplier returned null for packedKey=" + k);
                    }
                    return c;
                });
    }

    /**
     * Convenience overload that packs {@code (tableId, schemaId)} into a key and delegates to
     * {@link #get(long, Supplier)}. T.2 hot-path entry point; mirrors the bolt-on call sites that
     * always know the {@code tableId} / {@code schemaId} pair separately.
     *
     * @param tableId the Fluss table id
     * @param schemaId the Confluent global schema id
     * @param supplier codec factory; invoked at most once per {@code (tableId, schemaId)}
     * @return the cached or freshly compiled codec
     */
    public RecordCodec getOrCompile(long tableId, int schemaId, Supplier<RecordCodec> supplier) {
        return get(packKey(tableId, schemaId), supplier);
    }

    /** Current number of cached codecs. Useful for tests and metrics. */
    public int size() {
        return codecs.size();
    }

    /** Remove all cached codecs. Test-only; a production cache is grow-only. */
    public void clear() {
        codecs.clear();
    }

    /** {@code true} if a codec has already been compiled for {@code packedKey}. */
    public boolean contains(long packedKey) {
        return codecs.containsKey(packedKey);
    }
}
