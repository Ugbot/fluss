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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Benchmark for RocksDB {@link WriteBatch} flush latency and throughput across a sweep of batch
 * sizes (number of key-value pairs accumulated before the batch is written to RocksDB).
 *
 * <p>This directly informs the hard-coded {@code capacity = 500} flush trigger in {@code
 * org.apache.fluss.server.kv.rocksdb.RocksDBWriteBatchWrapper}. That value was inherited from Flink
 * with the rationale "hundreds of keys"; this benchmark measures how flush cost scales with the
 * accumulated batch size so the choice can be revisited with data rather than folklore.
 *
 * <p>The benchmark mirrors the wrapper's hot path: accumulate {@code batchSize} {@code put}
 * operations into a single {@link WriteBatch}, then {@link RocksDB#write(WriteOptions, WriteBatch)}
 * it with WAL disabled (exactly as {@code RocksDBWriteBatchWrapper.flush()} does), then clear the
 * batch for reuse.
 *
 * <p>Two views are reported:
 *
 * <ul>
 *   <li>{@link #flushBatch()} — {@link Mode#AverageTime} latency of building+flushing one whole
 *       batch (lower is better; shows fixed per-flush overhead amortised over the batch).
 *   <li>{@link #flushBatchThroughput()} — {@link Mode#Throughput} of whole-batch flush operations
 *       (ops/s; multiply by {@code batchSize} for record throughput).
 * </ul>
 *
 * <p>Payloads are randomized with a deterministic seed so runs are comparable. Key/value sizes are
 * chosen to be representative of small KV upsert records.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
public class RocksDBWriteBatchBenchmark {

    /** Deterministic seed so payloads are identical across runs. */
    private static final long RANDOM_SEED = 0xF1755L;

    private static final int KEY_BYTES = 16;
    private static final int VALUE_BYTES = 100;

    /**
     * Sweep of batch sizes (number of key-value pairs per flush). The wrapper currently hard-codes
     * 500; the surrounding points show how flush cost behaves above and below that choice.
     */
    @Param({"100", "500", "2000", "8000"})
    public int batchSize;

    private RocksDB db;
    private WriteOptions writeOptions;
    private WriteBatch batch;
    private Path dbDir;

    /**
     * Pre-generated, deterministic key/value payloads; each key's prefix is stamped per invocation.
     */
    private byte[][] keys;

    private byte[][] values;

    /** Monotonic per-invocation counter, written into each key prefix so writes are distinct. */
    private long invocation;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        NativeLibraryLoader.getInstance().loadLibrary(null);

        // Deterministically pre-generate payloads so the put() loop measures RocksDB, not RNG.
        Random random = new Random(RANDOM_SEED);
        keys = new byte[batchSize][];
        values = new byte[batchSize][];
        for (int i = 0; i < batchSize; i++) {
            byte[] key = new byte[KEY_BYTES];
            byte[] value = new byte[VALUE_BYTES];
            random.nextBytes(key);
            random.nextBytes(value);
            keys[i] = key;
            values[i] = value;
        }
    }

    // A FRESH DB per iteration bounds on-disk growth (each invocation writes DISTINCT keys, the
    // realistic insert workload), and auto-compactions are DISABLED so the measurement is
    // stationary
    // within the iteration instead of drifting as background compaction kicks in.
    @Setup(Level.Iteration)
    public void setupIteration() throws Exception {
        dbDir = Files.createTempDirectory("fluss-jmh-rocksdb-writebatch-");
        // WAL is disabled on writes (see WriteOptions) to match RocksDBWriteBatchWrapper.
        org.rocksdb.Options options =
                new org.rocksdb.Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
        db = RocksDB.open(options, dbDir.toString());
        options.close();
        writeOptions = new WriteOptions().setDisableWAL(true);
        // Mirror the wrapper's initial reservation: capacity * PER_RECORD_BYTES (100).
        batch = new WriteBatch(batchSize * 100);
        invocation = 0;
    }

    @TearDown(Level.Iteration)
    public void teardownIteration() throws Exception {
        if (batch != null) {
            batch.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (db != null) {
            db.close();
        }
        if (dbDir != null) {
            deleteRecursively(dbDir);
        }
    }

    /**
     * Stamps the current invocation counter into the first 8 bytes of every key (and the index into
     * bytes 8-9) so each flushed batch inserts DISTINCT keys rather than repeatedly overwriting a
     * fixed key set.
     */
    private void stampKeys() {
        long n = invocation++;
        for (int i = 0; i < batchSize; i++) {
            byte[] k = keys[i];
            k[0] = (byte) (n >>> 56);
            k[1] = (byte) (n >>> 48);
            k[2] = (byte) (n >>> 40);
            k[3] = (byte) (n >>> 32);
            k[4] = (byte) (n >>> 24);
            k[5] = (byte) (n >>> 16);
            k[6] = (byte) (n >>> 8);
            k[7] = (byte) n;
            k[8] = (byte) (i >>> 8);
            k[9] = (byte) i;
        }
    }

    /**
     * Builds a full batch of {@code batchSize} puts and flushes it, mirroring {@code
     * RocksDBWriteBatchWrapper.flushIfNeeded()} -&gt; {@code flush()}. Reported as average time per
     * whole-batch flush.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void flushBatch() throws RocksDBException {
        stampKeys();
        batch.clear();
        for (int i = 0; i < batchSize; i++) {
            batch.put(keys[i], values[i]);
        }
        db.write(writeOptions, batch);
    }

    /** Same workload as {@link #flushBatch()} but reported as whole-batch flush throughput. */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void flushBatchThroughput() throws RocksDBException {
        stampKeys();
        batch.clear();
        for (int i = 0; i < batchSize; i++) {
            batch.put(keys[i], values[i]);
        }
        db.write(writeOptions, batch);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Failed to delete temp RocksDB file: " + path, e);
                                }
                            });
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + RocksDBWriteBatchBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }
}
