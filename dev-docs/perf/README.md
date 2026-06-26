# Fluss Performance Benchmark Harness

JMH microbenchmarks live in `fluss-jmh`. This is the measurement substrate for the 0020 roadmap —
**every perf change must show a before/after number from here** ("don't guess, benchmark"), and the
results gate Phases 2/4/6.

## Inventory

| Benchmark | Module path | Measures | Roadmap tie-in |
|-----------|-------------|----------|----------------|
| `MemorySegmentBenchmark` | jmh test | MemorySegment accessor hot path (heap/off-heap, seq int/long, bulk copy) | **Phase 6** FFM-migration baseline |
| `BinaryRowAccessBenchmark` | jmh test | BinaryRow / BinarySegmentUtils field access (MemorySegment-backed) | Phase 6 (FFM scope) |
| `RocksDBWriteBatchBenchmark` | jmh test | RocksDB `WriteBatch` flush latency/throughput vs batch size (100/500/2000/8000) | Phase 4; informs the hardcoded-500 in `RocksDBWriteBatchWrapper` |
| `MemoryLogRecordsArrowBuildBenchmark` | jmh test | Arrow log-records produce encode path | Phase 1 produce |
| `FileLogProjectionBenchmark` | jmh test | `FileLogProjection.project()` across column-subset sizes | Phase 4 fetch/projection |
| `CrcChecksumBenchmark` | jmh test | record-batch CRC32C (`Crc32C` intrinsic vs `PureJavaCrc32C`) | Phase 6 (Vector API candidate) |
| `MessageCodecBenchmark` | jmh test | RPC wire encode (`MessageCodec`, pooled ByteBuf) | Phase 1 network |
| `KvPreWriteBufferBenchmark` | jmh test | in-memory KV pre-write buffer put/get (per-put HashMap lookup) | Phase 3/4 |
| `ArrowReadable/WritableChannelBenchmark` | jmh test | Arrow IPC channel read/write (pre-existing) | Phase 1 |
| `LogScannerBenchmark` | jmh test | log scan (pre-existing) | Phase 1 |
| `kafka/AvroDecodeBench`, `AvroEncodeBench`, `CodecCacheContentionBench` | jmh main | Kafka typed hot path (Avro decode/encode + codec cache) | Phase 1 Kafka |

## Running

Build target is **JDK 11** today (`./mvnw` wrapper is currently broken on a distribution SHA mismatch —
use system `mvn`). Set `JAVA_HOME` to a JDK 11:

```bash
JAVA_HOME=<jdk11> mvn -q test-compile -pl fluss-jmh -Drat.skip=true -Dspotless.check.skip=true
# run one benchmark's main():
JAVA_HOME=<jdk11> mvn -q exec:java -pl fluss-jmh \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=org.apache.fluss.jmh.MemorySegmentBenchmark
```

(Each benchmark has a `main()` using `OptionsBuilder`/`Runner`, so it can also be run directly from an
IDE or a built `fluss-jmh` jar.)

## Capturing baselines (Phase 0/1 exit criteria)

Baselines MUST be captured on a **quiet machine** — a JMH run on a CPU-saturated host produces
meaningless numbers. For each baseline:

1. **Throughput AND tail latency.** Don't report only the mean. For latency-sensitive paths, layer
   **HdrHistogram** p99/p999/**p9999** and use a **coordinated-omission-free** load generator
   (record intended vs actual send time) — mean and even p99 hide the tail HFT cares about.
2. **Allocation rate** via `-prof gc` (`...Runner` → add `-prof gc`, or run the jar with
   `-prof gc`). Phase 4's goal is ~0 alloc/op on the steady-state produce/fetch path.
3. **GC pauses** via JFR (`-XX:+FlightRecorder -XX:StartFlightRecording=...`) under sustained
   produce+fetch, compared across GC choices (G1 vs Generational ZGC — Phase 4).
4. Record results under `dev-docs/perf/baseline-<date>-<jdk>-<gc>.md` with the exact JDK build, GC
   flags, hardware, and the full JMH output table (not a summary).

## Re-run gates (per roadmap phase)

- **Phase 2 (Java 25):** re-run the whole suite on JDK 25 to quantify the free lift.
- **Phase 4 (alloc/GC):** `RocksDBWriteBatchBenchmark`, `FileLogProjectionBenchmark`,
  `MemoryLogRecordsArrowBuildBenchmark` with `-prof gc` before/after; GC-pause JFR before/after ZGC.
- **Phase 6 (FFM):** `MemorySegmentBenchmark` + `BinaryRowAccessBenchmark` must NOT regress vs the
  Unsafe baseline; `CrcChecksumBenchmark` gates any Vector-API CRC work.
