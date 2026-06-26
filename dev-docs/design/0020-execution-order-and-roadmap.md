<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# 0020 — Execution Order & Roadmap (post-merge)

## Context

The `future` branch consolidates current upstream `apache/fluss` main with the complete
Kafka bolt-on work (Schema Registry, typed-tables hot path, transactions/EOS, `fluss-catalog`,
`fluss-iceberg-rest`, Arrow log format for Kafka topics). From here we push four agendas, in a
deliberate order so each is measurable and low-risk:

1. **Performance** — both throughput and tail latency.
2. **TigerStyle** — assert everything, bound every queue/loop, allocate statically, zero-copy.
3. **Java 25** — hard break from Java 8/11 source level (this fork diverges from upstream).
4. **Native tiering service + dependency diet** — standalone (no-Flink) tiering with all lakes
   behind abstractions (Iceberg mandatory + Delta), Fluss catalog as read-through/write-through
   over the lake, and aggressive removal of heavy deps (Hadoop, Flink-from-tiering, AWS SDK v1).
5. **Mechanical sympathy (LMAX / HFT lens)** — design the hot path for the hardware: single-writer
   per partition, lock-free ring buffers, zero allocation and zero GC on the steady-state path,
   cache-line awareness (no false sharing), thread affinity, and latency measured honestly
   (p99/p999/p9999, no coordinated omission). This lens reinforces TigerStyle and is applied
   *within* the phases below, not as a separate phase — see "LMAX / HFT lens" near the end.

**Existing assets on `future` to build on (do not reinvent):**
- `fluss-catalog` — catalog service (Polaris/Unity-style) with SR/principal/grant/quota/txn
  entities. This is the basis for the read-through/write-through catalog.
- `fluss-iceberg-rest` — Iceberg REST Catalog HTTP server/handler. Basis for exposing Fluss
  tables to Iceberg readers and for the Iceberg write-through path.
- `fluss-lake-{iceberg,paimon,lance,hudi}` — lake impls behind the `LakeTieringFactory` SPI.
- `fluss-kafka` typed hot path + `KafkaFetchCodec` SPI; Arrow log format for Kafka topics.
- `fluss-flink-tiering` — current Flink-based tiering (to be made optional, not deleted yet).

Full audit findings (TigerStyle gaps + perf levers, with file:line references) live in the
agent roadmap; this document is the **ordered execution plan** the team works to.

---

## Strict execution order

Each phase is **benchmark-gated** (no perf change lands without a before/after JMH number) and
**must keep the test suite green**. Do phases in order; later phases assume earlier ones.

### Phase 0 — Land & stabilize `future`  *(in progress)*
- [ ] Full test suite green (unit + ITCase; Kafka Connect/Streams/E2E need a podman cluster).
- [ ] `spotless:apply` clean; RAT clean (already verified).
- [ ] Fix the `./mvnw` wrapper (distribution SHA-256 mismatch on apache-maven-3.8.6).
- [ ] CI runs on `future`; push to fork.
- Exit criteria: `mvn verify` green on changed modules; branch pushed.

### Phase 1 — Measure (benchmark harness + baselines)
"Don't guess, benchmark." Only 3 JMH benchmarks exist today.
- [~] Extend `fluss-jmh`: ADDED so far — `MemorySegmentBenchmark` (FFM baseline),
      `RocksDBWriteBatchBenchmark` (batch-size sweep), `MemoryLogRecordsArrowBuildBenchmark`
      (produce encode), `CrcChecksumBenchmark`, `FileLogProjectionBenchmark`, plus
      `MessageCodec`/`KvPreWriteBuffer`/`BinaryRow` benches in flight. Kafka typed hot path already
      covered by the existing `AvroDecodeBench`/`AvroEncodeBench`/`CodecCacheContentionBench`.
      Still TODO: end-to-end produce/fetch throughput + allocation rate (`-prof gc`).
- [ ] JFR GC-pause capture under sustained produce+fetch.
- [ ] **HFT:** measure latency with **HdrHistogram** (p99/p999/p9999), and **avoid coordinated
      omission** (LatencyUtils / load-generator that records intended vs actual send time). Mean
      and even p99 hide the tail that HFT cares about; report full histograms.
- [ ] Record baselines (Fluss-native AND Kafka paths) into `dev-docs/perf/baseline-*.md`.
- Exit criteria: reproducible baseline numbers committed; nothing tuned yet.

### Phase 2 — Java 25 baseline
- [ ] Flip `target.java.version` to 25; delete the `java8` profile and `compile-on-jdk8` CI job.
- [ ] Drop `--add-exports`/`--add-opens` crutches in root `pom.xml` as strong-encapsulation/FFM
      allow; bump deps (Caffeine 3.x, Arrow, Netty io_uring, ZK/curator).
- [ ] Re-run Phase 1 benchmarks to quantify the free lift.
- Exit criteria: builds/tests on JDK 25; baseline delta recorded. No behaviour change.

### Phase 3 — TigerStyle safety (low risk, improves p99)
- [x] Bound the unbounded server queues + add backpressure/shed: `kv/KvSnapshotResource`
      (bounded + `CallerRunsPolicy`, `kv.snapshot.async-operation.max-pending`) and
      `log/remote/RemoteLogIndexCache` (bounded backstop). `coordinator/event/CoordinatorEventManager`
      is intentionally left unbounded (its single thread re-enqueues; a bounded blocking queue would
      self-deadlock) but now has an `eventQueueSize` gauge + a `coordinator.event-queue.warn-threshold`
      backlog warning.
- [ ] **HFT:** where a bounded queue sits on the request hot path, prefer a **lock-free ring
      buffer (LMAX Disruptor or Agrona `OneToOneRingBuffer`/`ManyToOneRingBuffer`)** over
      `(Array|Linked)BlockingQueue` — single-writer, mechanical-sympathy, configurable wait
      strategy. Start with `CoordinatorEventManager` (single consumer thread already) and the
      `RequestProcessorPool` per-channel queues. Both Disruptor and Agrona are tiny, zero-/few-dep
      libraries — consistent with the dependency diet. (Deferred: needs dependency sign-off.)
- [x] Add assertions/preconditions on hot-path entry points: `log/LogTablet#read` (offset/length
      non-negative, isolation non-null) and `kv/KvTablet#putAsLeader` (records/mergeMode non-null).
      Still TODO: `replica/Replica` and the Kafka typed hot path.
- [x] Convert time-bounded retry loops to explicit iteration bounds (ZK registration in
      TabletServer/CoordinatorServer, `ZOOKEEPER_REGISTER_MAX_ATTEMPTS`). Still TODO: recovery loops.
- Exit criteria: tests green; p99 from Phase 1 harness stable or improved.

### Phase 4 — Allocation & GC
- [x] Audited per-fetch allocations in `record/FileLogProjection`: the per-batch `byte[]` (`:327`) is
      load-bearing (its `BytesView` aliases the array until response serialization, and one project()
      call retains many batches' headers), so it can't be a shared scratch buffer; `:394`'s
      `arrowMetadataBuffer` is already grow-on-demand-reused. Documented; the real win is pooling
      `FileLogProjection` instances per fetch thread (follow-up).
- [x] Default **Generational ZGC** (`-XX:+UseZGC -XX:+ZGenerational`) in `tablet-server.sh`/
      `coordinator-server.sh`/`config.sh`, with derived `-XX:MaxDirectMemorySize` and G1 selectable
      via `env.java.opts.*`. NOTE: takes effect only on JDK 21+ (no JDK 25 installed here yet).
- [ ] **HFT:** drive the steady-state produce/fetch path toward **zero allocation / zero GC** —
      object/buffer pooling on every per-request alloc (extend the existing `MemorySegmentPool`/
      `ArrowWriterPool`), reuse decode scratch. Add `-XX:+AlwaysPreTouch` and pre-size pools.
- [x] **HFT:** eliminated **false sharing** on the single-writer log counters in `log/LocalLog.java`
      via manual cache-line padding (Java-8/11 safe; not `@Contended` which needs --add-exports).
      Still TODO: writer-sequence and any future ring-buffer cursors.
- Exit criteria: benchmark-proven allocation-rate drop and GC-pause improvement.

### Phase 5 — Native tiering service + dependency diet (lakes behind abstractions)
- [ ] Extract `fluss-lake-tiering-core` (engine-agnostic) from the Flink tiering source/committer.
- [ ] Standalone `fluss-tiering-service` daemon over the `LakeTieringFactory`/`LakeWriter`/
      `LakeCommitter` SPI + RPC coordination (`CoordinatorGateway` prepare/commit/heartbeat).
      **All lakes behind abstractions** — no format-specific branching in the core.
- [ ] Iceberg via `iceberg-core` + `iceberg-aws` `S3FileIO` (AWS SDK v2) — **no Hadoop**.
- [ ] Add `fluss-lake-delta` (Delta Kernel write API; no Spark/Hadoop).
- [ ] Wire **read-through/write-through catalog** using `fluss-catalog` + `fluss-iceberg-rest`
      (Iceberg as source of truth for cold; Fluss as hot tier).
- [ ] Hudi: kept; audited like the other lakes; runs through the standalone service.
- [ ] Dependency diet: drop Flink from tiering runtime; native S3/GCS FileSystem to retire
      `fluss-fs-hadoop-shaded` + `hadoop-aws`; migrate `fluss-fs-s3` AWS SDK v1→v2; profile-gate
      `fluss-fs-{oss,azure,obs}` + Scala/Spark; `maven-enforcer` bans on `org.apache.hadoop`/
      `org.apache.flink` in the tiering-core + iceberg path.
- Exit criteria: existing Kafka→Arrow→Iceberg/Paimon tiering ITCases pass against the standalone
      service; a Delta tiering ITCase added; `dependency:tree` shows no Hadoop/Flink in the
      tiering path.

### Phase 6 — Deep perf
- [ ] Migrate `memory/MemorySegment` off `sun.misc.Unsafe` to FFM `MemorySegment`/`Arena`
      (benchmark-gated; keep the Fluss abstraction).
- [ ] Virtual threads on blocking I/O edges (ZK, remote-log/tiering I/O, snapshot upload) —
      not the netty/event path.
- [ ] Vector API (SIMD) on proven-hot codec loops (CRC, Arrow encode/decode, projection,
      Kafka Avro/JSON decode) behind a benchmarked fallback.
- [ ] **HFT:** **single-writer per partition/bucket** on the append path — confirm each log
      tablet is mutated by exactly one thread (sharded executor keyed by bucket) so the hot path
      is lock-free by construction, not by lock.
- [ ] **HFT:** **thread affinity / NUMA** — optional core-pinning for netty event loops and the
      per-bucket writer threads (OpenHFT Affinity or `taskset`/`numactl` at launch), behind a
      config flag; reduces context switches and cross-socket cache traffic.
- [ ] **HFT:** **wait-strategy choice** on the ring buffers — expose blocking / yielding /
      busy-spin (Disruptor `WaitStrategy`) so latency-critical deployments can trade a core for
      sub-microsecond wakeups; default to yielding/blocking for general use.
- [ ] **HFT:** keep hot dispatch **mono-/bi-morphic** — avoid megamorphic call sites on the
      `KafkaFetchCodec`/`LakeWriter`/`Send` interfaces in the inner loop (specialize or cache the
      concrete impl); verify with JIT inlining logs (`-XX:+PrintInlining`).
- Exit criteria: each item benchmark-proven; fallback path retained.

### Phase 7 — Structure
- [ ] Split the 2000+-line hot classes (`CoordinatorEventProcessor`, `Replica`, `ReplicaManager`,
      `LogTablet`) to expose invariants and improve inlining; finish assertion coverage.

---

## LMAX / HFT lens (mechanical sympathy)

Applied *within* the phases above; collected here as the checklist and rationale. The throughline:
**make the steady-state path do no allocation, take no locks, and never surprise the cache or the
GC.** This overlaps heavily with TigerStyle (static allocation, bounded structures, determinism).

- **Single-writer principle.** One thread owns each partition/bucket's mutable state (log end
  offset, HWM, writer state). Contention disappears by design, not by locking. (Phases 3, 6.)
- **Lock-free ring buffers over blocking queues** on hot paths — LMAX Disruptor or Agrona ring
  buffers replace `LinkedBlockingQueue` for the coordinator event loop and request channels.
  Bounded by construction (satisfies the TigerStyle bound-everything rule too). (Phase 3.)
- **Zero allocation / zero GC on the steady path.** Pool every per-request buffer; reuse decode
  scratch; `-XX:+AlwaysPreTouch`; pre-sized pools. The GC you don't trigger can't stall p999.
  (Phase 4.)
- **No false sharing.** Cache-line-pad/`@Contended` the single-writer counters and ring cursors
  that one thread writes and others read. (Phase 4.)
- **Mechanical-sympathy data layout.** Sequential, columnar access (Arrow already helps); avoid
  pointer-chasing in inner loops; FFM `MemorySegment` for predictable, bounds-checked off-heap.
  (Phases 4, 6.)
- **Thread affinity / NUMA + wait strategies.** Optional core-pinning and busy-spin/yield/block
  wait strategies for latency-critical deployments; off by default. (Phase 6.)
- **Honest latency.** HdrHistogram p99/p999/p9999, coordinated-omission-free load generation.
  A perf change is only "good" if the tail improves, not just the mean. (Phase 1, every phase.)
- **JIT discipline.** Warm up before measuring; keep hot interface dispatch mono-/bi-morphic;
  watch `-XX:+PrintInlining`. (Phases 1, 6.)
- **Small, targeted deps only.** LMAX Disruptor and Agrona are tiny, zero-/few-transitive-dep
  libraries — adding them is consistent with the dependency diet, unlike the heavy stack we're
  removing. Prefer them over hand-rolled lock-free code we'd have to maintain.

## Dependency-removal targets (tracked across Phases 2 & 5)
| Target | Action | Phase |
|--------|--------|-------|
| Hadoop (`hadoop-common`/`hadoop-aws`, ~300MB shaded) | Iceberg `S3FileIO` + native FS clients | 5 |
| Flink (as tiering runtime) | standalone `fluss-tiering-service` | 5 |
| AWS SDK v1 (EOL) | migrate to v2 | 5 |
| Scala/Spark, `fluss-fs-{oss,azure,obs}` | profile-gate out of default build | 5 |
| Caffeine 2.9.3 (Java 8 pin) | 3.x | 2 |
| `sun.misc.Unsafe` | FFM API | 6 |

## Verification (every phase)
- Add/extend a JMH benchmark in `fluss-jmh` (never ad-hoc); compare throughput, p99/p999,
  allocation rate before/after.
- Run with `-ea`; keep all `*Test`/`*ITCase` green. Build with Java 11 until Phase 2, then 25.
- `mvn spotless:check` + RAT + `maven-enforcer` clean.
