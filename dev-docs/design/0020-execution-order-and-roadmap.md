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
- [ ] Extend `fluss-jmh`: end-to-end produce throughput (batch-size sweep), fetch throughput
      (±projection), **Kafka typed hot path** (`KafkaFetchCodec`, Avro/JSON decode), RocksDB
      `WriteBatch` flush latency vs batch size, allocation rate per request (`-prof gc`).
- [ ] JFR GC-pause capture under sustained produce+fetch.
- [ ] Record baselines (Fluss-native AND Kafka paths) into `dev-docs/perf/baseline-*.md`.
- Exit criteria: reproducible baseline numbers committed; nothing tuned yet.

### Phase 2 — Java 25 baseline
- [ ] Flip `target.java.version` to 25; delete the `java8` profile and `compile-on-jdk8` CI job.
- [ ] Drop `--add-exports`/`--add-opens` crutches in root `pom.xml` as strong-encapsulation/FFM
      allow; bump deps (Caffeine 3.x, Arrow, Netty io_uring, ZK/curator).
- [ ] Re-run Phase 1 benchmarks to quantify the free lift.
- Exit criteria: builds/tests on JDK 25; baseline delta recorded. No behaviour change.

### Phase 3 — TigerStyle safety (low risk, improves p99)
- [ ] Bound the unbounded server queues + add backpressure/shed:
      `coordinator/event/CoordinatorEventManager`, `kv/KvSnapshotResource`,
      `log/remote/RemoteLogIndexCache`; capacities as `ConfigOption`s.
- [ ] Add assertions/preconditions on hot-path entry points: `replica/Replica`,
      `log/LogTablet#read`, `kv/KvTablet#putAsLeader` (and the Kafka typed hot path).
- [ ] Convert time-bounded retry loops to explicit iteration bounds (ZK registration, recovery).
- Exit criteria: tests green; p99 from Phase 1 harness stable or improved.

### Phase 4 — Allocation & GC
- [ ] Remove per-fetch allocations in `record/FileLogProjection` (`:327`, `:394`) via reusable
      scratch buffers; audit `MemoryLogRecords` copies.
- [ ] Evaluate + default **Generational ZGC** (`tablet-server.sh`/`config.sh`); set explicit
      `-XX:MaxDirectMemorySize`. Keep G1 selectable.
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
- Exit criteria: each item benchmark-proven; fallback path retained.

### Phase 7 — Structure
- [ ] Split the 2000+-line hot classes (`CoordinatorEventProcessor`, `Replica`, `ReplicaManager`,
      `LogTablet`) to expose invariants and improve inlining; finish assertion coverage.

---

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
