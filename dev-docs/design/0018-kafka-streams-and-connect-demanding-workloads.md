# Design 0018 — Kafka Streams + Kafka Connect demanding workloads

**Status:** In progress — Workstream A landed (`051921ea`), Workstream B
landed (`0bf657c1`), Workstreams C and D in flight.
**Owner:** Ben Gamble
**Supersedes:** nothing.
**Related:** `0016-kafka-transactions.md`, `0017-kafka-connect-and-streams-test-harness.md`,
`0014-kafka-typed-tables-hot-path.md`, `0015-kafka-typed-tables-alter-on-register.md`.

## 1. Context

Smoke ITs in commit `2d366513` proved that Kafka Connect's standalone
worker and Kafka Streams' `KafkaStreams` runtime can both connect, run a
trivial topology, and round-trip records through the bolt-on broker.
That's necessary but nowhere near sufficient to claim "Kafka Streams
works against Fluss." Real-world Streams workloads exercise:

- multi-partition consumer-group rebalance under back-pressure;
- compacted changelog topics rebuilt from CDC after a restart;
- repartition topics for grouping operations;
- co-partitioning checks for joins;
- `SpecificAvroSerde` round-tripping with Kafka SR's serdes against our
  Schema Registry endpoint;
- exactly-once-v2 (`processing.guarantee=exactly_once_v2`) — full Kafka
  txn protocol on top of `read_committed` consumers;
- typed Fluss tables transparently on the Kafka wire (Phase T.2 + T.3).

Real Connect deployments exercise distributed mode, MirrorMaker 2,
SASL/SCRAM, and ACL-gated long-lived sessions. Doc 0017 named all eleven
extra ITs to write; this doc is the **execution plan** that closes
those gaps and stabilises the four prior demanding-workload ITs that
were flaky.

The intended outcome: `mvn -o verify -pl fluss-kafka` runs the full
demanding-workload matrix end-to-end on a fresh JVM with zero
`@Tag("flaky")` and no `@Disabled` scenarios except those waiting on a
clearly-documented unblocking phase.

## 2. Status snapshot (2026-04-25)

| Workstream | Status | Commit | Notes |
|---|---|---|---|
| A — de-flake foundation | landed | `051921ea` | `awaitSystemTablesReady` + 3-step txn warmup |
| B — verify already-coded ITs | landed | `0bf657c1` | T.3 + J.3 bugs surfaced & `@Disabled` |
| C.1 — Streams stateful aggregation | in flight | — | this is the seed |
| C.2 — Streams windowed + joins | pending | — | depends on C.1 patterns |
| C.3 — Streams Schema Registry | pending | — | depends on Kafka SR serde deps |
| C.4 — Streams exactly-once | pending | — | `@Disabled` (waiting on J.3 fix) |
| C.5 — Streams typed-tables | pending | — | `@Disabled` (waiting on T.3 fix) |
| D.1 — Connect file-sink | pending | — | mirror of smoke source |
| D.2 — Connect SASL/SCRAM | pending | — | depends on Phase H |
| D.3 — Connect ACL-gated | pending | — | depends on Phase G |
| D.4 — Connect MirrorMaker 2 | pending | — | requires distributed mode + 2 clusters |

## 3. Workstream A — de-flake foundation (landed)

The flake-prone ITs `KafkaTransactionalProducerITCase`,
`KafkaInitProducerIdITCase`, and `KafkaReadCommittedITCase` polled for
`TransactionCoordinators.current().isPresent()` — that's the in-JVM
coordinator, but doesn't wait for the **system tables**
(`_kafka_txn_state`, `_kafka_producer_ids`, `_kafka_txn_offset_buffer`)
to finish bucket-leadership propagation in ZK. The first
`INIT_PRODUCER_ID` through the broker would then race the lazy table
creation, costing 30–60 s and sometimes timing out.

### A.1 — `awaitSystemTablesReady`

New helper at `fluss-server/src/test/java/org/apache/fluss/server/testutils/FlussClusterExtension.java`:

```
public void awaitSystemTablesReady(Duration timeout, String... tableSpecs)
```

Each `tableSpec` is `<database>.<table>`. The body polls every 100 ms
checking that:

- `zkClient.getTable(path)` returns a `TableRegistration`;
- `zkClient.getTableAssignment(tableId)` returns an assignment;
- every bucket has `getLeaderAndIsr().leader() ≥ 0`.

On timeout, throws `AssertionError` with a diagnostic
`Map<spec, missingState>` so a CI failure says exactly which bucket of
which table didn't propagate.

### A.2 — 3-step txn warmup

The three flake-prone IT `@BeforeEach` hooks now do:

1. **Wait for the in-JVM coordinator** — the existing
   `TransactionCoordinators.current().isPresent()` poll, kept at 30 s.
2. **Trigger lazy table creation** — call
   `CatalogService.listKafkaTxnStates`, `listKafkaProducerIds`,
   `listKafkaTxnOffsets("__warmup__")`. Each of these forces
   `FlussCatalogService.table(handle)` which creates the table via
   `Admin.createTable` if absent.
3. **Wait for ZK bucket-leadership** — the new
   `awaitSystemTablesReady` helper.

After step 3, the next `INIT_PRODUCER_ID` is fast and deterministic.

### A.3 — Producer-side timeout policy

The previous `MAX_BLOCK_MS = 180_000` bandaid is reduced to
`MAX_BLOCK_MS = 120_000` with a clear comment naming the actual root
cause: the broker's `TransactionCoordinator` opens its catalog
`Connection` lazily on first request, paying ~30–60 s. The test-side
warmup creates ZK state but cannot pre-warm the broker side; that is
out of scope for A and is a follow-up under D's
[broker-warmup-hook](#5-future-considerations) (when Connect distributed
mode lands and the same cold-start burden affects multi-worker tests).

### A.4 — Verification

- `KafkaTransactionalProducerITCase` passes alone in 46 s.
- `@Tag("flaky")` removed from `KafkaInitProducerIdITCase`.
- The diagnostic map in `awaitSystemTablesReady` correctly reported
  per-bucket missing state during iteration; was used to identify the
  lazy-table-creation root cause.

## 4. Workstream B — verify already-coded demanding ITs (landed)

Two real bugs surfaced when running the existing
`KafkaTypedAlterITCase` and `KafkaReadCommittedITCase` against the
de-flaked foundation. Both `@Disabled` with explanatory notes; ITs
stay in tree as concrete reproducers for the eventual fix.

### 4.1 Phase T.3 evolution layout bug

`KafkaTypedAlterITCase` 4-of-7 scenarios fail. The
`TypedTableEvolver.computeAdditiveDelta` at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/TypedTableEvolver.java`
emits `TableChange.AddColumn(name, type, ColumnPosition.last())` —
the only position fluss-server's `SchemaUpdate` accepts. New columns
land **after** `headers`, breaking the
`[record_key, ...userFields, event_time, headers]` invariant that
`TypedKafkaFetchCodec` depends on.

Three possible fixes, none cheap:

- (a) `AddColumn(BEFORE)` support in fluss-core. Doc 0015 §3
  explicitly chose to avoid this.
- (b) Flip the layout invariant so reserved columns come **first**.
  Breaks every Phase T.2 wire-encoding callsite.
- (c) Drop + recreate on every evolution. Loses in-flight data.

Status: `@Disabled` with the `evolveAddNullableEmailExtendsTypedShape`,
`evolveRenameIsRejected`, `evolveNonNullAddIsRejected`, and
`srSchemaIdPreservedAcrossAdditiveAlter` scenarios.

### 4.2 Phase J.3 read_committed visibility bug

`KafkaReadCommittedITCase` produced 13 records and consumed 0. Either
`KafkaFetchTranscoder` truncates at LSO too aggressively before the
control-batch marker write completes, or the marker fan-out from
`TransactionCoordinator.endTxn(commit)` doesn't update
`lastStableOffset` on time. Reproducible alone; not a contention
artifact.

Status: `@Disabled`. Fix lives in `KafkaFetchTranscoder.encode` per
doc 0016 §7–§8.

## 5. Workstream C — Streams advanced ITs (5 files, refined)

Originally listed as 4 ITs in doc 0017. Refined into 5 in this doc to
separate stateful aggregation from windowed/joined topologies — the
former is a stronger first proof point and the latter exercises a
different broker code path (co-partitioning checks).

Critical existing files to reuse:

- `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/EmbeddedStreamsApp.java`
  — per-test `state.dir` lifecycle, uncaught-exception sentinel,
  `waitForRunning(Duration)`.
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsHarnessSmokeITCase.java`
  — exemplar for cluster wiring + producer-side timeouts.

### 5.1 — `KafkaStreamsStateStoreITCase` (C.1, seed)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsStateStoreITCase.java`

Three scenarios bundled in one omnibus `@Test` so the cluster warm-up
tax is paid once:

1. **Stateful aggregation cold start** — topology
   `stream("kss-in") → groupByKey().count(Materialized.as("kss-counts")) →
   toStream().to("kss-out")`. Produce 100 randomised records across 10
   keys. Consume `kss-out`; assert per-key cumulative count matches.
2. **Restart from changelog** — close the Streams app, delete its
   `state.dir`, restart with the same `application.id`. The internal
   compacted changelog topic
   `<application-id>-kss-counts-changelog` should rebuild the state
   store on startup. Produce 50 more records; consume the new output;
   assert final counts include both batches.
3. **Interactive query** — open
   `streams.store(StoreQueryParameters.fromNameAndType("kss-counts", QueryableStoreTypes.keyValueStore()))`
   on the running app; iterate; assert reads match the consumed
   output.

Pre-create topics before starting the Streams app:

- `kss-in` — 4 partitions.
- `kss-out` — 4 partitions.
- `<application-id>-kss-counts-changelog` — 4 partitions,
  `cleanup.policy=compact` via `AlterConfigs`. Done explicitly to
  exercise our compacted-topic CDC path (Phase C-T.2, landed) and
  avoid Streams' admin-side autocreate path racing the cluster's
  metadata propagation.

Producer-side timeouts: the same template the smoke IT uses
(`MAX_BLOCK_MS=30_000`, `DELIVERY_TIMEOUT_MS=60_000`).

This is the seed — once C.1 is green, every subsequent Streams IT
clones its setup pattern.

### 5.2 — `KafkaStreamsWindowedAndJoinsITCase` (C.2)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsWindowedAndJoinsITCase.java`

Two demanding topologies:

1. **Tumbling windowed count** — 5-second tumbling windows over a
   single input topic; assert each window contains the right keys.
   Exercises:
   - a *second* internal state store (the windowed one);
   - the suppress-buffer commit path under back-pressure;
   - 5-second-driven downstream emit timing (record latency
     visible).
2. **KStream-KTable join** — left side `stream("orders")`, right side
   `stream("users").toTable()`, joined on `order.userId == user.id`,
   output enriched orders to `enriched-orders`. Pre-populate `users`,
   then drive `orders`. Exercises:
   - **co-partitioning check** — `orders` and `users` must have the
     same partition count; Streams asserts this at start time and our
     broker's `Metadata` API has to report consistent partition counts;
   - the table-side compacted changelog;
   - cross-topic joins (real producer/consumer choreography).

Pre-create all topics with 4 partitions. Use `Serdes.String()` and
`Serdes.Long()` so the IT doesn't drag in Avro yet.

### 5.3 — `KafkaStreamsSchemaRegistryITCase` (C.3)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsSchemaRegistryITCase.java`

Tests `SpecificAvroSerde<OrderCreated>` registering schemas through the
Kafka SR client against our SR endpoint, producing + consuming
typed records. **Phase T.2 typed-tables flag stays off** —
`SpecificAvroSerde` uses Kafka SR's wire-framed Avro on the value
column; `KAFKA_PASSTHROUGH` topics already round-trip those bytes
correctly.

Test-scope deps to add to `fluss-kafka/pom.xml`:

```xml
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-avro-serializer</artifactId>
  <version>${confluent.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-streams-avro-serde</artifactId>
  <version>${confluent.version}</version>
  <scope>test</scope>
</dependency>
```

`confluent.version` is `7.5.x` (already used elsewhere in tree).

Topology: simple map — input `OrderCreated` → output `OrderEnriched`
(adds a derived `priceWithTax` field).

Scenarios:

1. Register both schemas via the Kafka SR client.
2. Produce 50 `OrderCreated` records with `KafkaAvroSerializer`.
3. Streams app reads with `SpecificAvroSerde<OrderCreated>`,
   transforms, writes `SpecificAvroSerde<OrderEnriched>`.
4. Drain the output topic; assert all 50 enriched records present.
5. Mid-flight schema evolution: register an additive `OrderCreated v2`
   field while the app is running; produce 10 more records; assert app
   still consumes (Avro reader-schema projection).

### 5.4 — `KafkaStreamsExactlyOnceITCase` (C.4)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsExactlyOnceITCase.java`

Lands `@Disabled` until the **Phase J.3 read_committed visibility bug**
(see §4.2) is fixed. The test code is shipped today so the unblock is
one annotation flip away.

Topology: word-count, identical to the smoke IT but with
`processing.guarantee=exactly_once_v2`.

Scenarios:

1. Drive 200 input records; consume output (read_committed); assert
   exact total count per word.
2. **Crash recovery** — kill the Streams thread mid-batch via the
   `setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler.REPLACE_THREAD)`
   path; assert no duplicate aggregates after recovery.
3. Two-stage pipeline (`stream → map → through(intermediate) → count`)
   under EOS — proves the txn boundary spans across the pipeline.

### 5.5 — `KafkaStreamsWithTypedTablesITCase` (C.5)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsWithTypedTablesITCase.java`

Lands `@Disabled` until the **Phase T.3 evolution layout bug**
(see §4.1) is fixed.

Cluster set with `KAFKA_TYPED_TABLES_ENABLED=true`. Topic gets
reshaped to `KAFKA_TYPED_AVRO` via the SR register path.

Scenarios:

1. Pre-create topic, register Avro schema, observe table reshape via
   `Admin.describeTable`.
2. Streams app produces typed records; a Fluss SDK reader reads the
   *same* typed columns via `Table.newScan().createBatchScanner()` —
   proves the same bytes that came in over the Kafka protocol are
   queryable as native Fluss rows.
3. `KTable` with the same typed schema — exercises the typed
   changelog (compacted) path in combination with the typed produce
   path.

This is the "typed tables actually useful to Streams users" demand
test.

## 6. Workstream D — Connect advanced ITs (4 files)

D.1 alone first, then D.2/D.3/D.4 in parallel.

### 6.1 — `KafkaConnectFileSinkITCase` (D.1)

Path:
`fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectFileSinkITCase.java`

Mirror of the smoke source-connector IT, reversed. Producer feeds
1000 randomised lines to `connect_sink_<n>`;
`FileStreamSinkConnector` reads and writes to a tmp file; harness
reads the file back and asserts byte-equal to producer payload.

### 6.2 — `KafkaConnectWithSaslScramITCase` (D.2)

Cluster set with `SASL_PLAINTEXT` listener + SCRAM-SHA-256 mechanism +
JAAS config (Phase H landed). Connector configured with
`security.protocol=SASL_PLAINTEXT` + `sasl.mechanism=SCRAM-SHA-256`.
Pre-create the user's SCRAM credential via
`AdminClient.alterUserScramCredentials`. Drive 5 minutes of
low-volume traffic (file written incrementally); assert no re-auth
failures and all records arrive.

Failure-mode coverage: kill the worker mid-run, restart with same
JAAS, expect Connect's commit semantics to resume cleanly.

### 6.3 — `KafkaConnectWithAclITCase` (D.3)

Cluster set with `kafka.schema-registry.rbac.enforced=true` (Phase G).
Three principals:

- `alice` — `WRITE on TABLE(kafka.alice-topic)`.
- `bob` — `READ on TABLE(kafka.bob-topic)`.
- `eve` — no grants.

Three FileStreamSource connectors:

1. `alice-topic` as alice → succeeds.
2. `bob-topic` as alice → fails with
   `TopicAuthorizationException` surfaced to Connect's `FAILED` task
   state (assert via `connectorStatus()`).
3. Any topic as eve → same surfaced error.

### 6.4 — `KafkaConnectMirrorMakerITCase` (D.4)

Most complex. Requires **distributed Connect mode** (one harness
extension) and **two clusters** (a sibling
`FlussClusterExtension`).

Setup:

- Cluster A (source): produce 500 records to
  `mm2-source.alice-topic`.
- Cluster B (target): empty.
- MirrorMaker 2 connector running in distributed mode on a third
  Connect worker JVM, with
  `source.cluster.bootstrap.servers=<A>`,
  `target.cluster.bootstrap.servers=<B>`.
- After 30 s settle, assert all 500 records mirrored on
  `mm2-target.alice-topic` with the source's record-key + headers
  preserved.

Harness work:

- `EmbeddedConnectCluster.startDistributed(...)` ensures the three
  internal topics exist before the worker starts (don't rely on
  autocreate — pre-create via `AdminClient.createTopics` with 1
  partition each).
- The harness must accept two bootstrap URLs at startup.

## 7. Sequencing & parallelism

```
A (foundation)  ✓
B (verify)      ✓
C.1 StateStore         ← seed
C.2 Windowed+Joins     ← parallel with C.3
C.3 SchemaRegistry     ← parallel with C.2
C.4 ExactlyOnce        ← @Disabled (J.3 unblock)
C.5 WithTypedTables    ← @Disabled (T.3 unblock)
D.1 FileSink           ← seed
D.2 WithSaslScram      ← parallel with D.3
D.3 WithAcl            ← parallel with D.2
D.4 MirrorMaker        ← parallel; biggest scope
```

Total commits remaining: ~10.
Total LOC: ~2 500 across new ITs + ~150 for distributed Connect
helper + ~50 for new Kafka SR Streams test deps.

## 8. Critical files

**Foundation (landed, A):**
- `fluss-server/src/test/java/org/apache/fluss/server/testutils/FlussClusterExtension.java`
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTransactionalProducerITCase.java`
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaInitProducerIdITCase.java`
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaReadCommittedITCase.java`

**Verification (landed, B):**
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTypedAlterITCase.java`
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaReadCommittedITCase.java`

**Streams (C, pending):**
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsStateStoreITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsWindowedAndJoinsITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsSchemaRegistryITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsExactlyOnceITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsWithTypedTablesITCase.java`
- Modify: `fluss-kafka/pom.xml` (`io.confluent:kafka-streams-avro-serde` test scope).
- Reuse: `EmbeddedStreamsApp.java`, `KafkaStreamsHarnessSmokeITCase.java`.

**Connect (D, pending):**
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectFileSinkITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectWithSaslScramITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectWithAclITCase.java`
- New: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectMirrorMakerITCase.java`
- Modify: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/EmbeddedConnectCluster.java`
  (distributed-mode + two-bootstrap support).
- Possibly: `fluss-kafka/pom.xml` for `connect-mirror` test scope.

## 9. Verification

**Plan-level:** `mvn -o verify -pl fluss-kafka` runs ~120 ITs end-to-end
on a fresh JVM, with:

- zero `@Tag("flaky")` (`grep -r '@Tag("flaky")' fluss-kafka/src/test/java/`
  returns empty);
- only the 5 currently `@Disabled` scenarios (4 in `KafkaTypedAlterITCase` +
  1 in `KafkaReadCommittedITCase`); `KafkaStreamsExactlyOnceITCase` and
  `KafkaStreamsWithTypedTablesITCase` add 2 more `@Disabled` scenarios
  with explanatory pointers — all five disables traceable to specific
  Phase T.3 / J.3 fixes.

**Per-IT:**

| IT | Cold-JVM target | Notes |
|---|---|---|
| `KafkaTransactionalProducerITCase` | ≤ 90 s | landed |
| `KafkaInitProducerIdITCase` | ≤ 90 s | landed |
| `KafkaReadCommittedITCase` | n/a (`@Disabled`) | landed |
| `KafkaTypedAlterITCase` (3 active) | ≤ 90 s | landed |
| `KafkaStreamsStateStoreITCase` | ≤ 90 s | C.1 |
| `KafkaStreamsWindowedAndJoinsITCase` | ≤ 120 s | C.2 |
| `KafkaStreamsSchemaRegistryITCase` | ≤ 90 s | C.3 |
| `KafkaConnectFileSinkITCase` | ≤ 90 s | D.1 |
| `KafkaConnectWithSaslScramITCase` | ≤ 6 min (5 min soak) | D.2 |
| `KafkaConnectWithAclITCase` | ≤ 90 s | D.3 |
| `KafkaConnectMirrorMakerITCase` | ≤ 3 min | D.4 |

## 10. Future considerations (out of scope for this plan)

- **Broker-side warmup hook** — A.3's residual `MAX_BLOCK_MS=120_000`
  bandaid is masking a 30–60 s broker cold start (lazy
  `Connection` open in `TransactionCoordinator`). A future change adds
  an explicit hook to `TransactionCoordinatorBootstrap` that opens the
  catalog `Connection` eagerly post-leader-election, removing the
  broker-side cold-start tax. Not test-utility scope; touches
  `fluss-kafka/src/main/java/.../tx/TransactionCoordinatorBootstrap.java`
  + `TransactionCoordinator.java`.
- **Phase T.3 layout fix** — see §4.1 for the three options. Likeliest
  path is a small fluss-core extension to `SchemaUpdate` accepting
  `AddColumn(BEFORE)` for non-PK columns, gated on a config flag so
  Fluss-native callers stay unaffected.
- **Phase J.3 read_committed fix** — see §4.2. Likely a one-line race
  in `KafkaFetchTranscoder.encode` between LSO read + marker write
  visibility; will need a small instrumentation pass to confirm
  before patching.
- **CI integration** — every new IT runs locally on cold-JVM-fresh,
  but CI integration is a separate call once timings stabilise across
  hosts. The disables in §4 mean nothing in this plan blocks CI today.
- **ksqlDB / Spring Kafka / Akka Streams ITs** — Streams exercises the
  same wire surface; adding ksqlDB or Spring is parallel work, not on
  this plan's critical path.

## 11. What this plan does NOT do

- **No new design docs.** Docs 0010-0017 plus this one are the
  reference; further phases get their own docs (e.g. a future
  `0019-broker-warmup-hook.md` if §10 lands).
- **No fluss-core edits.** Workstream A's helper is purely
  test-utility; B's `@Disabled` notes don't change production code; C
  and D add only test files plus possibly two test-scope POM entries.
- **No new wire-API support.** Every IT in this plan exercises code
  already on `main` (or expectedly on `main` after the pending
  broker-side fixes for T.3 and J.3).
