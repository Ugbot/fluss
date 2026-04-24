# Design 0017 — Kafka Connect and Kafka Streams Test Harness

**Status:** Proposed
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0001-kafka-api-protocol-and-metadata.md`,
`0006-kafka-api-mapping-into-fluss.md`,
`0010-kafka-acl-enforcement.md`,
`0012-kafka-bolt-on-observability.md`,
`0013-kafka-sr-schema-references.md`,
`0014-kafka-typed-tables-hot-path.md`,
forward-refs `0015` (SR typed hot path wire contract) and
`0016` (Kafka EOS / transactions).

## Motivation

Every IT in `fluss-kafka/src/test/java/` today is a kafka-clients
AdminClient, KafkaProducer, or KafkaConsumer driven against a
`FlussClusterExtension`. That covers the request/response pairs and
the basic session shape, but it deliberately does not cover the two
client-side frameworks our users are most likely to deploy:
**Kafka Connect** (long-lived workers with embedded producers,
consumers, and a task-supervision loop) and **Kafka Streams** (a
topology that fuses producer, consumer, and local RocksDB state
against compacted changelog topics). Both wrap kafka-clients and
then exercise timing, lifecycle, and coordination paths that a
unit-style IT never touches: re-authentication across a
`connections.max.reauth.ms` boundary, rebalance storms when a task
restarts, exactly-once producer fencing, multi-task Connect workers
sharing one worker-level group, and the graceful consumer close
that flushes offsets and releases the group lock.

None of the phases in the roadmap can claim production-readiness
while those codepaths remain unexercised. Phase G (ACLs; doc 0010)
has no regression test against a framework that hot-swaps its
client principal. Phase M (observability; doc 0012) has no
workload that actually emits the high-cardinality label shapes real
deployments produce. Phase T (typed hot path; doc 0014) has no
end-to-end proof that a Streams app reading the typed wire sees
byte-identical output to a Kafka app reading the same topic. Phase
J (exactly-once; doc 0016) cannot be validated at all without a
Streams app configured with `processing.guarantee=exactly_once_v2`,
because a bare kafka-clients transactional producer does not
exercise the consumer-group-metadata-in-txn path that Streams
depends on.

This document defines two reusable in-JVM harnesses —
`EmbeddedConnectCluster` and `EmbeddedStreamsApp` — and the smoke
ITs that prove them. The harnesses land in one focused PR. Each
subsequent phase's PR then adds one Connect or Streams IT on top,
at ~300 LOC each, with no incremental infrastructure cost. The net
effect is that every wire-level phase gets a realistic client-driven
regression test alongside its kafka-clients unit IT, for roughly
the cost of writing the unit IT twice.

## Scope

Two in-JVM test harnesses and four smoke/validation ITs in the
`fluss-kafka` module. Both harnesses point their bootstrap at a
`FlussClusterExtension`-managed KAFKA listener, identical to the
existing ITs in `fluss-kafka/src/test/java/org/apache/fluss/kafka/`.

- **`EmbeddedConnectCluster`** — wraps Kafka Connect's `Connect`
  runtime inside the test JVM. Defaults to `ConnectStandalone` to
  keep start-up cheap (~3-5 s); exposes a `distributed` opt-in flag
  for the MM2 IT which requires a cluster-mode worker. Manages
  per-test tmp dirs for source/sink connector state files and for
  the worker's offset/status/config storage (distributed mode only).
- **`EmbeddedStreamsApp`** — starts a `KafkaStreams` instance for a
  caller-supplied `Topology` and `Properties`. Per-test `state.dir`
  is a `Path.of("target", "streams-" + UUID.randomUUID())` so
  parallel test forks cannot collide on RocksDB locks. Waits for
  `KafkaStreams.State.RUNNING` before returning control.

Both harnesses are JUnit 5 aware: `EmbeddedConnectCluster`
implements `BeforeEachCallback` + `AfterEachCallback` for
`@RegisterExtension` use; `EmbeddedStreamsApp` implements
`AutoCloseable` for use inside an explicit `@BeforeEach`/`@AfterEach`
pair. Either style is acceptable inside a single IT; the existing
`KafkaSaslPlainITCase` shows the `@RegisterExtension` idiom on the
cluster extension itself and the smoke ITs follow the same shape.

## Kafka Connect harness

### 3.1 Dependencies

New test-scope entries in `fluss-kafka/pom.xml` (kafka.version is
already pinned at `3.9.2`, see `fluss-kafka/pom.xml:33`):

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>connect-api</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>connect-runtime</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>connect-json</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>connect-file</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
```

`connect-runtime` is the only one of these that carries significant
transitive weight (Jetty, Jackson, reflections). It is strictly
test-scope so it never leaks into the distribution jar. The MM2 IT
additionally needs `connect-mirror` and `connect-mirror-client` at
test scope; those are added in the same PR so the MM2 IT lands
green the first time.

### 3.2 Harness class

Location: `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/EmbeddedConnectCluster.java`.

```java
public final class EmbeddedConnectCluster
        implements BeforeEachCallback, AfterEachCallback {

    private final String bootstrapServers;
    private final Map<String, String> workerProps;
    private final boolean distributed;
    private final Path offsetsFile;   // standalone mode
    private Connect connect;
    private Herder herder;

    public EmbeddedConnectCluster(String bootstrapServers,
                                  Map<String, String> extraWorkerProps);
    public EmbeddedConnectCluster distributedMode();

    public void startConnector(String name,
                               Map<String, String> connectorConfig);
    public void stopConnector(String name);
    public ConnectorInfo connectorInfo(String name);
    public ConnectorStateInfo connectorStatus(String name);
    public URI workerUri();           // REST base; distributed only
}
```

Defaults the harness sets on every worker regardless of mode:

| Property | Value | Reason |
|---|---|---|
| `bootstrap.servers` | from `FlussClusterExtension` | points at our broker |
| `key.converter` | `org.apache.kafka.connect.json.JsonConverter` | matches `connect-json` dep |
| `value.converter` | `org.apache.kafka.connect.json.JsonConverter` | same |
| `key.converter.schemas.enable` | `false` | keep payloads compact for asserts |
| `value.converter.schemas.enable` | `false` | same |
| `offset.flush.interval.ms` | `500` | tight loop for tests; default is 60 s |
| `plugin.path` | `""` | rely on classpath; connect-file is on it |
| `auto.offset.reset` | `earliest` (consumer override) | tests start after topic populated |

Distributed mode adds `group.id`, `config.storage.topic`,
`offset.storage.topic`, `status.storage.topic`, each with a
deterministic per-test suffix to avoid cross-test contamination;
the harness pre-creates those three topics with 3 partitions and
`cleanup.policy=compact` via the `kafka-clients` AdminClient before
starting the worker.

### 3.3 Planned ITs

- **`KafkaConnectFileSourceITCase`** — `FileStreamSourceConnector`
  reads 1000 lines (randomised via `ThreadLocalRandom`) from a
  JUnit `@TempDir` file and writes to `fluss-topic`. A Fluss `Table`
  opened via the public SDK then reads the backing
  `kafka.fluss-topic` table and asserts all 1000 payloads arrive
  with no gaps or duplicates. Exercises Connect's source-side
  offset-commit cadence (the harness sets `offset.flush.interval.ms=500`
  to make this deterministic under a 30 s deadline).
- **`KafkaConnectFileSinkITCase`** — a Fluss SDK writer populates
  `kafka.sink-topic`; `FileStreamSinkConnector` consumes and writes
  to a `@TempDir` file. Byte-equal assertion (order within a
  partition is preserved by the sink). Exercises the sink-side
  `OffsetCommitRequest` path and the `ConsumerRebalanceListener`
  hooks Connect installs.
- **`KafkaConnectMirrorMakerITCase`** — two `FlussClusterExtension`
  instances: source and target. MM2 runs in the target's harness in
  distributed mode with `MirrorSourceConnector` +
  `MirrorCheckpointConnector` + `MirrorHeartbeatConnector`.
  Produces records on the source, asserts replication on the target,
  asserts offset sync in `__consumer_offsets` replication. This is
  the only IT that requires `distributed` mode and is the reason
  the harness supports it.
- **`KafkaConnectWithSaslScramITCase`** — file-source over
  SASL_PLAINTEXT with SCRAM-SHA-256, mirroring the JAAS wiring in
  `KafkaSaslPlainITCase.java:77-95`. Connect workers keep a single
  producer + consumer session alive for the test's full 60 s; the
  IT sets `connections.max.reauth.ms=15000` on the broker side so
  at least one re-auth round is forced. Asserts zero task failures
  across the re-auth boundary. This is the first IT in the suite
  that actually exercises the long-lived-session path — every
  existing kafka-clients IT opens and closes a fresh producer per
  test.
- **`KafkaConnectWithAclITCase`** — principal `alice` holds
  `WRITE on TABLE(kafka.alice-topic)` only (see design 0010 for the
  resource model). Two connectors configured on the same worker:
  one targeting `alice-topic` (expected `RUNNING`), one targeting
  `bob-topic` (expected `FAILED` with the task error surfacing
  `TOPIC_AUTHORIZATION_FAILED`). Asserts the Connect herder reports
  task state correctly and the worker does not crash.

## Kafka Streams harness

### 4.1 Dependencies

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams-test-utils</artifactId>
  <version>${kafka.version}</version>
  <scope>test</scope>
</dependency>
```

`kafka-streams-test-utils` brings `TopologyTestDriver`,
`TestInputTopic`, and `TestOutputTopic`. Those are used in
topology-shape unit tests that don't need a real broker; the
planned ITs do not use them, but the dep is cheap and unblocks
future topology-shape tests in the same module.

### 4.2 Harness class

Location: `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/EmbeddedStreamsApp.java`.

```java
public final class EmbeddedStreamsApp implements AutoCloseable {

    private final KafkaStreams streams;
    private final Path stateDir;

    public EmbeddedStreamsApp(Topology topology,
                              Properties props,
                              Path stateDir);

    public void start();
    public void waitForRunning(Duration timeout);
    public KafkaStreams.State state();
    public void close();   // closes streams, deletes stateDir
}
```

The constructor forces the following overrides onto the caller's
`Properties`, failing fast if the caller tried to set a conflicting
value:

| Property | Forced value | Reason |
|---|---|---|
| `state.dir` | per-test UUID path | RocksDB lock isolation |
| `auto.offset.reset` | `earliest` | tests start after topic populated |
| `commit.interval.ms` | `500` | tight loop; default is 30 s |
| `default.deserialization.exception.handler` | fail-fast | surface wire-shape regressions |

`close()` calls `streams.close(Duration.ofSeconds(10))` then
deletes `stateDir` recursively via `IOUtils.deleteFileQuietly`
(guaranteed cleanup even on a failed test).

### 4.3 Planned ITs

- **`KafkaStreamsWordCountITCase`** — canonical word-count:
  raw lines produced to `input-topic`, topology
  `stream → flatMapValues(split) → groupBy(word) → count() → toStream → to(output-topic)`,
  consumer on `output-topic` asserts exact counts. Randomised input
  per `ThreadLocalRandom` seed. Exercises: partition assignment,
  `groupByKey` repartition topic, default Serdes path, state-store
  flush, changelog restore on startup (we stop/start the app once
  mid-test to prove restoration). **Paired with Phase M (doc 0012)**:
  asserts the high-cardinality metric labels for the changelog
  topic and the consumer group appear in the metrics reporter.
- **`KafkaStreamsExactlyOnceITCase`** — same word-count with
  `processing.guarantee=exactly_once_v2`. **Depends on Phase J
  (doc 0016)**; if J has not landed, the IT is `@Disabled` with a
  pointer comment to the blocking phase. Under a forced producer
  restart mid-stream (we kill the underlying `KafkaProducer` via
  reflection on `StreamsConfig.PRODUCER_PREFIX` internals — standard
  technique in Kafka's own EOS tests), asserts zero duplicate
  aggregate outputs downstream.
- **`KafkaStreamsStateStoreITCase`** — explicit RocksDB-backed
  keyed state store with a compacted changelog topic. App runs,
  is `close()`d, is recreated with the same `application.id`,
  asserts the state is restored from the changelog (not from local
  RocksDB — the per-test `state.dir` is deleted on close). This is
  the first IT that end-to-end proves compacted-topic CDC against a
  real consumer that depends on it; Phase C-T.2 already landed the
  compacted storage, but nothing today reads it as a changelog.
- **`KafkaStreamsSchemaRegistryITCase`** — Streams app wired with
  Confluent `SpecificAvroSerde` pointing at our Schema Registry
  HTTP listener. Produces typed Avro records to `input-topic`,
  consumes typed records from `output-topic`, asserts field-level
  equality. **Depends on Phase T.2 + T.3** (doc 0014, doc 0015)
  to prove the typed-hot-path round-trip survives the Streams
  serde contract; also exercises Phase SR-X.5 schema references
  when a schema chain is configured.
- **`KafkaStreamsWithTypedTablesITCase`** — a Fluss SDK writer
  produces typed rows directly into the backing table
  `kafka.typed-topic`, *bypassing* the Kafka protocol. A Streams
  app reads `typed-topic` via the Kafka protocol. Assertion: the
  Streams app sees byte-identical Avro-framed payloads to what the
  Kafka protocol would have emitted for the same rows. Proves that
  the typed hot path (doc 0015) preserves the Kafka wire contract
  end-to-end, including Confluent framing, schema-id embedding,
  and tombstone semantics.

## Harness design notes

Both harnesses are stateful: reuse within one IT class across
multiple `@Test` methods is supported via `@BeforeEach` wiring but
not via `@BeforeAll`, because Connect's internal thread pools hold
references to the bootstrap URL and a cluster restart between
tests requires a fresh worker.

`EmbeddedConnectCluster` exposes both the immutable `ConnectorInfo`
(configuration snapshot) and the dynamic `ConnectorStateInfo`
(per-task `RUNNING`/`FAILED`/`PAUSED` plus last error). Tests
assert on `ConnectorStateInfo.Connector` and each
`ConnectorStateInfo.Task`; the ACL IT in particular needs the task
state to flip to `FAILED` with a specific error message.

`EmbeddedStreamsApp.waitForRunning(Duration)` polls
`KafkaStreams.state()` every 100 ms until it equals `RUNNING` or
the deadline expires. On timeout it throws an
`AssertionError` that includes the full `StateListener` history
(we install one in the constructor and accumulate transitions)
— this is crucial for debugging flaky starts.

`kafka-streams-test-utils` is *not* used by the planned ITs; they
all drive a real `FlussClusterExtension` for fidelity. The dep is
present so follow-up topology-shape unit tests can be added cheaply
when a topology grows complex enough to warrant them.

## Stability considerations

- Connect workers have a cold-start cost of 3-5 s on a warm JVM,
  higher under CI. Use `Awaitility.await().atMost(30, SECONDS)` or
  equivalent polling with a 30 s deadline; do not use the Connect
  REST `/connectors/<name>/status` synchronously without a timeout.
- Streams apps need a per-test `state.dir` to avoid RocksDB lock
  collisions. The module's Surefire fork count is 1 today, but
  future parallel fork bumps should not silently break this — the
  UUID suffix is defensive.
- `auto.offset.reset=latest` is the Kafka default and will cause
  silent test hangs ("consumer sees nothing"). Both harnesses
  force `earliest` on every internal consumer.
- Connect distributed mode requires the three storage topics
  (`config`, `offset`, `status`) to exist before the worker starts.
  Pre-create with 3 partitions and `cleanup.policy=compact` via
  AdminClient. Three partitions matches Connect's default and
  avoids a rebalance when a second worker joins in the MM2 IT.
- Streams' `StreamsUncaughtExceptionHandler` defaults to
  `SHUTDOWN_CLIENT`. The harness installs a test-visible handler
  that records the exception and transitions the app to `ERROR`;
  tests can then `assertThat(app.state()).isEqualTo(RUNNING)` to
  detect silent topology failures.

## Cross-phase test matrix

Every phase in the roadmap gets exactly one Connect or Streams IT
on top of this harness, landed alongside that phase's PR:

| Phase / Doc | Connect IT | Streams IT |
|---|---|---|
| G (0010) | `KafkaConnectWithAclITCase` | — |
| H (landed) | `KafkaConnectWithSaslScramITCase` | — |
| M (0012) | — | `KafkaStreamsWordCountITCase` |
| SR-X.5 (0013) | — | `KafkaStreamsSchemaRegistryITCase` (reused with schema refs) |
| T.2 (0014) | — | `KafkaStreamsSchemaRegistryITCase` (typed on) |
| T.3 (0015) | — | `KafkaStreamsWithTypedTablesITCase` |
| J (0016) | — | `KafkaStreamsExactlyOnceITCase` |
| compaction (landed) | — | `KafkaStreamsStateStoreITCase` |
| MM2 validation (this PR) | `KafkaConnectMirrorMakerITCase` | — |
| source/sink validation (this PR) | `KafkaConnectFileSourceITCase`, `KafkaConnectFileSinkITCase` | — |

The bottom three rows land in this PR as the harness's
demonstration load; the rest land one per subsequent phase's PR.

## Sequencing

This doc and the harnesses land as **commit 9 of 10** in the
execution order recorded in the plan file, immediately *before*
Phase T.2. The reason is ordering: Phase T.2 is the first phase
that meaningfully changes the Kafka-to-Fluss decode path for typed
tables, and a Streams IT is the cheapest way to validate typed
hot-path behaviour end-to-end. Landing the harness first makes that
validation a 300 LOC delta rather than a 1000 LOC delta.

Phases G (0010) and M (0012) can consume the harness immediately
once it lands — neither depends on T.2 — so their ITs can land
in parallel PRs once this one merges.

## Files

Exhaustive list of the files touched by the harness PR:

**Modify:**
- `fluss-kafka/pom.xml` — add 6 test-scope dependency entries
  (`connect-api`, `connect-runtime`, `connect-json`, `connect-file`,
  `kafka-streams`, `kafka-streams-test-utils`) plus `connect-mirror`
  and `connect-mirror-client` for the MM2 IT (8 entries total).

**Add (harnesses):**
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/EmbeddedConnectCluster.java`
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/EmbeddedStreamsApp.java`

**Add (smoke ITs, land with the harness):**
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectHarnessSmokeITCase.java`
  — starts a `FileStreamSourceConnector` over a 10-line tmp file,
  waits for the connector to report `RUNNING`, drains the resulting
  topic via a kafka-clients consumer, asserts exactly 10 records
  arrived, stops the connector, asserts `ConnectorStateInfo` is
  absent afterward.
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/streams/KafkaStreamsHarnessSmokeITCase.java`
  — trivial topology: `stream → filter(alwaysTrue) → to`. Produces
  20 random records to the input topic, asserts 20 arrive on the
  output topic, asserts the app state reaches `RUNNING` and returns
  to `NOT_RUNNING` cleanly on close, asserts the per-test
  `state.dir` is deleted on close.

**Add (MM2 smoke IT, lands with the harness to prove distributed mode):**
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/connect/KafkaConnectMirrorMakerITCase.java`
  — two `FlussClusterExtension` instances, MM2 in distributed mode
  on the target, asserts replication + offset sync for a small
  workload.

## Verification

Running `./mvnw verify -pl fluss-kafka` must green both smoke ITs
plus the MM2 IT after this PR merges. Each subsequent phase's PR
adds one IT on top and must keep the smoke ITs green — a broken
smoke IT indicates the harness contract has drifted, not the
phase under test.

The harness PR also updates the module's Surefire configuration
only if needed to increase the default per-class timeout from the
current global default to 120 s; Connect IT workers with
distributed mode can take up to 45 s just to stabilise, and the
existing 60 s default is too tight.

## Scope estimate

| Component | LOC |
|---|---|
| `EmbeddedConnectCluster.java` | ~400 |
| `EmbeddedStreamsApp.java` | ~200 |
| `KafkaConnectHarnessSmokeITCase` | ~150 |
| `KafkaStreamsHarnessSmokeITCase` | ~120 |
| `KafkaConnectMirrorMakerITCase` | ~300 |
| `pom.xml` edits | ~40 |
| **Harness PR total** | **~1200** |
| Each subsequent phase IT | ~300 |

One PR lands the harness + three ITs. Seven subsequent PRs each
add one IT ~300 LOC. Total incremental testing surface across the
roadmap: ~3300 LOC, all test-scope, all in `fluss-kafka`.

## Out of scope

- **ksqlDB.** Confluent-licensed. It is implemented on top of Kafka
  Streams, so the Streams ITs already exercise the same underlying
  codepaths. Adding a ksqlDB IT adds no coverage.
- **Spring Kafka.** Same kafka-clients underneath; framework-specific
  bugs (annotation processing, context lifecycle) are not our
  concern. A user debugging a Spring Kafka integration can reach
  for the same kafka-clients ITs we already have.
- **Akka Streams / Alpakka Kafka.** Same argument as Spring Kafka.
- **Connect REST external clients.** The harness exposes
  `workerUri()` for the distributed-mode MM2 IT, but we do not
  build a full REST-driven IT suite; in-process configuration via
  `startConnector(name, config)` is enough for every phase in the
  roadmap.
- **Streams interactive queries (IQ).** Requires a stable
  host-info binding and a REST layer; not on the roadmap. If a
  phase needs it later, the harness exposes `KafkaStreams` directly
  so the test can reach in without harness changes.
