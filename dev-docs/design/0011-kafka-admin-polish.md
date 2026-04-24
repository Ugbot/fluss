# Design 0011 — Kafka admin polish: ElectLeaders, DescribeProducers, DescribeClientQuotas, AlterClientQuotas

**Status:** Phase I gap-fill, 2026-04-24.
**Supersedes:** none — narrows §I.2/I.3 of design 0006 to the four admin APIs
where the wire handler exists but the depth behind it is thin.
**Related:** `0001-kafka-api-protocol-and-metadata.md`,
`0005-catalog-and-projections.md`, `0006-kafka-api-mapping-into-fluss.md`,
`0010-kafka-acl-enforcement.md`.

## Context

Kafka's ecosystem of inspection and cluster-admin tooling — Cruise Control,
LinkedIn's `kafka-reassign-partitions` wrappers, Confluent Control Center,
`kafka-configs.sh --entity-type users --describe`,
`kafka-producer-state-viewer`, Strimzi's operator health checks — routes
through a small set of admin wire APIs that are orthogonal to Produce /
Fetch. The bolt-on has to answer those calls with truthful data or the
tools silently disagree with reality: Cruise Control decides every
partition is correctly balanced because every preferred-leader-election
returns `ELECTION_NOT_NEEDED`; `kafka-producer-state-viewer` renders an
empty table where it should show the idempotent producer's last
sequence; `kafka-configs.sh --alter --add-config` succeeds but the next
`--describe` reveals nothing was written.

Phase G (`0010`) closed the authorization gap — all four APIs are gated
per the audit in that doc (L100–L103). Phase I.1 landed the handler
skeletons. What's thin is the depth behind each handler:
`KafkaElectLeadersTranscoder` classifies partitions correctly but never
drives a real election; `KafkaDescribeProducersTranscoder` already
projects `WriterStateManager` state through
`ReplicaManager.getProducerStates` (`ReplicaManager.java:2060`) but no IT
pins the idempotent-producer round-trip;
`KafkaClientQuotasTranscoder` persists every upsert into
`_catalog._client_quotas` (`SystemTables.java:125`) but no IT pins
persistence across a cluster restart. One PR, three independent commits.

## Status at time of writing

Every path below already returns a syntactically valid Kafka response; the
question is whether the data is truthful. Line numbers reference
`KafkaRequestHandler.java` and the per-API transcoder files.

| API | Handler entry | Transcoder | Wire-shape | Data-truth | What's missing |
|---|---|---|---|---|---|
| `ELECT_LEADERS` | `handleElectLeadersRequest` L1734 | `admin/KafkaElectLeadersTranscoder.java` | Shipped | Read-only classification | Drive a real preferred-leader election when `preferred != current leader`; today returns `PREFERRED_LEADER_NOT_AVAILABLE` with a "not yet exposed" message (`KafkaElectLeadersTranscoder.java:183`). |
| `DESCRIBE_PRODUCERS` | `handleDescribeProducersRequest` L2018 | `admin/KafkaDescribeProducersTranscoder.java` | Shipped | Wired through `ReplicaManager.getProducerStates` (`ReplicaManager.java:2060`) → `WriterStateManager.activeWriters()` (`LogTablet.java:272`) | No IT pins the Kafka-wire round-trip; `producerEpoch=0`, `coordinatorEpoch=-1`, `currentTxnStartOffset=-1` are intentional and need doc-cover. |
| `DESCRIBE_CLIENT_QUOTAS` | `handleDescribeClientQuotasRequest` L1766 | `admin/KafkaClientQuotasTranscoder.java` | Shipped | `CatalogService.listClientQuotas(filter)` (`CatalogService.java:157`) is real; backs onto `_catalog._client_quotas` (`SystemTables.java:125`) | No IT pins persistence-across-restart; multi-dimensional entity flattening is documented but not yet test-covered. |
| `ALTER_CLIENT_QUOTAS` | `handleAlterClientQuotasRequest` L1799 | same | Shipped | `CatalogService.upsertClientQuota` / `deleteClientQuota` (`CatalogService.java:149, 154`) | Same: no restart IT, no explicit null-value-removes IT. |

Authz coverage lands per Doc 0010 Table §Audit: `ALTER on CLUSTER` for
`ELECT_LEADERS` and `ALTER_CLIENT_QUOTAS`, `DESCRIBE on CLUSTER` for
`DESCRIBE_CLIENT_QUOTAS`, and `READ on TABLE(kafkaDatabase, topic)` per
partition for `DESCRIBE_PRODUCERS`. No new gates here.

## 3. ElectLeaders — real preferred-replica election

Kafka's wire shape is:

```
ElectLeadersRequest {
  electionType: int8          // 0 = PREFERRED, 1 = UNCLEAN
  topicPartitions: [{ topic: string, partitions: [int32] }]?
}
ElectLeadersResponse {
  throttleTimeMs: int32
  errorCode: int16
  replicaElectionResults: [{
    topic: string
    partitionResult: [{ partitionId: int32, errorCode: int16, errorMessage: string? }]
  }]
}
```

An absent `topicPartitions` means "elect for every topic" — the current
transcoder short-circuits to `NONE` with an empty list
(`KafkaElectLeadersTranscoder.java:109`), which is a valid Kafka response
but lets Cruise Control believe the cluster-wide pass was a no-op.

### Fluss mapping

The election machinery already exists inside the coordinator. Three
strategies in `ReplicaLeaderElection.java` —
`DefaultLeaderElection`, `ControlledShutdownLeaderElection`,
`ReassignmentLeaderElection` — all implement the "pick the first replica
in assignment that is alive and in ISR" rule Kafka calls preferred
election. They are invoked by `TableBucketStateMachine` from
`CoordinatorEventProcessor.java:215` but the entry points are
package-private (Doc 0006 §0).

The polish exposes one primitive on `CoordinatorGateway`. New proto
messages `ElectLeadersRequest { table_bucket_list }` /
`ElectLeadersResponse { per_bucket_result: (bucket, error_code,
error_message?) }` land in `FlussApi.proto` with `ApiKeys.ELECT_LEADERS`
at slot 1062 (next free after `SCAN_KV`, PUBLIC visibility):

```java
// fluss-rpc/src/main/java/org/apache/fluss/rpc/gateway/CoordinatorGateway.java
@RPC(api = ApiKeys.ELECT_LEADERS)
CompletableFuture<ElectLeadersResponse> electLeaders(ElectLeadersRequest request);
```

`CoordinatorService` dispatches to a new `ElectLeadersHandler` that
enqueues a `CoordinatorEvent` (serialising against table-state-machine
mutations) and drives `TableBucketStateMachine.handleStateChange` with
a new `PreferredReplicaPartitionLeaderElection` strategy — a direct
port of `DefaultLeaderElection.leaderElection`
(`ReplicaLeaderElection.java:46`) with the early-out "preferred ==
currentLeader → empty" promoted from the caller into the strategy so
the state machine can return `ELECTION_NOT_NEEDED` without mutating ZK.

The transcoder becomes: (1) look up each topic via
`KafkaTopicsCatalog.lookup`; (2) group into `List<TableBucket>`; (3)
one `coordinatorGateway.electLeaders(...)` call; (4) map per-bucket
results — `NONE` or `ELECTION_NOT_NEEDED` as the coordinator indicates,
`LEADER_NOT_AVAILABLE` (preferred not in ISR) →
`PREFERRED_LEADER_NOT_AVAILABLE`, `UNKNOWN_TABLE_OR_BUCKET` →
`UNKNOWN_TOPIC_OR_PARTITION`, RPC timeout → `NOT_CONTROLLER`.
`electionType == UNCLEAN` continues to short-circuit to
`INVALID_REQUEST` (`KafkaElectLeadersTranscoder.java:98`).

### Error surfaces

| Kafka error | Fluss condition |
|---|---|
| `NONE` | Election fired, leader flipped to preferred replica. |
| `ELECTION_NOT_NEEDED` | Preferred replica already the leader before the call. |
| `PREFERRED_LEADER_NOT_AVAILABLE` | Preferred replica not in ISR, or offline. |
| `UNKNOWN_TOPIC_OR_PARTITION` | Catalog miss, or bucket has no assignment. |
| `NOT_CONTROLLER` | Coordinator unreachable / RPC timeout. |
| `INVALID_REQUEST` | `electionType == UNCLEAN` (not supported). |
| `CLUSTER_AUTHORIZATION_FAILED` | Top-level, from `handleElectLeadersRequest` L1743. |

Authz is already `ALTER on CLUSTER` (`KafkaRequestHandler.java:1743`);
no change.

## 4. DescribeProducers — real idempotent-producer state

Kafka's wire shape, per partition, returns a list of `ProducerState`:

```
ProducerState {
  producerId: int64
  producerEpoch: int32
  lastSequence: int32
  lastTimestamp: int64
  coordinatorEpoch: int32
  currentTxnStartOffset: int64
}
```

### Fluss mapping

The projection is already wired end-to-end. `KafkaDescribeProducersTranscoder`
(transcoder file L115–L159) looks up the bucket via
`TableBucket bucket = new TableBucket(tableId, partitionId)`, calls
`ReplicaManager.getReplicaSnapshot` for leadership state, then
`ReplicaManager.getProducerStates(bucket)` (`ReplicaManager.java:2060`).
That method pulls the active-writers map from `LogTablet.activeWriters()`
(`LogTablet.java:272`), which is itself a view over
`WriterStateManager.activeWriters()` — the same map the Produce path
consults to dedupe by `(producerId, sequence)` at
`LogTablet.java:776`. Every idempotent producer (one that sets
`enable.idempotence=true` and therefore sends a producer-id on
InitProducerId) populates this map as a side effect of writing, with no
Phase J prerequisite.

Field-by-field mapping, per `ReplicaManager.java:2087–2101`:

| Kafka `ProducerState` field | Fluss source | Rationale |
|---|---|---|
| `producerId` | `WriterStateEntry.writerId` | Key of `activeWriters()`. Minted by `InitProducerId`. |
| `producerEpoch` | `0` (hard-coded) | Fluss has no per-writer epoch today; Kafka clients tolerate 0 (Doc 0006 §0). |
| `lastSequence` | `WriterStateEntry.lastBatchSequence()` | The sequence number of the most recent successfully-appended batch. |
| `lastTimestamp` | `WriterStateEntry.lastBatchTimestamp()` | Millis since epoch of that batch. |
| `coordinatorEpoch` | `-1` (hard-coded) | No transaction coordinator. |
| `currentTxnStartOffset` | `-1` (hard-coded) | No transactions. |

The `-1` / `0` fillers are a wire-format convention Kafka uses for "no
value" in these fields; `AdminClient.describeProducers` surfaces them
verbatim without error.

### Error surfaces

| Kafka error | Condition |
|---|---|
| `NONE` | Bucket is hosted, this broker is leader, active-writers map read cleanly. |
| `NOT_LEADER_OR_FOLLOWER` | Bucket is hosted but this broker is follower (transcoder L129). |
| `UNKNOWN_TOPIC_OR_PARTITION` | Catalog miss, or bucket not hosted on this broker (transcoder L122, L143). |
| `UNKNOWN_SERVER_ERROR` | `getProducerStates` threw (transcoder L139). |
| `TOPIC_AUTHORIZATION_FAILED` | Per-partition, from `handleDescribeProducersRequest` L2051. |

Authz is `READ on TABLE(kafkaDatabase, topic)` as a topic batch
(`KafkaRequestHandler.java:2034`). Already wired.

The code is shipped. Polish is: (a) write the IT that proves an
idempotent producer's `(pid, epoch, lastSequence)` flow out through
`AdminClient.describeProducers`; (b) promote the `0` / `-1` filler
rationale from `ReplicaManager` into the transcoder class Javadoc.

## 5. DescribeClientQuotas / AlterClientQuotas — storage-only (Path A)

Kafka's wire shapes:

```
AlterClientQuotasRequest {
  entries: [{
    entity: [{ entityType: string, entityName: string? }]   // null name = default entity
    ops: [{ key: string, value: double, remove: bool }]
  }]
  validateOnly: bool
}
DescribeClientQuotasRequest {
  components: [{ entityType: string, matchType: int8, match: string? }]
                                        // 0=EXACT, 1=DEFAULT, 2=SPECIFIED
  strict: bool
}
```

Supported `quota_key` values (Kafka's `ClientQuotaType`):
`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`,
`controller_mutation_rate`. The transcoder accepts any string — matching
Kafka's own lenient storage semantics (`KafkaClientQuotasTranscoder.java:52`).

### Catalog entity

Already present. `SystemTables.CLIENT_QUOTAS` (`SystemTables.java:125`)
registers `_catalog._client_quotas` with the schema:

```
PK (entity_type STRING, entity_name STRING, quota_key STRING)
   quota_value DOUBLE
   updated_at  TIMESTAMP_LTZ(3)
```

`entity_name = ""` (`ClientQuotaEntry.DEFAULT_ENTITY_NAME`,
`ClientQuotaEntry.java:38`) represents Kafka's null-name "default
entity" convention. The transcoder normalises null-name inbound to the
empty string (`KafkaClientQuotasTranscoder.java:194`) and back to null
outbound (`KafkaClientQuotasTranscoder.java:119`).

### Path A decision

Writes persist, reads return, no enforcement:

- `kafka-configs.sh --entity-type users --entity-name alice --alter
  --add-config producer_byte_rate=1024000` succeeds and survives restart.
- `--describe` echoes the stored value.
- Produce / Fetch / dispatch paths do **not** consult these rows. No
  token bucket is installed; a client exceeding its stored
  `producer_byte_rate` still gets full throughput.

Path B (enforcement) is a later phase — see §10. Path A is
compatibility with admin tooling, not rate-limiting.

### Multi-dimensional entities

Kafka allows joint entities like `(user=alice, client-id=ingest)`. The
transcoder flattens a multi-dimensional entity to one row per component
(`KafkaClientQuotasTranscoder.java:188`); reads group single-dimension
rows back by `(entityType, entityName)`
(`KafkaClientQuotasTranscoder.java:105`), so every returned entry
carries exactly one entity-component. Default-entity requests
(`--entity-default`) match `entity_name = ""` through
`ClientQuotaFilter.Component.ofDefault(entityType)`
(`KafkaClientQuotasTranscoder.java:221`). Joint enforcement is not
Path A's concern — when Path B lands, the token bucket will key on the
full composite and the flattening here changes (flagged in §10).

### Error surfaces

| Kafka error | Condition |
|---|---|
| `NONE` | Upsert / delete succeeded, or describe returned 0..N entries. |
| `INVALID_REQUEST` | Empty entity list; EXACT match with null value; unknown matchType. |
| `UNKNOWN_SERVER_ERROR` | Catalog I/O failure (connection drop, ZK hiccup). |
| `CLUSTER_AUTHORIZATION_FAILED` | Per-entry on alter, top-level on describe; from `handleAlterClientQuotasRequest` L1816 / `handleDescribeClientQuotasRequest` L1782. |

Authz is `ALTER on CLUSTER` (alter) and `DESCRIBE on CLUSTER`
(describe) per Doc 0010 Table §Audit. Already wired.

## 6. Catalog / SPI additions

All three handlers sit on top of existing interfaces. No new catalog API
is introduced. What changes:

| Surface | File | Status |
|---|---|---|
| `CatalogService.upsertClientQuota` / `deleteClientQuota` / `listClientQuotas` | `fluss-catalog/.../CatalogService.java:149, 154, 157` | Shipped. |
| `FlussCatalogService` impls | `fluss-catalog/.../FlussCatalogService.java:487, 509, 521` | Shipped. Mirrors the `ConsumerOffsetsTable` pattern. |
| `SystemTables.CLIENT_QUOTAS` + `ClientQuotaEntry` / `ClientQuotaFilter` | `fluss-catalog/.../SystemTables.java:125, 174`; `.../entities/` | Shipped. |
| `CoordinatorGateway.electLeaders(ElectLeadersRequest)` | `fluss-rpc/.../gateway/CoordinatorGateway.java` | **New** `@RPC(api = ApiKeys.ELECT_LEADERS)` method. |
| `ApiKeys.ELECT_LEADERS` | `fluss-rpc/.../protocol/ApiKeys.java` | **New** slot 1062, PUBLIC. |
| Proto `ElectLeadersRequest` / `Response` / `PbElectLeaders*` | `fluss-rpc/src/main/proto/FlussApi.proto` | **New**. Mirrors `AdjustIsr` shape (L378–L969) — table-grouped. |
| `ReplicaLeaderElection.PreferredReplicaPartitionLeaderElection` | `fluss-server/.../statemachine/ReplicaLeaderElection.java:31` | **New** fourth subclass; `DefaultLeaderElection` algorithm with `preferred == currentLeader → empty` promoted into the strategy. |
| `CoordinatorEventProcessor.onElectLeaders(...)` | `fluss-server/.../CoordinatorEventProcessor.java:568` | **New** `CoordinatorEvent` subclass + branch in `process(...)`. Drives `TableBucketStateMachine.handleStateChange`. |
| `KafkaElectLeadersTranscoder` | `fluss-kafka/.../admin/KafkaElectLeadersTranscoder.java` | **Rewritten**: drops the "no public election API" branch (current L183); routes through the new gateway. |

No changes to `CatalogService` or `SystemTables`. No changes to the
`_client_quotas` schema. The bolt-on footprint remains under the
discipline set in Doc 0006 §0.1.

## 7. Files

Adds:

```
fluss-rpc/src/main/proto/FlussApi.proto                                   (+ ~50 LOC)
fluss-server/src/main/java/org/apache/fluss/server/coordinator/
    ElectLeadersHandler.java                                              (new)
    statemachine/
        ReplicaLeaderElection.java          (+ PreferredReplicaPartitionLeaderElection)
fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/
    ElectLeadersEvent.java                                                (new)
fluss-kafka/src/test/java/org/apache/fluss/kafka/
    KafkaClientQuotasAdminITCase.java                                     (new, ~350 LOC)
```

Modifies:

```
fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java        (+1 enum)
fluss-rpc/src/main/java/org/apache/fluss/rpc/gateway/CoordinatorGateway.java (+1 method)
fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java
fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java
fluss-kafka/src/main/java/org/apache/fluss/kafka/admin/KafkaElectLeadersTranscoder.java
fluss-kafka/src/main/java/org/apache/fluss/kafka/admin/KafkaDescribeProducersTranscoder.java (Javadoc only)
fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaElectLeadersITCase.java (append scenarios)
fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaDescribeProducersITCase.java (append scenarios)
dev-docs/design/0006-kafka-api-mapping-into-fluss.md                      (audit-row refresh)
```

No edits under `fluss-common`, no new config options, no new
`ConfigOptions` entries.

## 8. Verification

Three ITs, one per commit, under
`fluss-kafka/src/test/java/org/apache/fluss/kafka/`. The first two
extend existing files; the third is new.

### 8.1 `KafkaElectLeadersITCase` — preferred election flips leader

Existing file (259 LOC) already exercises the read-only classification.
New scenarios assert the election fires:

1. **Baseline.** 3-node `FlussClusterExtension`; create
   `kafka.elect-topic` with 3 partitions, replication=3. Assert
   partition 0's leader is replica 0 (preferred — first in assignment).
2. **Force non-preferred leader.** Fluss has no public "migrate leader"
   helper. Use `FlussClusterExtension.stopTabletServer(0)` then
   `startTabletServer(0)`: stop triggers
   `ControlledShutdownLeaderElection` to migrate leader to replica 1 or
   2; after restart, replica 0 rejoins ISR as follower. Assert leader
   is now 1 (or 2).
3. **Fire PREFERRED.**
   `adminClient.electLeaders(ElectionType.PREFERRED, singleton(tp0))`;
   assert future completes and, within a bounded wait, partition 0's
   leader is 0 again.
4. **Already-preferred.** Run on partition 1 (never migrated); assert
   per-partition error is `ELECTION_NOT_NEEDED`.
5. **Unknown partition.** `TopicPartition("elect-topic", 99)` →
   `UNKNOWN_TOPIC_OR_PARTITION`.
6. **Unclean refusal.** `ElectionType.UNCLEAN` → future fails with
   `InvalidRequestException`.

### 8.2 `KafkaDescribeProducersITCase` — idempotent producer round-trip

Existing file (212 LOC). New scenario appended:

1. Create 1-partition topic `kafka.idempotent-topic`.
2. `KafkaProducer<byte[], byte[]>` with `enable.idempotence=true`,
   `acks=all`, `max.in.flight.requests.per.connection=5`. Produce 10
   records to partition 0; `flush()`.
3. `adminClient.describeProducers(singleton(tp0))`; await.
4. Assert the returned `ProducerState` list has exactly one entry with
   `producerId > 0`, `producerEpoch == 0`, `lastSequence == 9`,
   `lastTimestamp > 0`, `coordinatorEpoch == -1`,
   `currentTxnStartOffset == -1`.
5. **Negatives.** Query a non-leader broker in a multi-node cluster →
   `NOT_LEADER_OR_FOLLOWER`; query a non-existent topic →
   `UNKNOWN_TOPIC_OR_PARTITION`.

### 8.3 `KafkaClientQuotasAdminITCase` — catalog-backed round-trip

New file. Follows `KafkaAdminConfigsITCase` shape — single-node
`FlussClusterExtension`, PLAINTEXT listener, no authorizer.

1. **Upsert.** `alterClientQuotas({user=alice}, producer_byte_rate=1024.0)`;
   assert future completes.
2. **Describe back.** `describeClientQuotas(containsOnly(ofEntity("user",
   "alice")))`; assert result is exactly `{user=alice} →
   {producer_byte_rate=1024.0}`.
3. **Default entity.** Upsert `producer_byte_rate=2048` for
   `ClientQuotaEntity(singletonMap("user", null))`. Describe with
   `ofDefaultEntity("user")`; assert returned entry has null
   entity-name and `quotaValue=2048.0`.
4. **Remove via null value.** `alterClientQuotas` with
   `Op("producer_byte_rate", null)` for `{user=alice}`. Subsequent
   describe returns no entry for alice with that key; default-entity
   row from step 3 unaffected.
5. **Persistence across restart.** Upsert `{user=bob,
   consumer_byte_rate=4096}`; `FLUSS_CLUSTER_EXTENSION.stopAndRestartCluster()`;
   re-open `AdminClient`; describe `{user=bob}`; assert value still
   `4096.0`.
6. **Strict filter.** Upsert `{client-id=svc, request_percentage=25.0}`;
   describe with strict filter on `user`; assert the `client-id` entry
   is **not** returned.
7. **Invalid request.** Empty entity list via protocol-level harness
   (not `AdminClient`, which refuses client-side); assert
   `INVALID_REQUEST` per-entry with message "Empty entity in
   AlterClientQuotas entry."
8. **Enforcement-negative.** Produce 10 MB/s to a topic with
   `{user=alice, producer_byte_rate=1024}` set; assert throughput is
   **not** throttled. Path A has no enforcement; this row pins the
   documented non-behaviour so Path B doesn't land silently.

No new grep gates — Doc 0010's `AuthzHelper` rules still hold.

## 9. Scope and sequencing

One PR with three independent commits. Each commit compiles and tests
pass on its own so they can be bisected.

**Commit 1: ElectLeaders end-to-end** (~400 LOC).
`ApiKeys.ELECT_LEADERS`, proto, `CoordinatorGateway.electLeaders`,
`PreferredReplicaPartitionLeaderElection`, `ElectLeadersEvent` +
`CoordinatorEventProcessor.onElectLeaders` + `CoordinatorService`
dispatch, transcoder rewrite, IT scenarios (~80 LOC of tests).

**Commit 2: DescribeProducers IT + transcoder Javadoc** (~300 LOC,
mostly test). No production change —
`ReplicaManager.getProducerStates`, `WriterStateManager.activeWriters`,
transcoder are in place. Promote the `producerEpoch=0` /
`coordinatorEpoch=-1` / `currentTxnStartOffset=-1` rationale from
`ReplicaManager.java:2056–2058` into the transcoder Javadoc. Append the
idempotent-producer scenario (~200 LOC).

**Commit 3: ClientQuotas IT + transcoder polish** (~400 LOC). No
production change in the happy path. Audit the transcoder against the
eight IT scenarios and fix any divergence surfaced — expected:
null-value-removes needs an explicit `op.value() == null` branch,
currently inferred from `op.remove()` at
`KafkaClientQuotasTranscoder.java:197`. Verify kafka-clients sets
`remove=true` when value is null, or add the check. New
`KafkaClientQuotasAdminITCase` (~350 LOC).

Roughly 1100 LOC total, 80 % test.

## 10. Out of scope

- **Client-quota enforcement (Path B).** Per-principal token bucket on
  Produce / Fetch is a separate phase. Belongs in `fluss-server` so
  native Fluss clients hit the same throttle; will need the full
  `(user, client-id, ip)` composite key, not the flattened rows here.
- **Per-broker / per-topic quotas.** Kafka's `<topic>.*` quota keys are
  stored via `AlterConfigs`, not `AlterClientQuotas`. Rare; deferred.
- **`IncrementalAlterClientQuotas`.** Kafka 3.0 addition; not
  advertised, deferred until a concrete tooling complaint surfaces.
- **Transactional producers in `DescribeProducers`.** `coordinatorEpoch`
  / `currentTxnStartOffset` stay `-1` until Fluss grows a transaction
  coordinator (Phase J). `-1` is a valid Kafka wire value for "no
  transaction".
- **Unclean preferred-leader election.** Fluss has no analogue;
  `INVALID_REQUEST` stays (Doc 0001 §metadata).
- **`electLeaders` without `topicPartitions` (elect-all).** Transcoder
  continues to short-circuit to `NONE`-with-empty-list; AdminClient
  does not rely on it (Cruise Control issues per-partition calls).
- **SCRAM-credentials admin depth.** `DescribeUserScramCredentials` /
  `AlterUserScramCredentials` are listed as authz gaps in Doc 0010
  §Gap list; storage-depth polish is a separate phase — they back onto
  a broker-private credential store, not the catalog.
