# Design 0016 — Kafka Transactions: exactly-once producer, `INIT_PRODUCER_ID`, consumer isolation

**Status:** Draft, 2026-04-24. ADR for Phase J. Deferred from the 2026-04-23
planning review once the scope of the one Fluss-core edit (last-stable-offset
on `LogTablet`) was confirmed out of reach for Phase I.
**Owner:** Ben Gamble
**Related:** `0001-kafka-api-protocol-and-metadata.md` (wire protocol surface),
`0004-kafka-group-coordinator.md` (the coordinator pattern this reuses),
`0006-kafka-api-mapping-into-fluss.md` (per-API landing map this extends),
`0010-kafka-acl-enforcement.md` (adds `TRANSACTIONAL_ID` resource type).

## Context & scope

Kafka transactions are the last piece of functional parity the bolt-on is
missing. Three concrete workloads want them: (a) Kafka Streams with
`processing.guarantee=exactly_once_v2`, which drives the majority of
Kafka-compatible deployments and is unusable without transactions, (b)
atomic multi-partition writes — a producer that publishes to `orders` and
`outbox` in the same batch and wants them either both committed or both
discarded, and (c) consumer-read-your-writes across groups, where a transform
pipeline commits both produced records and the consumer offsets that drove
them in a single atomic step. The Phase G and H bolt-on currently refuses all
six transaction-control APIs with `UNSUPPORTED_VERSION` (the
`handleUnsupportedRequest` fall-through at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:602`);
the `INIT_PRODUCER_ID` handler at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:2186`
is a stub that mints a monotonic in-process id (`STUB_PRODUCER_ID` at line
234) with `epoch=0`, enough for idempotent producers to advance their state
machine but structurally unable to carry transactional semantics.

This doc is the ADR that unblocks implementation. It covers the six
transaction wire APIs (`INIT_PRODUCER_ID` upgraded from stub,
`ADD_PARTITIONS_TO_TXN`, `ADD_OFFSETS_TO_TXN`, `END_TXN`,
`WRITE_TXN_MARKERS`, `TXN_OFFSET_COMMIT`) plus the two observability APIs
(`DESCRIBE_TRANSACTIONS`, `LIST_TRANSACTIONS`) that fall out of the state
store for free. It documents the one Fluss-core addition the phase requires
— a last-stable-offset (`LSO`) cursor on `LogTablet` and a corresponding
field on the wire fetch result — and the one small data-model change (a new
`ResourceType.TRANSACTIONAL_ID`). Everything else is additive inside
`fluss-kafka/` and `fluss-catalog/`. The phase is deliberately the biggest
remaining one and the hardest to sequence; the sub-phase split (J.1 → J.2 →
J.3) in §17 is designed so each lands a self-standing IT set and none
requires a big-bang merge.

## Kafka 3.9 transaction protocol overview

Kafka's transactional producer uses a stateful state machine negotiated with
a per-`transactional.id` coordinator. The client first calls
`INIT_PRODUCER_ID` against that coordinator (discovered via
`FindCoordinator` with coordinator-type `TRANSACTION` — already handled in
`fluss-kafka` via `FindCoordinator` request routing) and receives a
`(producerId, epoch)` pair. Each `beginTransaction()` is client-local; the
first `send()` of the transaction triggers `ADD_PARTITIONS_TO_TXN` against
the coordinator, binding every participating `(topic, partition)` to the
ongoing txn. `sendOffsetsToTransaction()` issues `ADD_OFFSETS_TO_TXN`
(registering the consumer group with the txn) followed by
`TXN_OFFSET_COMMIT` directly to the group coordinator for the actual offset
rows. `commitTransaction()` / `abortTransaction()` call `END_TXN` against
the txn coordinator; the coordinator then fans out `WRITE_TXN_MARKERS` to
every broker owning a participating partition, which appends a control
batch (commit or abort marker) to each partition's log. The LSO on each
partition advances past the marker, at which point consumers with
`isolation.level=read_committed` can see the records.

| API | Direction | Purpose |
|---|---|---|
| `INIT_PRODUCER_ID` | client → txn coord | Allocate/fence producer id + epoch |
| `ADD_PARTITIONS_TO_TXN` | client → txn coord | Bind partitions to the ongoing txn |
| `ADD_OFFSETS_TO_TXN` | client → txn coord | Bind a consumer group's offsets |
| `END_TXN` | client → txn coord | Commit or abort |
| `WRITE_TXN_MARKERS` | txn coord → broker | Append commit/abort markers per partition |
| `TXN_OFFSET_COMMIT` | client → group coord (broker) | Commit consumer offsets inside txn |
| `DESCRIBE_TRANSACTIONS` / `LIST_TRANSACTIONS` | client → txn coord | Observability |

Three APIs are new client-facing surfaces (`ADD_PARTITIONS_TO_TXN`,
`ADD_OFFSETS_TO_TXN`, `END_TXN`). Two are upgrades of existing handlers
(`INIT_PRODUCER_ID` from stub; `TXN_OFFSET_COMMIT` adds a
transactional-buffer path on top of the existing `OffsetCommit`). One
(`WRITE_TXN_MARKERS`) is a Kafka-internal coordinator-to-broker call that
we issue inside the process — it traverses our own localhost Kafka
listener so the serialisation is identical to the inter-broker case in
vanilla Kafka. The two observability APIs are trivial read-throughs over
the state table and ship alongside the core six.

## Transaction coordinator architecture

One `TransactionCoordinator` process-wide, always hosted on the Fluss
coordinator-leader. This mirrors the `GroupCoordinator` placement described
in design 0004 (hosted on each tablet-server, sharded by consistent-hash
over `group_id`); the difference is that transaction coordination is
global, not sharded, because (a) the traffic is low (each
`transactional.id` issues at most a handful of `END_TXN` calls per second
at peak, and `WRITE_TXN_MARKERS` fan-out is bounded by participating
partitions), (b) the state table is small (one row per `transactional.id`,
never more than a few thousand in any realistic deployment), and (c)
single-writer semantics on the state table eliminate the need for a
two-phase commit between coordinator shards. If this scales out badly we
can revisit — but consistent-hash sharding by `transactional.id` is a
drop-in change on top of the architecture below.

Placement uses the existing `CoordinatorLeaderBootstrap` SPI at
`fluss-server/src/main/java/org/apache/fluss/server/coordinator/spi/CoordinatorLeaderBootstrap.java:52`.
A new `TransactionCoordinatorBootstrap` sits in `fluss-kafka/` alongside
`SchemaRegistryBootstrap` (see
`fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryBootstrap.java:41`
for the exemplar). On leadership acquisition,
`CoordinatorServer.startLeaderBootstraps` (at
`fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java:347`)
instantiates it, which (a) opens a Fluss `Connection` to the local
cluster, (b) loads `__kafka_txn_state__` in one scan, rehydrating
in-memory `TransactionState` objects for every row not in terminal state,
(c) for any `PrepareCommit` / `PrepareAbort` row, resumes the marker
fan-out from where it left off, and (d) registers an RPC route so every
Kafka `FindCoordinator(type=TRANSACTION)` on any tablet-server returns the
coordinator-leader's Kafka listener endpoint. On leadership loss the
bootstrap's `close()` shuts down the in-memory state — the new leader
rebuilds it from the table.

Failover invariant: the only durable state is `__kafka_txn_state__`
(transaction lifecycle) and `__kafka_producer_ids__` (id allocator
high-water mark). Every state transition writes the table *before*
starting the next action; every action (marker fan-out, offset-commit
flush) is idempotent under replay. A new leader that inherits a
`PrepareCommit` row re-fans the markers; brokers that already saw the
earlier attempt treat the duplicate as a no-op (§11).

## State machine

The per-`transactional.id` state machine is faithful to Kafka's own —
deviating would break the client's retry logic. States live in-memory on
the coordinator-leader and are persisted to `__kafka_txn_state__` on
every transition.

```
Empty
  -> Ongoing           (on ADD_PARTITIONS_TO_TXN or ADD_OFFSETS_TO_TXN)
Ongoing
  -> PrepareCommit     (on END_TXN commit=true)
  -> PrepareAbort      (on END_TXN commit=false, or timeout)
PrepareCommit
  -> CompleteCommit    (after WRITE_TXN_MARKERS to all participating partitions)
  -> Empty             (cleanup, producer reusable)
PrepareAbort
  -> CompleteAbort
  -> Empty
```

| From | Trigger | To | Durable write before transition |
|---|---|---|---|
| `Empty` | `INIT_PRODUCER_ID` (same `transactional.id`, bump epoch) | `Empty` | yes — new epoch |
| `Empty` | `ADD_PARTITIONS_TO_TXN` | `Ongoing` | yes — partition set + start ts |
| `Ongoing` | `ADD_PARTITIONS_TO_TXN` (extend set) | `Ongoing` | yes — extended set |
| `Ongoing` | `ADD_OFFSETS_TO_TXN` (attach group) | `Ongoing` | yes — group added |
| `Ongoing` | `END_TXN(commit=true)` | `PrepareCommit` | yes — before any marker |
| `Ongoing` | `END_TXN(commit=false)` | `PrepareAbort` | yes |
| `Ongoing` | `transaction.timeout.ms` elapsed | `PrepareAbort` | yes |
| `PrepareCommit` | all markers ACKed | `CompleteCommit` | yes |
| `PrepareAbort` | all markers ACKed | `CompleteAbort` | yes |
| `CompleteCommit` | cleanup | `Empty` | yes — partition set cleared |
| `CompleteAbort` | cleanup | `Empty` | yes |

Invariants:

- **Durable-before-action.** The row write to `__kafka_txn_state__`
  happens before the action that follows the transition. If we crash
  after the write but before the action, replay will re-issue. If we
  crash before the write, the client's retry brings us back to the same
  pre-state and we re-evaluate idempotently.
- **Leader-only writes.** Only the coordinator-leader writes the state
  table. The `UpsertWriter` on `__kafka_txn_state__` is owned by the
  `TransactionCoordinator` singleton.
- **Epoch-gated transitions.** Every request carries `(producerId,
  epoch)`. A request whose epoch is below the stored epoch is rejected
  with `INVALID_PRODUCER_EPOCH` or `PRODUCER_FENCED` (§11). An equal
  epoch is the normal case. A higher epoch is impossible — the client
  never bumps unilaterally.

## Durable state entity — `_catalog.__kafka_txn_state__`

Table schema, mirroring the `FlussPkOffsetStore` pattern at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/group/FlussPkOffsetStore.java:48`
(one PK table per Kafka concept, registered via `SystemTables` at
`fluss-catalog/src/main/java/org/apache/fluss/catalog/SystemTables.java:42`,
accessed through the catalog service). The table lives in the reserved
`_catalog` database (see `SystemTables.DATABASE` at
`fluss-catalog/src/main/java/org/apache/fluss/catalog/SystemTables.java:38`):

```
CREATE TABLE _catalog.__kafka_txn_state__ (
  transactional_id       STRING   NOT NULL,
  producer_id            BIGINT   NOT NULL,
  producer_epoch         SMALLINT NOT NULL,
  state                  STRING   NOT NULL,     -- Empty|Ongoing|PrepareCommit|PrepareAbort|CompleteCommit|CompleteAbort
  topic_partitions       STRING,                -- encoded "topic:partition,topic:partition,..."
  group_ids              STRING,                -- encoded "g1,g2,..." (ADD_OFFSETS_TO_TXN targets)
  timeout_ms             INT      NOT NULL,
  txn_start_timestamp    TIMESTAMP_LTZ(3),      -- null when state = Empty
  last_updated_at        TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (transactional_id) NOT ENFORCED
)
DISTRIBUTED BY (16);
```

Design choices:

- **String-encoded partition set.** Kept as a CSV for the same reason
  `kafka.__consumer_groups__` (design 0004 §2) uses a JSON blob for
  membership: Fluss's row type does not yet support `SET<STRING>` as a
  first-class PK column, and the set is always read and written as a
  whole. Not a bottleneck — worst case is a few thousand partitions per
  transaction, well under the row-size limit.
- **One row per `transactional.id`, rewritten on every transition.** No
  history table. A Fluss PK table gives us MVCC-by-compaction; the state
  table is effectively a log-compacted topic by construction.
- **Write path: `UpsertWriter`.** Same as `FlussPkOffsetStore.commit()`
  (the `commit(...)` method at
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/group/FlussPkOffsetStore.java:60-75`
  is the template). We await the write future before acknowledging the
  client so there is never a window where the client believes a
  transition happened and the table hasn't caught up.
- **Read path on failover: full scan.** On leader-start, the coordinator
  issues one scan (via the Fluss client `Table.scan()`) over every row,
  rebuilds in-memory state. Txns in terminal states (`Empty`,
  `CompleteCommit`, `CompleteAbort`) are loaded as-is; txns in prepare
  states trigger marker fan-out resumption. This is cheap: a few
  thousand rows, one-shot per leader election.

Bootstrap: the table is created on first write via the same lazy pattern
as `FlussPkOffsetStore` (`openOrCreateTable` at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/group/FlussPkOffsetStore.java`
around the table-lock guard). Registered in
`SystemTables.all()` so tooling and admin queries see it.

## Producer-id allocator — real `INIT_PRODUCER_ID`

The existing handler at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:2186`
mints an `AtomicLong`-based id and returns epoch 0. Three paths need the
upgrade:

**Path 1 — `INIT_PRODUCER_ID` with a `transactional.id`, first call.**
The request travels `client → any tablet-server Kafka listener → routed
to coordinator-leader's Kafka listener`. On the leader the
`TransactionCoordinator` (a) checks `__kafka_txn_state__` for an
existing row; if absent, (b) allocates a new producer id from
`__kafka_producer_ids__` (see below), (c) writes a new row with
`state=Empty, producer_id=<new>, producer_epoch=0`, and (d) replies
with `(producerId, epoch=0)`.

**Path 2 — `INIT_PRODUCER_ID` with a `transactional.id`, same id
re-used.** On a re-init (crashed producer restarting with the same
`transactional.id`), the coordinator (a) reads the existing row, (b)
bumps `producer_epoch`, (c) if the row's state is `Ongoing`, drives it
through `PrepareAbort → CompleteAbort → Empty` *before* replying (so
the returning producer sees a clean slate — this is the fencing path),
(d) writes `state=Empty, producer_id=<unchanged>, producer_epoch=<+1>`,
(e) replies with `(producerId, epoch=<new>)`. Any in-flight request
from the old epoch (still-running rogue producer) is rejected with
`INVALID_PRODUCER_EPOCH` or `PRODUCER_FENCED` when it hits the broker.

**Path 3 — `INIT_PRODUCER_ID` without a `transactional.id`.** This is
the idempotent-only producer. The request does not route to the
txn-coord; any tablet-server can serve it. We allocate from a
session-local pool (an `AtomicLong` seeded from the global allocator,
bumped in blocks of 1000 per tablet-server — identical to how
`writerId` is allocated in `fluss-server` today). No durable state
required because an idempotent producer that crashes and re-inits
deserves a fresh id — Kafka's semantics too. The existing dedupe on
the broker side (batch `producerId + baseSequence`) still holds.

Allocation primitive: `_catalog.__kafka_producer_ids__`:

```
CREATE TABLE _catalog.__kafka_producer_ids__ (
  producer_id        BIGINT   NOT NULL,
  transactional_id   STRING,                    -- null for idempotent-only
  epoch              SMALLINT NOT NULL,
  allocated_at       TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (producer_id) NOT ENFORCED
)
DISTRIBUTED BY (16);
```

Allocation is a coordinator-leader-owned counter:
`AtomicLong nextProducerId`, seeded at bootstrap from
`SELECT MAX(producer_id) FROM _catalog.__kafka_producer_ids__` + 1. On
each allocation we (a) bump the counter, (b) write the row, (c) await
the write. Contention is trivial — transactional producers init at
most every few seconds.

**Alternative considered: ZooKeeper counter.** Fluss's coordinator
still uses ZK for leader election and a few metadata paths
(`ZooKeeperClient` is passed into `CoordinatorLeaderBootstrap.start`).
We could host the `nextProducerId` as a ZK sequential znode. Rejected:
the catalog-PK approach keeps all Kafka state in one place (the
reserved `_catalog` db) and composes with the same admin queries that
inspect `__kafka_txn_state__`. ZK znodes are a private
coordinator-server thing; bolt-ons don't reach into them.

## Transaction markers and the LSO — the one Fluss-core addition

This is the only part of the design that reaches below the
`fluss-kafka` bolt-on into `fluss-server` and `fluss-rpc`. Call it out
in every review.

`WRITE_TXN_MARKERS` is a Kafka-internal coordinator-to-broker RPC. In
vanilla Kafka it traverses the inter-broker listener. In our bolt-on
the coordinator is on the Fluss coordinator-leader and the
participating "brokers" are whichever tablet-servers own the leader
replica for each participating `(topic, partition)`. The fan-out
therefore traverses the Kafka listener on each tablet-server — same
serialisation, same authz gate — with the only wrinkle that the
`TransactionCoordinator` has to discover leader placement for each
participating partition, which it already does via
`MetadataManager` (available in the `CoordinatorLeaderBootstrap.start`
argument list).

**The marker batch.** A marker is a control batch with no records —
Kafka encodes this as a batch with the control bit set and a payload
of `(controlType: 0=abort|1=commit, coordinatorEpoch: int32)`. On
append the tablet-server writes it to the log like any other batch;
its offset is the LSO-advancing event.

**`LogTablet.lastStableOffset()` — the new cursor.** The log file
already tracks `highWatermark` as a `LogOffsetMetadata` field
(`fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java:108`,
accessor at line 240). We add a parallel `lastStableOffset` cursor
with the invariant: *`LSO ≤ highWatermark`, equal to the highest
offset that precedes every still-open transactional batch*. It is
maintained by a min-heap of `(producerId, firstOffset, baseSequence)`
tuples — one entry per open transactional batch on this partition.
Operations:

- **Append of a transactional data batch.** Insert the
  `(producerId, firstOffset, ...)` into the heap.
- **Append of a `WRITE_TXN_MARKERS(commit)`** for this producer. Remove
  the matching entry. LSO advances to
  `min(highWatermark, heap.peek().firstOffset)` — or to
  `highWatermark` if the heap is empty.
- **Append of `WRITE_TXN_MARKERS(abort)`.** Remove the matching entry
  and record the range `[firstOffset, markerOffset)` in an
  `abortedTransactions` skip-list on the replica. LSO advances as
  above. The skip-list survives on-disk via a sidecar file
  (`<partition>/.aborted`) — rebuilt on recovery by scanning markers
  in the local log from the oldest retained segment.

The heap and skip-list are per-`LogTablet` fields; they add O(n) memory
in the number of open txns (bounded by the coordinator's outstanding
txn count, which is tiny), and O(log n) cost on append and fetch.

**`FetchResult.lastStableOffset` — the new wire field.** Exposed via
`fluss-rpc/src/main/java/org/apache/fluss/rpc/entity/FetchLogResultForBucket.java:37`
(today it carries `highWatermark` but no LSO; add a sibling field). The
Kafka `FetchResponse` v4+ wire surface already has an `lastStableOffset`
field that is today wired to `highWatermark` in the Kafka-side
transcoder; once the Fluss field exists we flip that transcoder to
read it. Older fetch versions (v0–v3) are unaffected — the field is
version-gated on the Kafka side.

**Why this is the only Fluss-core edit.** Everything else in Phase J
can be additive inside `fluss-kafka/` and `fluss-catalog/`. The LSO
cursor cannot: it has to be maintained under the same lock that guards
log append, and it has to be snapshotted alongside the HWM at fetch
time. Attempting to maintain it in the Kafka bolt-on would either race
append or require a new Fluss-core hook that is strictly more invasive
than just adding the field. Budget: ~200 LOC in `LogTablet.java`, ~10
LOC in `FetchLogResultForBucket.java`, ~30 LOC in the Kafka-side
`FetchResponse` transcoder.

## Consumer isolation — `isolation.level=read_committed`

Two mechanisms:

1. **Truncate the fetch response at LSO.** When
   `isolation_level=READ_COMMITTED`, the Kafka Fetch transcoder clamps
   the returned records at `min(highWatermark, lastStableOffset)`. Any
   batch whose first offset is ≥ LSO is excluded. This is a cheap
   byte-offset truncate on the `LogRecords` already materialised from
   `FetchLogResultForBucket`.
2. **Filter aborted ranges.** For batches that lie below the LSO but
   were part of an aborted transaction, the fetch transcoder consults
   the replica's `abortedTransactions` skip-list (§7) and excludes
   batches whose `(producerId, baseSequence)` falls in an aborted
   range. The skip-list is keyed by `firstOffset` so the scan per
   fetch is O(log n + k) where k is the count of aborted batches in
   the fetched range — usually zero.

For `isolation.level=read_uncommitted` the behaviour is unchanged:
truncate at HWM, no filtering. This matches Kafka: a read-uncommitted
consumer sees data from in-flight and aborted txns.

`OffsetsForLeaderEpoch` and similar admin reads are unaffected — they
concern log offsets, not LSO.

## End-to-end flow — commit

```
Client  →  (any tablet-server)                    FindCoordinator(type=TRANSACTION, txn.id=t1)
                                                   → reply: coord-leader's Kafka endpoint
Client  →  coord-leader                           INIT_PRODUCER_ID(txn.id=t1)
                                                   coord writes __kafka_producer_ids__ row
                                                   coord writes __kafka_txn_state__: Empty, pid=101, epoch=0
                                                   → reply: pid=101 epoch=0
Client.beginTransaction()                          (client-local; no RPC)
Client  →  coord-leader                           ADD_PARTITIONS_TO_TXN(t1, pid=101, epoch=0,
                                                                          [alice:0, alice:1])
                                                   coord writes __kafka_txn_state__:
                                                     state=Ongoing, partitions=alice:0,alice:1
                                                   → reply: ok
Client  →  broker(alice:0)                        Produce(alice:0, isTransactional=true, pid=101, epoch=0)
Client  →  broker(alice:1)                        Produce(alice:1, isTransactional=true, pid=101, epoch=0)
                                                   each broker appends; heap entry added per batch;
                                                   LSO stays at (former HWM)
Client  →  coord-leader                           END_TXN(t1, pid=101, epoch=0, commit=true)
                                                   coord writes __kafka_txn_state__:
                                                     state=PrepareCommit
                                                   → reply: ok (async fan-out continues)
coord-leader → broker(alice:0)                    WRITE_TXN_MARKERS(alice:0, pid=101, epoch=0, COMMIT)
                                                   broker appends marker; heap entry removed;
                                                   LSO advances
coord-leader → broker(alice:1)                    WRITE_TXN_MARKERS(alice:1, pid=101, epoch=0, COMMIT)
                                                   → ack
                                                   coord writes __kafka_txn_state__:
                                                     state=CompleteCommit → Empty (partitions cleared)
```

## End-to-end flow — abort

Identical until `END_TXN`:

```
Client  →  coord-leader                           END_TXN(t1, pid=101, epoch=0, commit=false)
                                                   coord writes __kafka_txn_state__:
                                                     state=PrepareAbort
                                                   → reply: ok
coord-leader → broker(alice:0)                    WRITE_TXN_MARKERS(alice:0, pid=101, epoch=0, ABORT)
                                                   broker appends marker; heap entry removed;
                                                   abortedTransactions += [firstOffset, markerOffset)
                                                   LSO advances
coord-leader → broker(alice:1)                    WRITE_TXN_MARKERS(alice:1, pid=101, epoch=0, ABORT)
                                                   coord writes __kafka_txn_state__:
                                                     state=CompleteAbort → Empty
```

A `read_committed` consumer fetching `alice:0` after the abort sees the
LSO advance past both the data batch and the marker but filters the
data batch via the aborted-ranges list.

## Offset commits inside transactions — `TXN_OFFSET_COMMIT`

`TXN_OFFSET_COMMIT` is sent by the client directly to the group
coordinator broker (not the txn coordinator). The complication is that
the offset write must be atomic with the txn commit: if the txn
aborts, the offsets must not be visible to the consumer group. We
reuse the existing `FlussPkOffsetStore`
(`fluss-kafka/src/main/java/org/apache/fluss/kafka/group/FlussPkOffsetStore.java:78`)
but interpose a per-`(transactional.id, group_id)` buffer:

1. On `TXN_OFFSET_COMMIT` the broker (a) validates
   `(producerId, epoch)` against the txn coordinator via a lightweight
   lookup against `__kafka_txn_state__` (cached, short TTL), (b)
   stashes the offsets in an in-memory
   `TransactionalOffsetBuffer[(txn.id, group_id) → {topic, partition,
   offset}]`. Nothing goes to `__consumer_offsets__` yet.
2. On `WRITE_TXN_MARKERS(commit)` the broker flushes the buffer to
   `__consumer_offsets__` via the existing `UpsertWriter` path — the
   same code the `handleOffsetCommitRequest` hits today.
3. On `WRITE_TXN_MARKERS(abort)` the broker drops the buffer.

Buffer durability: the buffer is rebuilt on broker restart by reading
`__kafka_txn_state__` and replaying `TXN_OFFSET_COMMIT` batches from
the log — or, more simply, by persisting the buffer itself as rows in
a second PK table `_catalog.__kafka_txn_offset_buffer__`:

```
CREATE TABLE _catalog.__kafka_txn_offset_buffer__ (
  transactional_id   STRING   NOT NULL,
  group_id           STRING   NOT NULL,
  topic              STRING   NOT NULL,
  partition          INT      NOT NULL,
  offset             BIGINT   NOT NULL,
  metadata           STRING,
  producer_epoch     SMALLINT NOT NULL,
  buffered_at        TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (transactional_id, group_id, topic, partition) NOT ENFORCED
)
DISTRIBUTED BY (16);
```

On marker commit the broker (a) upserts each buffered offset into
`__consumer_offsets__`, (b) deletes the buffered rows. On marker
abort, (b) only. Net extra cost over non-transactional offset commit:
one PK-table round-trip on commit, one delete on abort — tolerable
given the per-txn commit rate.

## Failure modes

| Failure | Detection | Recovery |
|---|---|---|
| Producer crash mid-`Ongoing` | `transaction.timeout.ms` timer on coord (default 60 s, §14) | Coord transitions `Ongoing → PrepareAbort`; normal abort path |
| Client re-inits with same `transactional.id` | `INIT_PRODUCER_ID` on existing row | Coord aborts any `Ongoing` txn; bumps epoch; old producer's next request fails with `INVALID_PRODUCER_EPOCH` |
| Coord-leader failover during `Ongoing` | New leader scans `__kafka_txn_state__` | Client's next RPC re-routes via `FindCoordinator`; state is intact; client retries |
| Coord-leader failover during `PrepareCommit` / `PrepareAbort` | As above, plus partial marker fan-out | New leader re-issues `WRITE_TXN_MARKERS` to every participating partition; markers are idempotent (same `pid + epoch + state`) |
| Broker crash during marker append | `WRITE_TXN_MARKERS` future never completes | Coord retries; on the re-elected replica the marker is either already present (log-level idempotence via `pid + epoch`) or it appends fresh |
| Rogue old-epoch producer | Request hits broker with stale epoch | Broker rejects with `INVALID_PRODUCER_EPOCH`; client-side Kafka producer treats this as terminal |
| `__kafka_txn_state__` write fails | Coord catches `ExecutionException` on `UpsertWriter` future | Coord replies `COORDINATOR_NOT_AVAILABLE`; client retries after `FindCoordinator` refresh |

The fencing rule is tight: once the coord bumps a `transactional.id`'s
epoch, every broker that sees the old epoch must reject. Brokers learn
the new epoch lazily (from the first request that carries it, or from
a `WRITE_TXN_MARKERS` that references it) and cache it on the
`LogTablet`'s per-partition producer-state tracker. Stale requests
from the pre-bump producer hit this cache and fail fast.

`PRODUCER_FENCED` vs `INVALID_PRODUCER_EPOCH`: we return
`PRODUCER_FENCED` when the bump came from a deliberate client re-init
(the common KStreams case) and `INVALID_PRODUCER_EPOCH` when the bump
came from a coordinator-side timeout. Kafka clients handle both the
same way but the distinction is useful in observability.

## Observability — `DESCRIBE_TRANSACTIONS` and `LIST_TRANSACTIONS`

Both are trivial reads over `__kafka_txn_state__`:

- `LIST_TRANSACTIONS` → scan the full table, project
  `(transactional_id, state, producer_id, last_updated_at)`, filter by
  the request's state predicate.
- `DESCRIBE_TRANSACTIONS(txn.id)` → `Lookuper.lookup((txn.id))` on the
  table, decode the full row including partition set and timeout, format
  as the Kafka `DescribeTransactionsResponseData` shape.

Both ship alongside the core six. Cheap to implement (~150 LOC
combined) and invaluable for operator confidence. They route to the
txn coord (via `FindCoordinator`) but any tablet-server could serve
them off the same table — we route through the coord just to keep the
fan-in simple.

## Authorization

Every transactional API goes through the existing authorizer gate (see
design 0010). One new `ResourceType` value is required:

```java
public enum ResourceType {
    ANY((byte) 1),
    CLUSTER((byte) 2),
    DATABASE((byte) 3),
    TABLE((byte) 4),
    GROUP((byte) 5),
    TRANSACTIONAL_ID((byte) 6);   // new
    ...
}
```

Located at
`fluss-common/src/main/java/org/apache/fluss/security/acl/ResourceType.java:54`
— a one-line addition plus the byte-code constant. Like `GROUP` (see
`ResourceType.java:66`) this is a direct child of `CLUSTER`, so
cluster-wide grants roll up to it. The authorizer needs no logic
changes — the hierarchy rule applies uniformly.

Per-API authz:

| API | Op | Resource |
|---|---|---|
| `INIT_PRODUCER_ID` (with `transactional.id`) | `WRITE` | `TRANSACTIONAL_ID(name)` |
| `INIT_PRODUCER_ID` (no `transactional.id`) | `WRITE` | `CLUSTER` |
| `ADD_PARTITIONS_TO_TXN` | `WRITE` | `TABLE(topic)` + `WRITE` on `TRANSACTIONAL_ID(name)` |
| `ADD_OFFSETS_TO_TXN` | `READ` | `GROUP(group_id)` + `WRITE` on `TRANSACTIONAL_ID(name)` |
| `END_TXN` | `WRITE` | `TRANSACTIONAL_ID(name)` |
| `WRITE_TXN_MARKERS` | inter-broker | `CLUSTER_ACTION` (existing) |
| `TXN_OFFSET_COMMIT` | `READ` | `GROUP(group_id)` + `WRITE` on `TRANSACTIONAL_ID(name)` |
| `DESCRIBE_TRANSACTIONS` | `DESCRIBE` | `TRANSACTIONAL_ID(name)` |
| `LIST_TRANSACTIONS` | `DESCRIBE` | `CLUSTER` |

This is the second Fluss-core touch for Phase J — one enum value plus
one byte. Identical in shape to the `GROUP` addition in Phase G.2
described in design 0010.

## Configuration

All new options live under the `kafka.transaction.*` namespace and
follow the `ConfigBuilder` pattern (see the `ConfigBuilder` reference
at `fluss-common/src/main/java/org/apache/fluss/config/ConfigBuilder.java`).

| Key | Type | Default | Purpose |
|---|---|---|---|
| `kafka.transaction.max-timeout.ms` | int | 900000 | Upper bound on client-requested `transaction.timeout.ms`. Kafka's default. |
| `kafka.transaction.default-timeout.ms` | int | 60000 | Applied when the client sends 0. |
| `kafka.transaction.coordinator.refresh.interval.ms` | int | 10000 | How often the coord-leader scans `__kafka_txn_state__` for timed-out txns. |
| `kafka.transaction.producer-id.block-size` | int | 1000 | How many ids each tablet-server pre-allocates from the central counter for idempotent-only producers. |
| `kafka.transaction.marker.fanout.parallelism` | int | 16 | Max concurrent `WRITE_TXN_MARKERS` RPCs per `END_TXN`. |

Defaults match Kafka's out of the box so existing client tuning
transfers.

## Files

Add (new files unless noted):

- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionCoordinator.java`
  — the singleton, owns in-memory state, drives the state machine.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionState.java`
  — the per-`transactional.id` record (enum + fields), serialised to
  the PK table.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionCoordinatorBootstrap.java`
  — implements
  `org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap`;
  wired via `META-INF/services/`.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/KafkaTxnStateStore.java`
  — PK-table wrapper around `__kafka_txn_state__`, same shape as
  `FlussPkOffsetStore`.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/KafkaProducerIdStore.java`
  — PK-table wrapper around `__kafka_producer_ids__`, holds the
  `nextProducerId` counter.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/KafkaTxnOffsetBufferStore.java`
  — PK-table wrapper around `__kafka_txn_offset_buffer__`.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionalOffsetBuffer.java`
  — in-memory `(txn.id, group_id) → offsets` buffer, write-through to
  the store.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TxnTimeoutReaper.java`
  — scheduled executor that scans for timed-out `Ongoing` txns and
  drives them into `PrepareAbort`.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TxnMarkerWriter.java`
  — fans out `WRITE_TXN_MARKERS` to participating brokers with the
  configured parallelism.
- `fluss-catalog/src/main/java/org/apache/fluss/catalog/entities/KafkaTxnStateEntity.java`
  — the `CatalogService`-level entity for the txn-state row.
- `fluss-catalog/src/main/java/org/apache/fluss/catalog/entities/KafkaProducerIdEntity.java`
  — the producer-ids entity.
- `fluss-catalog/src/main/java/org/apache/fluss/catalog/SystemTables.java`
  — add three new `Table` constants
  (`KAFKA_TXN_STATE`, `KAFKA_PRODUCER_IDS`, `KAFKA_TXN_OFFSET_BUFFER`)
  and list them in `all()`.

Modify:

- `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java`
  — replace the stubbed `handleInitProducerIdRequest` at line 2186
  with the three-path implementation described in §6; add
  `handleAddPartitionsToTxn`, `handleAddOffsetsToTxn`, `handleEndTxn`,
  `handleWriteTxnMarkers`, `handleTxnOffsetCommit`,
  `handleDescribeTransactions`, `handleListTransactions`; add the six
  new `ApiKeys.*` values to `IMPLEMENTED_APIS` (lines 199 + 319 area);
  route each new API in the dispatch switch at line 500+ (currently
  falling through to `handleUnsupportedRequest` at line 602).
- `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
  — add `lastStableOffset` cursor and the producer-state tracker
  (min-heap + aborted-ranges skip-list); update on every append; snapshot
  at fetch time.
- `fluss-server/src/main/java/org/apache/fluss/server/log/LocalLog.java`
  — sidecar `.aborted` file read/write for crash-recovery of the skip-list.
- `fluss-rpc/src/main/java/org/apache/fluss/rpc/entity/FetchLogResultForBucket.java`
  — add `lastStableOffset` field parallel to `highWatermark` at line 37;
  new constructor overloads.
- `fluss-common/src/main/java/org/apache/fluss/security/acl/ResourceType.java`
  — add `TRANSACTIONAL_ID((byte) 6)` value.
- `fluss-common/src/main/java/org/apache/fluss/security/acl/Resource.java`
  — factory method for `transactionalId(String name)`.
- `fluss-server/src/main/java/org/apache/fluss/server/authorizer/DefaultAuthorizer.java`
  — hierarchy rule for `TRANSACTIONAL_ID` (child of `CLUSTER`, no
  sub-resources).
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/auth/KafkaAuthorizerBridge.java`
  — map Kafka's `TRANSACTIONAL_ID` resource type onto the new Fluss
  enum value.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/produce/ProduceHandler.java`
  — plumb `isTransactional` + `(pid, epoch)` into the append path so
  the `LogTablet` producer-state tracker sees them.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/FetchHandler.java`
  — honour `isolation_level`; truncate at LSO and filter aborted ranges
  when `READ_COMMITTED`.
- `META-INF/services/org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap`
  inside `fluss-kafka/` — add `TransactionCoordinatorBootstrap`.

Integration tests (new files under
`fluss-kafka/src/test/java/org/apache/fluss/kafka/`):

- `tx/KafkaTransactionalProducerITCase.java`
- `tx/KafkaTxnFailoverITCase.java`
- `tx/KafkaReadCommittedITCase.java`
- `tx/KafkaTxnOffsetCommitITCase.java`
- `tx/KafkaDescribeTransactionsITCase.java`

## Verification

Each IT covers one concrete invariant:

- **Basic commit** (`KafkaTransactionalProducerITCase.testBasicCommit`)
  — begin → produce 100 records across 2 partitions → commit; a
  `read_committed` consumer reads all 100.
- **Basic abort**
  (`KafkaTransactionalProducerITCase.testBasicAbort`) — begin → produce
  100 → abort; a `read_committed` consumer reads 0; a
  `read_uncommitted` consumer reads 100. Confirms filtering works in
  both directions.
- **Fencing on re-init**
  (`KafkaTransactionalProducerITCase.testFencing`) — two producers with
  the same `transactional.id`; the second init fences the first; any
  subsequent send from the first fails with `ProducerFencedException`.
- **Coordinator failover mid-txn**
  (`KafkaTxnFailoverITCase.testFailoverMidOngoing`) — begin → produce
  → kill coord-leader → elect new leader → commit; a `read_committed`
  consumer reads all records. Verifies `__kafka_txn_state__` rehydration
  and marker fan-out resumption from `PrepareCommit`.
- **Coordinator failover mid-prepare**
  (`KafkaTxnFailoverITCase.testFailoverMidPrepareCommit`) — inject a
  failure between `PrepareCommit` write and completed marker fan-out;
  new leader completes the fan-out; markers are idempotent.
- **Exactly-once Kafka Streams**
  (`KafkaStreamsExactlyOnceITCase`, covered in design 0017's
  Streams-focused addendum) — a streams app with
  `processing.guarantee=exactly_once_v2` processes 10 000 records with
  one forced restart mid-pipeline; downstream count equals upstream
  count.
- **`DescribeTransactions` over lifecycle**
  (`KafkaDescribeTransactionsITCase.testLifecycle`) — issue
  `DescribeTransactions(t1)` at each state transition (Empty →
  Ongoing → PrepareCommit → Empty) and assert the reported state
  matches.
- **`TXN_OFFSET_COMMIT` under `read_committed`**
  (`KafkaTxnOffsetCommitITCase.testCommitVsAbort`) — a txn that
  commits offsets and aborts: the group sees nothing; a txn that
  commits offsets and commits: the group sees the offsets exactly
  once.
- **LSO monotonicity** (`KafkaReadCommittedITCase.testLsoMonotonic`)
  — randomized property test: generate interleaved commit / abort
  sequences across partitions, assert LSO is monotonic non-decreasing
  on every fetch and never exceeds HWM.
- **Producer-id stability across coord restart**
  (`KafkaTxnFailoverITCase.testProducerIdStable`) — same
  `transactional.id` before and after coord-leader cycle retains the
  same `producer_id` but a bumped epoch.

All tests use randomized inputs (record counts, payload sizes, txn
sizes) per the project's no-hardcoded-happy-paths rule. None uses
`@Timeout`.

## Sub-phases / PRs

Phase J ships in three sub-PRs that are each independently reviewable
and each lands a passing IT set.

**J.1 — coordinator skeleton + real `INIT_PRODUCER_ID`.** The
`TransactionCoordinator`, its bootstrap, `__kafka_txn_state__` and
`__kafka_producer_ids__` system tables, the `TRANSACTIONAL_ID`
resource type, and the rewritten `INIT_PRODUCER_ID` handler. No other
txn APIs yet — they still fall through to
`handleUnsupportedRequest`. Demonstrates the coordinator stands up,
fails over, and allocates ids correctly. Scope: ~900 LOC + 3 ITs.

**J.2 — txn APIs end-to-end against a single partition.** The five
client-facing APIs (`ADD_PARTITIONS_TO_TXN`, `ADD_OFFSETS_TO_TXN`,
`END_TXN`, `WRITE_TXN_MARKERS`, `TXN_OFFSET_COMMIT`) plus the two
observability APIs. No LSO yet — `read_committed` consumers see the
same records as `read_uncommitted` (abort filtering is a no-op).
Demonstrates the commit path works; aborts silently succeed on the
coord side but aren't yet visible to consumers. Scope: ~1000 LOC + 4
ITs.

**J.3 — LSO + `read_committed` + marker recovery.** The `LogTablet`
cursor, `FetchLogResultForBucket.lastStableOffset` field, the aborted
ranges skip-list with sidecar persistence, the fetch-path filtering,
and the failover ITs. Completes the phase. Scope: ~600 LOC + 3 ITs.

## Scope

~2500 LOC additive across three PRs, almost entirely in
`fluss-kafka/` and `fluss-catalog/`. Two small Fluss-core edits
(LSO cursor on `LogTablet`, `TRANSACTIONAL_ID` enum value) total
under 300 LOC and are called out explicitly in the J.3 PR
description.

## Out of scope

- **SASL-only listener restrictions.** The txn APIs don't change per
  listener semantics; they go through the same
  `KafkaAuthorizerBridge` gate as every other API. No listener
  plumbing changes.
- **`fetch.min.bytes` + LSO interaction tuning.** With
  `isolation.level=read_committed` a fetch may be smaller than
  `fetch.min.bytes` because records past the LSO are excluded.
  Kafka's behaviour is to still return the partial response; we match
  that. Throughput tuning for this case is deferred.
- **Multi-cluster transactions (MirrorMaker XA).** Kafka never
  supported distributed transactions across clusters; nor do we.
  `transactional.id` is scoped to one Fluss cluster.
- **`DescribeProducers` cross-referenced from txn state.** The
  existing `DescribeProducers` handler
  (`KafkaRequestHandler.handleDescribeProducersRequest`) reads
  `ReplicaManager.getProducerStates` and is unchanged. Joining it
  against `__kafka_txn_state__` is a follow-on admin UX improvement,
  not a correctness concern.
- **KIP-890 (consumer fencing from the txn coordinator side).** A
  Kafka 3.8+ proposal. Not in 3.9's baseline; defer with the rest of
  the post-3.9 surface.
- **Compacted `__kafka_txn_state__` via Fluss lake-tiering.** The
  table is small enough (< 1M rows per realistic cluster) that
  native PK-table compaction is sufficient. No lake side-outputs.

---

## Appendix A — cross-references to the existing codebase

- Stub being replaced: `handleInitProducerIdRequest` at
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:2186`;
  the `STUB_PRODUCER_ID` counter at line 234.
- Fall-through for unsupported txn APIs:
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:602`
  (the `default:` branch of the dispatch switch).
- Group-coordinator bootstrap exemplar (pattern we copy):
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryBootstrap.java:41`.
- Bootstrap SPI contract: `CoordinatorLeaderBootstrap` at
  `fluss-server/src/main/java/org/apache/fluss/server/coordinator/spi/CoordinatorLeaderBootstrap.java:52`.
- Bootstrap discovery + start loop:
  `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java:347`.
- System-table registration pattern:
  `fluss-catalog/src/main/java/org/apache/fluss/catalog/SystemTables.java:42`
  (see `NAMESPACES` definition for the exemplar, and `all()` at line 166
  for the bootstrap list).
- PK-table store pattern:
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/group/FlussPkOffsetStore.java:78`
  (structure), line 91 (cache), and the `commit(...)` method for the
  upsert-and-await shape.
- High-watermark accessor we parallel with LSO:
  `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java:240`
  (`getHighWatermark()`), field at line 108.
- Fetch result wire entity to extend:
  `fluss-rpc/src/main/java/org/apache/fluss/rpc/entity/FetchLogResultForBucket.java:37`
  (the `highWatermark` field — add LSO alongside).
- `ResourceType` to extend with `TRANSACTIONAL_ID`:
  `fluss-common/src/main/java/org/apache/fluss/security/acl/ResourceType.java:54`
  (enum body starts here; `GROUP` at line 66 is the exemplar for a
  cluster-child resource type).
- Existing `FindCoordinator` routing on which we piggyback for
  `coordinator-type=TRANSACTION`: `handleFindCoordinatorRequest` at
  `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:524`.
