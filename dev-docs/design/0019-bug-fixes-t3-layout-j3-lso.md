# Design 0019 — Bug Fixes: Typed Table Evolution (T.3) & Read-Committed Visibility (J.3)

**Status:** In progress.
**Owner:** Ben Gamble
**Supersedes:** nothing.
**Related:** `0015-kafka-typed-tables-alter-on-register.md`, `0016-kafka-transactions.md`,
`0018-kafka-streams-and-connect-demanding-workloads.md`.

## 1. Context

Doc 0018 Workstream B ran the full IT suite against the already-coded phases and
surfaced two bugs, both `@Disabled` with short inline notes. This doc replaces
those notes with a full root-cause analysis, exact file and line citations, fix
options with tradeoffs, and a verification plan.

Both bugs block Workstream C: `KafkaStreamsExactlyOnceITCase` waits on J.3,
`KafkaStreamsWithTypedTablesITCase` waits on T.3. Neither fix requires changes
to `fluss-common` or `fluss-server`.

---

## 2. T.3 — Typed Table Shape Evolution Layout Bug

### 2.1 Symptom

`KafkaTypedAlterITCase` has 7 scenarios. 3 pass (first-register only). 4 are
`@Disabled` at lines 209, 237, 265, and 365 with the message:

> "T.3 layout bug — TypedTableEvolver emits AddColumn(LAST) but reserved columns
> (event_time, headers) need to come after user fields."

### 2.2 Root cause

`TypedKafkaFetchCodec` hardcodes passthrough-shape column indices:

```java
// TypedKafkaFetchCodec.java lines 67-71
private static final int COL_RECORD_KEY = 0;
private static final int COL_PAYLOAD    = 1;
private static final int COL_EVENT_TIME = 2;
private static final int COL_HEADERS    = 3;
```

These are correct for the pre-evolution 4-column passthrough layout
`[record_key, payload, event_time, headers]`. After T.3 reshapes the table to
`[record_key, ...userCols, event_time, headers]`, `event_time` and `headers` are
at positions `N+1` and `N+2`. The codec reads from the wrong field offsets and
either panics with an index-out-of-bounds or silently returns corrupt bytes.

The comment at lines 147–153 of `TypedKafkaFetchCodec.java` acknowledges this
is a T.2-interim hack — the codec was frozen at passthrough positions to unblock
T.2, with a note that T.3 must update the indices. The update was never made.

### 2.3 Why the evolve path does not fix the ordering itself

`TypedTableEvolver.evolveTyped()` (lines 334–369) does the right thing for the
`TableChange` objects it emits. `stripReserved()` (lines 488–510) extracts only
user columns from the current schema (`subList(1, all.size() - 2)`), diffs
against the new SR schema, and emits `AddColumn(LAST)` for each new user field.
`SchemaUpdate.addColumn()` in `fluss-server` enforces `ColumnPosition.last()`.

The column _ordering_ in the Fluss table is correct — new user columns land
before the reserved suffix. The bug is entirely in the codec reading stale
positional constants, not in the table structure.

### 2.4 Disabled scenarios

| Scenario | Lines | What it tests |
|---|---|---|
| `evolveAddNullableEmailExtendsTypedShape` | 209–235 | v1 `{id, name}` → v2 adds nullable `email`; round-trip fetch after reshape |
| `evolveRenameIsRejected` | 237–263 | v1 → v2 (adds `email`) → v3 renames `id` to `user_id`; expects 422 reject |
| `evolveNonNullAddIsRejected` | 265–289 | v1 → v2 (adds `email`) → v3 adds non-null `score`; expects 422 reject |
| `confluentIdPreservedAcrossAdditiveAlter` | 365–386 | v1 → v2 (adds `email`); verify v1 Confluent schema id still resolves on fetch |

All four depend on the v1 → v2 round-trip succeeding. Because the codec corrupts
the fetch after reshape, none can pass.

### 2.5 Fix options

**Option 1 — Dynamic indices (recommended)**

Update `TypedKafkaFetchCodec` to resolve `event_time` and `headers` column
positions by name from the live table schema at codec-construction time rather
than using hardcoded constants. Store the resolved indices as instance fields.
Re-construct (or re-resolve) when the table schema version changes.

- Scope: `TypedKafkaFetchCodec.java` only (~20 lines changed)
- No fluss-core edits
- Correct for both passthrough-shape and typed-shape tables
- Historical records produced under v1 remain readable because the codec is
  keyed to the table schema version at fetch time

**Option 2 — Re-compile codec on reshape**

`TypedTableEvolver` invalidates the compiled codec entry in `CompiledCodecCache`
immediately after `alterTable()` completes. The next fetch triggers a fresh
`AvroCodecCompiler`/`JsonCodecCompiler`/`ProtobufCodecCompiler` compilation with
the new table schema, picking up the correct indices.

- Scope: `TypedTableEvolver.java` + `CompiledCodecCache.java` (~30 lines)
- More indirection; cache invalidation must be reliable
- Preferable if other callers also depend on a single source of truth in the cache

**Option 3 — Defer evolution (no code change)**

Document that only first-register works. Additive evolution requires a table
drop until the codec lifecycle is addressed in a later phase.

- Scope: zero
- Workstream C.5 stays blocked indefinitely

**Recommendation:** Option 1. Smallest change, no core edits, correct behavior
for both passthrough and evolved table shapes without introducing cache lifecycle
complexity.

### 2.6 Key files

| File | Lines | Role |
|---|---|---|
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/TypedKafkaFetchCodec.java` | 67–71, 102–110 | Hardcoded indices; read path |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/TypedTableEvolver.java` | 334–369, 467–478, 488–510 | `evolveTyped()`, AddColumn delta, `stripReserved()` |
| `fluss-server/src/main/java/org/apache/fluss/server/coordinator/SchemaUpdate.java` | 75–76 | Enforces `AddColumn(LAST)` |
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTypedAlterITCase.java` | 209–386 | 4 disabled scenarios |

---

## 3. J.3 — Read-Committed Transaction Visibility (LSO Race)

### 3.1 Symptom

`KafkaReadCommittedITCase` is entirely `@Disabled` at lines 138–146 with the
message:

> "Phase J.3 read_committed visibility bug — produced 13 records, consumer reads
> 0. Either KafkaFetchTranscoder truncates at LSO too aggressively before the
> control-batch marker write completes, or the marker fan-out from
> TransactionCoordinator.endTxn(commit) doesn't update the in-memory
> lastStableOffset on time."

The test is **not a flake**. It reproduces reliably because the race window is
wide: the consumer starts polling before `endTxn()` returns, and marker fan-out
is asynchronous.

### 3.2 Root cause — LSO race between fetch and marker append

`lastStableOffset` in `LogTablet` is computed as:

```java
// LogTablet.java lines 286-295
public long lastStableOffset() {
    long hwm = getHighWatermark();
    synchronized (lock) {
        OpenTxn oldest = openTxnHeap.peek();
        if (oldest == null) {
            return hwm;
        }
        return Math.min(hwm, oldest.firstOffset);
    }
}
```

The `openTxnHeap` entry for a transaction is:
- **added** when the first data batch is appended (`trackTransactionalBatches()`,
  line 963, called while holding the tablet lock)
- **removed** when the commit control-batch marker is appended (`applyMarker()`,
  line 1040, also holding the tablet lock)

The fetch path reads LSO at `ReplicaManager.java` line 1467 **without holding
the tablet lock**:

```java
long lastStableOffset = replica.getLogLastStableOffset();
```

`KafkaFetchTranscoder.toPartitionData()` (lines 353–400) then uses that snapshot
as the ceiling:

```java
long ceiling = readCommitted ? Math.min(hwm, lastStableOffset) : hwm;
// line 436: if (batch.baseLogOffset() >= ceiling) continue;
```

### 3.3 Exact event sequence that produces 0 records

1. Producer sends 13 transactional records. Data batches appended; HWM advances
   to `firstOffset + 13`. `openTxnHeap` populated with `OpenTxn(writerId,
   firstOffset)`.
2. LSO = `min(HWM, heap.peek().firstOffset)` = `firstOffset`. (Heap not empty.)
3. **Consumer fetches here.** Ceiling = `min(HWM, LSO)` = `firstOffset`. All 13
   data batches have `baseLogOffset >= firstOffset` → all excluded → 0 records.
4. `endTxn(commit)` calls `applyMarkersAndComplete()` (line 684), which fans out
   marker writes asynchronously and returns to the caller.
5. Marker batch appended to tablet → `applyMarker()` removes from heap → LSO
   advances to HWM.
6. **Next consumer fetch** sees ceiling = HWM → 13 records visible.

The window between steps 1 and 5 is the race. Because `applyMarkersAndComplete()`
returns its futures without awaiting them (lines 743–758), `endTxn()` replies to
the producer **before markers are durably written**. The consumer, already
polling, catches the window.

### 3.4 Why `KafkaTransactionalProducerITCase` is not affected

`KafkaTransactionalProducerITCase.java` line 72 explicitly states that read-side
filtering is out of scope for that suite. It tests only wire-level success and
coordinator state machine transitions (`Empty → Ongoing → PrepareCommit →
CompleteCommit → Empty`). No `read_committed` fetch is ever issued.

### 3.5 Fix options

**Option 1 — Synchronous marker fan-out (recommended)**

In `TransactionCoordinator.applyMarkersAndComplete()` (lines 722–758), collect
the `CompletableFuture` returned by each `sink.append()` call and await all
futures before returning. `endTxn()` then only replies to the producer after all
markers are durably written and LSO has advanced on every participating tablet.

```java
// current: fire-and-forget
sink.append(tp, writerId, epoch, commit);

// fix: collect and join
List<CompletableFuture<Void>> markerFutures = new ArrayList<>();
for (TopicPartition tp : partitions) {
    markerFutures.add(sink.append(tp, writerId, epoch, commit));
}
FutureUtils.completeAll(markerFutures).join();
```

- Scope: `TransactionCoordinator.java` lines 722–758 (~15 lines)
- Matches Kafka broker semantics (blocking commit)
- Eliminates the race entirely; no lock changes on the fetch path
- Marker fan-out is bounded by participating partition count (small)
- No throughput regression on the hot fetch path

**Option 2 — Lock fetch-path LSO read**

Hold the `LogTablet` lock at `ReplicaManager.java` line 1467 when reading LSO.
Fetch always sees LSO consistent with the latest heap state.

- Scope: `ReplicaManager.java` + `LogTablet.java` (~5 lines)
- Adds lock contention between every concurrent fetch and marker append
- Correct but heavier; Option 1 is cheaper

**Recommendation:** Option 1. Blocking commit is the right semantic contract and
avoids touching the hot fetch path.

### 3.6 Key files

| File | Lines | Role |
|---|---|---|
| `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java` | 286–295, 963, 1033–1046 | `lastStableOffset()`, `trackTransactionalBatches()`, `applyMarker()` |
| `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java` | 1467 | Fetch reads LSO snapshot without tablet lock |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/KafkaFetchTranscoder.java` | 353–400, 436 | LSO ceiling computation and batch exclusion |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionCoordinator.java` | 644–687, 722–758 | `endTxn()`, async `applyMarkersAndComplete()` |
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaReadCommittedITCase.java` | 138–146, 149–239 | `@Disabled` annotation + 3 scenarios |

---

## 4. Verification plan

### 4.1 T.3 — before / after

| | Count |
|---|---|
| Passing before fix | 3 / 7 |
| Passing after fix | 7 / 7 |

Scenarios that must go green:

- `evolveAddNullableEmailExtendsTypedShape` — produce under v1, register v2,
  fetch under v2, assert `email` field decoded correctly
- `evolveRenameIsRejected` — v3 rename attempt returns 422
- `evolveNonNullAddIsRejected` — v3 non-null add returns 422
- `confluentIdPreservedAcrossAdditiveAlter` — v1 Confluent id resolves correctly
  on fetch after v2 reshape

Run: `./mvnw test -Dtest=KafkaTypedAlterITCase -pl fluss-kafka`

### 4.2 J.3 — before / after

| | Count |
|---|---|
| Passing before fix | 0 / 3 |
| Passing after fix | 3 / 3 |

Scenarios that must go green:

- Committed records visible to `read_committed` consumer
- Aborted records hidden from `read_committed`, visible to `read_uncommitted`
- Mixed: aborted batch followed by committed batch — only committed batch visible

Run: `./mvnw test -Dtest=KafkaReadCommittedITCase -pl fluss-kafka`

### 4.3 Downstream unlocks (Doc 0018 Workstream C)

After both fixes land:

| Workstream | Unblocked by |
|---|---|
| C.4 — Streams exactly-once | J.3 fix |
| C.5 — Streams typed-tables | T.3 fix |

---

## 5. Files to change

### T.3 fix

| File | Change |
|---|---|
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/TypedKafkaFetchCodec.java` | Replace hardcoded `COL_*` constants with schema-derived dynamic index lookup |
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTypedAlterITCase.java` | Remove `@Disabled` from 4 methods |

### J.3 fix

| File | Change |
|---|---|
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/tx/TransactionCoordinator.java` | Await all marker futures in `applyMarkersAndComplete()` before returning |
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaReadCommittedITCase.java` | Remove `@Disabled` |

No changes to `fluss-common`, `fluss-server`, or any other module.

---

## 6. Out of scope

- Multi-format re-typing (`AVRO → PROTOBUF` after first registration) — deferred,
  requires a separate `ReplaceColumn` TableChange variant
- `isolation.level` tuning (`fetch.min.bytes`, LSO-aware batching) — post-fix
  performance work
- Lake-tiering compaction for `__kafka_txn_state__` — separate initiative
