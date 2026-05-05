# Design 0015 — Kafka typed tables: ALTER-on-register (Phase T.3)

**Status:** Draft, 2026-04-24
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0009-multi-format-registry.md`, `0014-kafka-typed-table-produce-fetch.md`

## 1. Context

Design 0002 established the Fluss-first Schema Registry and the two-state
lifecycle of a Kafka topic's backing table: `KAFKA_PASSTHROUGH` (the default
four-column shape from `KafkaDataTable.schema()`) and `KAFKA_TYPED_<FORMAT>`
(shape derived from a registered Avro / JSON Schema / Protobuf schema).
Design 0009 extended the translator SPI to cover all three day-one formats.
Phase T.2 (doc 0014) plumbed typed codec compilation through the produce
and fetch hot paths, keyed on `__tables__.format`. T.2's work is latent
until something moves a topic out of `KAFKA_PASSTHROUGH`: no typed codec
will ever be selected while the catalog row still says the table is
passthrough-shaped. That is the gap this document closes.

Phase T.3 makes `POST /subjects/{topic}-value/versions` the edge that
reshapes the data table. The first registration transitions the catalog
format from `KAFKA_PASSTHROUGH` to `KAFKA_TYPED_<FORMAT>` and replaces the
opaque `payload BYTES` column with typed `f1..fN` columns derived from the
translator output. Subsequent registrations on the same topic extend the
typed table under strict additive-nullable rules, preserving the live
column layout so historical SR schema ids keep resolving. The central
question this doc answers is whether we need a new `TableChange` variant
in `fluss-common` (e.g. `ReplaceColumn`) to express that reshape, or
whether existing `AddColumn` / `DropColumn` / `ModifyColumn` /
`RenameColumn` plus a careful first-registration path are enough to meet
the "zero fluss-core edits" goal.

## 2. Status at time of writing

`SchemaRegistryService.register(String subject, String schemaText, String
formatId)` at `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryService.java:327`
performs — in order — subject-tombstone clearing
(`SchemaRegistryService.java:341`), idempotent match against the latest
live version (`SchemaRegistryService.java:347`), tombstoned-version
resurrection (`SchemaRegistryService.java:363`), compatibility gate
(`SchemaRegistryService.java:374`), creation of the catalog namespace and
`KAFKA_PASSTHROUGH`-shaped table row if absent
(`SchemaRegistryService.java:376`, flowing into `ensureCatalogEntities`
at `SchemaRegistryService.java:893`), `catalog.registerSchema(...)` which
appends a `SchemaVersionEntity` and mints a SR schema id
(`SchemaRegistryService.java:377`), and finally `catalog.bindKafkaSubject`
to link the subject to the table row (`SchemaRegistryService.java:384`).

Nothing in that chain touches the Fluss data table's columns. The
`KAFKA_PASSTHROUGH`-shape rowtype built by `KafkaDataTable.schema()` at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/metadata/KafkaDataTable.java:56`
is created exactly once — when the Kafka admin path creates the topic —
and from that point on the registration flow only mutates catalog entity
tables. The consequence is that a topic with a registered Avro schema and
no reshape is still backed by `(record_key, payload, event_time, headers)`
rows, so T.2's typed-codec branch never fires.

## 3. `TableChange` surface audit and the ReplaceColumn decision

`fluss-common/src/main/java/org/apache/fluss/metadata/TableChange.java`
defines the `SchemaChange` family at line 218. The concrete subclasses
and their current client-side semantics are:

| Class | File line | Semantic |
|---|---|---|
| `AddColumn(name, type, comment, position)` | `TableChange.java:229` | Insert a column at `FIRST`, `LAST`, or `AFTER(col)`. |
| `DropColumn(name)` | `TableChange.java:279` | Remove a column by name. |
| `ModifyColumn(name, type, comment, position)` | `TableChange.java:297` | Change type / comment / position of an existing column. |
| `RenameColumn(oldName, newName)` | `TableChange.java:351` | Rename a column in place. |

`Admin.alterTable(TablePath, List<TableChange>, boolean)` at
`fluss-client/src/main/java/org/apache/fluss/client/admin/Admin.java:308`
is the client entry point. The request flows through the coordinator
(`CoordinatorService.alterTable` at
`fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java:504`)
into `MetadataManager.alterTableSchema` at
`fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java:438`,
which delegates column-level work to `SchemaUpdate.applySchemaChanges`
(`fluss-server/src/main/java/org/apache/fluss/server/coordinator/SchemaUpdate.java:31`).
That class is the constraint: it handles `AddColumn` and throws
`SchemaChangeException` for `DropColumn`, `ModifyColumn`, `RenameColumn`
(`SchemaUpdate.java:96`, `:100`, `:104`). Its `AddColumn` branch further
requires `ColumnPosition.last()` (`SchemaUpdate.java:75`) and a nullable
`DataType` (`SchemaUpdate.java:79`).

The reshape we need is

```
KAFKA_PASSTHROUGH shape:
  record_key BYTES, payload BYTES, event_time TIMESTAMP_LTZ(3), headers ARRAY<ROW<...>>

KAFKA_TYPED_<FORMAT> shape:
  record_key BYTES, f1 T1, f2 T2, ..., fN TN, event_time TIMESTAMP_LTZ(3), headers ARRAY<ROW<...>>
```

Three options were considered:

- **Option A — `DropColumn("payload")` + N × `AddColumn(fX, nullable Tx, LAST)`**
  in a single `alterTable` request. The single-request claim matters so
  the reshape is atomic with respect to the coordinator's ZK write at
  `MetadataManager.java:464`. This is the "zero fluss-core edits" shape
  on paper, but `SchemaUpdate.dropColumn` at `SchemaUpdate.java:96`
  rejects `DropColumn` unconditionally. Adopting Option A therefore
  requires editing `SchemaUpdate` to allow `DropColumn` on non-primary-key
  columns, plus teaching the ZK schema-registration path to bump the
  schema id through a dropped-column delta. That is a fluss-core edit.

- **Option B — introduce `TableChange.ReplaceColumn(oldName, newFields)`**
  in `fluss-common` and plumb it through `SchemaUpdate`. One semantic
  operation, cleanest on the wire, explicit about the "replace payload
  with expanded fields" intent. Also a fluss-core edit — two, in fact:
  `TableChange.java` and `SchemaUpdate.java`.

- **Option C — first-registration path bypasses `alterTable` entirely.**
  The key observation is that a `KAFKA_PASSTHROUGH` topic with no
  registered schema and no produce traffic has nothing to preserve. The
  Kafka topic row in the catalog is created lazily by
  `SchemaRegistryService.ensureCatalogEntities` right before we register
  the first schema version; the underlying data table likewise has no
  records on first-register because no producer has yet seen a Kafka SR
  magic-byte envelope that would pass T.2's typed encoder. On first
  registration we drop the pre-existing passthrough data table and
  recreate it with the expanded shape, all within the SR handler and
  behind a `_catalog.__tables__` format flip. Subsequent evolutions are
  pure appends and use `alterTable` with exactly the `AddColumn(nullable,
  LAST)` shape that `SchemaUpdate` already accepts today.

**Decision: Option C for first registration, existing `AddColumn` for
subsequent evolutions.** Zero fluss-core edits. The reshape is
expressible as `Admin.dropTable` + `Admin.createTable` + catalog format
flip, each of which is already plumbed through the coordinator and tested.
We pay a small safety cost: the drop-and-recreate window on an empty
table is not strictly atomic with the catalog row write. That window is
bounded by §8's rollback rules. Option A or B remain the correct answer
if a future phase needs to reshape a typed table whose columns disagree
with a new schema (type narrowing, renames); we call that out in §13.

The honest second-order consequence is that Option C collapses when the
topic already has records. A Kafka topic created through the Kafka admin
path and produced to by a raw-bytes producer before anyone registers a
schema cannot be reshaped without either data loss or a translator for
the in-situ `payload` bytes. T.3 refuses first-registration in that case
(§5) and returns 409 with a message pointing the operator at the manual
resolution: drop the topic or register *before* the first produce.

## 4. First registration: `KAFKA_PASSTHROUGH` → `KAFKA_TYPED_<FORMAT>`

Sequence inside a new class `TypedTableEvolver.evolveOnRegister(...)`,
called from `SchemaRegistryService.register` on the branch where the
catalog `format` column reads `KAFKA_PASSTHROUGH`:

1. Client issues `POST /subjects/{topic}-value/versions` with
   `schemaType` and `schema` body. `SchemaRegistryHttpHandler` parses
   the type through `canonicaliseFormat`
   (`SchemaRegistryService.java:393`).
2. `SchemaRegistryService.register` resolves the subject to a topic via
   `SubjectResolver.topicFromValueSubject` (`SchemaRegistryService.java:334`)
   and confirms the Kafka data table exists via `requireKafkaTable`
   (`SchemaRegistryService.java:336`). Both checks must succeed before
   we contemplate reshaping anything.
3. Look up the current `CatalogTableEntity` (`format` field at
   `fluss-catalog/src/main/java/org/apache/fluss/catalog/entities/CatalogTableEntity.java:37`).
   If absent, `ensureCatalogEntities` has not yet run; run it, which
   creates the row in `KAFKA_PASSTHROUGH` form.
4. Branch on `entity.format()`:
   - `KAFKA_PASSTHROUGH` → Phase T.3 first-register path (this section).
   - `KAFKA_TYPED_AVRO` / `_JSON` / `_PROTOBUF` → Phase T.3 evolve path
     (§5). If the requested `formatId` differs from the stored typed
     format, reject with 409 `cannot re-type topic from <X> to <Y>`.
5. Translate text → proposed `RowType` via
   `FormatRegistry.instance().translator(formatId).translateTo(text)`
   (SPI at
   `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/FormatRegistry.java:121`;
   Avro impl at
   `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/AvroFormatTranslator.java:89`,
   with Protobuf and JSON equivalents registered through
   `META-INF/services`).
6. Validate the proposed `RowType`:
   - Top-level must be a record — already enforced by the translators.
   - Every field has a name; no duplicates.
   - No field collides with the reserved columns `record_key`,
     `event_time`, `headers`. Collision → 422 `reserved column name '<n>'`.
   - Format-specific rejects stay with the translator (doc 0009 §4).
7. Check whether the data table is producible-empty. The cheapest answer
   is `listOffsets(topicPath, allBuckets, OffsetSpec.LATEST)`: if all
   bucket end-offsets equal the corresponding start-offsets, the table
   has no records. If any bucket is non-empty, return 409 with body
   `topic '<t>' already has records; cannot reshape on first register`
   and do not proceed. This guard keeps the drop-and-recreate path safe.
8. Build the expanded `KAFKA_TYPED_<FORMAT>` schema: `record_key BYTES`
   (plus primary-key marker iff the existing table was compacted),
   followed by `proposedRowType` fields rewritten as nullable, followed
   by `event_time TIMESTAMP_LTZ(3) NOT NULL`, followed by `headers
   ARRAY<ROW<name STRING, value BYTES>>`. Nullability of the expanded
   columns is forced so that downstream additive evolution (§5) stays
   consistent with the `SchemaUpdate` nullable invariant at
   `SchemaUpdate.java:79`.
9. Call `admin.dropTable(tablePath, false)` and then `admin.createTable(
   tablePath, newDescriptor, false)`. Both calls go through the
   coordinator; failure of either surfaces as a 503-mapped
   `SchemaRegistryException` and the catalog row is not flipped.
10. On success, flip `_catalog.__tables__.format` from
    `KAFKA_PASSTHROUGH` to `KAFKA_TYPED_<FORMAT>`. The matching enum
    extension lives in `CatalogTableEntity` and the catalog service.
11. `catalog.registerSchema(kafkaDatabase, topic, canonicalFormat,
    schemaText, null)` mints a new SR schema id and records the version
    (`SchemaRegistryService.java:377`). `catalog.bindKafkaSubject`
    binds the subject.
12. Return the SR schema id to the HTTP caller.

Steps 9–11 are the critical window. Rollback semantics are covered in §8.

## 5. Subsequent registrations: additive typed evolution

Entered when step 4 lands on a `KAFKA_TYPED_<FORMAT>` row whose stored
format matches the request's `formatId`:

1. Translate proposed `schemaText` through the format's translator to get
   `proposedRowType`.
2. Retrieve the current table's `RowType` via
   `admin.getTableInfo(tablePath).get().getSchema().toRowType()`. Strip
   the reserved prefix/suffix columns (`record_key`, `event_time`,
   `headers`) to obtain `currentUserRowType`.
3. Compute the diff between `currentUserRowType` and `proposedRowType`:
   - **Accepted:** every field of `currentUserRowType` is present in
     `proposedRowType` at the same position with the same type, and
     `proposedRowType` ends with zero or more additional fields. The
     additional fields must all be declared nullable in the proposed
     schema (for Avro: `[null, T]` union; for JSON Schema: `"null"` in
     `"type"` or an explicit nullable idiom; for Protobuf: `optional`
     or proto3 scalar which is nullable by convention).
   - **Rejected** (return 409 with a field-level message, never touching
     the data table):
     - A column in `currentUserRowType` is absent from
       `proposedRowType` → `column removed: <n>`.
     - A column exists in both but types differ →
       `column type changed: <n>: <from> -> <to>`.
     - A column is renamed (heuristic: absent at the current position,
       present at the same index with a different name while types
       match) → `column rename not supported on typed tables: <old> -> <new>`.
     - Column order disagrees → `column reorder not supported: <n>`.
     - A newly-appended column is non-null → `new column must be nullable: <n>`.
4. Build `List<TableChange>` comprising `TableChange.addColumn(name,
   nullableType, null, ColumnPosition.last())` for each accepted new
   field, in schema order.
5. Call `admin.alterTable(tablePath, changes, false).get()`. This goes
   through `SchemaUpdate.addColumn` which already enforces nullable +
   `LAST` (`SchemaUpdate.java:75`, `:79`).
6. On success, call `catalog.registerSchema(...)` to append a new
   `SchemaVersionEntity` and mint a new SR schema id. Old SR schema ids
   remain valid — they live in `_catalog.__schemas__` keyed by
   `schema_id UUID` and are not touched by the alter. T.2's decoder
   cache in `CompiledCodecCache`, keyed on `(tableId, schemaId)`
   (see doc 0014 §3), continues to resolve historical records because
   the cache key is schema-scoped, not table-shape-scoped.
7. On `alterTable` failure, do not write to `__schemas__` — the caller
   sees 409 and the invariant "a live SR schema id always matches the
   current table shape up to its `AddColumn`-projection" is preserved.

The per-field diff runs in `TypedTableEvolver.computeAdditiveDelta(
RowType current, RowType proposed)`, returning either a list of changes
or an `EvolutionReject` with a structured reason that
`SchemaRegistryService` maps to the CONFLICT HTTP status.

## 6. Naming strategies

| Strategy | Subject resolves to | Register behaviour in T.3 |
|---|---|---|
| `TopicNameStrategy` (default) | `<topic>-{value,key}` → the topic's Fluss data table | ALTER the topic's table per §4/§5 |
| `RecordNameStrategy` | subject is the record FQN, shared across topics | First register creates a record-typed table named after the FQN in `<kafkaDatabase>.__records__.<fqn>`; topic rows stay `KAFKA_PASSTHROUGH` with a `backing_ref` pointing at the record table |
| `TopicRecordNameStrategy` | `<topic>-<fqn>` | Topic stays `KAFKA_PASSTHROUGH`; a `schema_id INT` column is added on first register and records store the envelope with a per-record schema id; **no column reshape** |

T.3 implements `TopicNameStrategy` end to end. `RecordNameStrategy` gets
the minimum plumbing to compile — a `backing_ref` column in the topic row
pointing at the record table — but the record-table reshape reuses the
same `TypedTableEvolver` path so the delta is tiny. `TopicRecordNameStrategy`
typed storage is deferred to a later phase because the multi-schema-per-topic
case needs a different read-path story (per-record envelope tagging) that
is orthogonal to the ALTER decision above.

## 7. Key schemas

`<topic>-key` subject registration follows §4/§5 verbatim against the
`record_key` column. Translating the key schema produces a
`proposedKeyType` and the reshape replaces `record_key BYTES` with the
proposed type (wrapped nullable unless the topic is compacted, in which
case the primary-key invariant from `KafkaDataTable.schema(true)` at
`KafkaDataTable.java:58` forces non-null and a compacted topic may only
register a non-null key schema; otherwise 409). Subsequent key-schema
evolutions only accept a same-type same-position match — there is no
"append to the key" concept, so every non-identity diff is rejected.
Key schemas are optional and orthogonal to value-schema lifecycle; the
format flip on the catalog row records both the value and key format
ids, stored as `KAFKA_TYPED_<VALUE_FORMAT>__<KEY_FORMAT>` when a key
schema is present (`_NONE` if not).

## 8. Rollback semantics

The window between `alterTable` / `dropTable` + `createTable` and the
subsequent catalog flip is not protected by a two-phase commit. The
failure states and their handling:

- **`alterTable` (or `dropTable` + `createTable`) fails before the
  catalog update.** The catalog row is unchanged, no `SchemaVersionEntity`
  is appended, and no SR schema id is minted. The caller sees a 409 or
  503 depending on cause. Invariant: catalog format always matches the
  physical table shape.
- **`alterTable` succeeds but the catalog update fails (typed-add path,
  §5).** The data table has the new column; the `__schemas__` history
  does not. Clients will not observe the new column via typed decode
  because the SR schema id was never minted. Remediation: the caller
  retries the register. On retry, step 2 of §5 sees the already-added
  column in the current `RowType`; the diff degenerates to "proposed
  matches current plus zero new fields" and the alter is a no-op; the
  catalog write is attempted again. Idempotent by construction.
- **First-register drop succeeds, create fails.** The topic is
  temporarily gone. The catalog row still says `KAFKA_PASSTHROUGH`. A
  retry attempts to create the typed-shape table from scratch; because
  no produce traffic is possible against a non-existent table, there is
  no data loss. Operators see a brief 503 window on the topic.
- **Create succeeds, catalog flip fails.** The table is physically
  typed but the catalog row still reads `KAFKA_PASSTHROUGH`. Retry the
  register; the reshape step re-runs §4.7's empty-table check, which
  succeeds on a freshly-created table; the drop-and-recreate is a no-op
  in effect (drop an empty table, create the same shape), and the flip
  is re-attempted. Again idempotent.

No partial state is unrecoverable so long as the HTTP caller is willing
to retry. The evolver logs at WARN whenever it enters a recovery branch.

## 9. SR schema id preservation across ALTER

SR schema ids are minted inside `catalog.registerSchema` and stored in
`_catalog.__schemas__` keyed by a `schema_id UUID`. The data table's
column layout lives in ZK under `/fluss/tables/.../schemas` and is
versioned independently. The ALTER path in §5 touches both stores; the
reshape in §4 drops the ZK schema and re-registers a fresh one. In
neither case do existing `__schemas__` rows move: their `schema_id`,
`sr_id`, `format`, and `schema_text` fields are append-only.

This matches 0002's Fluss-first authority principle. The SR schema id is
a hash-derived external handle over `(catalog table id, version,
format)`; the version counter is stored on the `__schemas__` row, not
the data table, so a reshape of the data table cannot invalidate a
previously-minted id. T.2's decoder cache resolves a produce envelope's
magic-byte-plus-id tuple against `__schemas__` first, then compiles a
codec against whatever column layout the *current* data table has — the
cache key is `(tableId, schemaId)` which is reshape-stable.

A subtle consequence: a record produced under schema v1 and read after
an additive alter to v2 decodes through the v1 codec cached by
`(tableId, v1.schemaId)`. That codec targets the v1 field set. The
read-side projection layer in T.2 pads absent v2-only columns with
nulls, which is safe because every additively-added column is nullable
by the §5.3 rule. This is the only cross-doc interaction T.3 has with
T.2's hot path and it falls out of the nullable invariant for free.

## 10. Files

### Added

- `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/TypedTableEvolver.java`
  — owns `evolveOnRegister`, `computeAdditiveDelta`, the empty-table
  guard, the reject classifier, and the rollback-safe orchestration of
  drop/create/alter + catalog flip.
- `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTypedAlterITCase.java`
  — end-to-end suite per §11.

### Modified

- `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryService.java`
  — branch on the catalog row's `format` inside `register(...)` at line
  327 and call into `TypedTableEvolver`. Wire the evolver into the
  constructor so it is injectable from tests.
- `fluss-kafka/src/main/java/org/apache/fluss/kafka/catalog/CustomPropertiesTopicsCatalog.java`
  — teach the catalog service about `KAFKA_TYPED_AVRO`,
  `KAFKA_TYPED_JSON`, `KAFKA_TYPED_PROTOBUF` (plus the
  `__<KEY_FORMAT>` suffix from §7).
- `fluss-catalog/src/main/java/org/apache/fluss/catalog/entities/CatalogTableEntity.java`
  — extend the format constants; entity is a plain value type so this
  is a string-set extension, not a struct change.

### Not modified (goal)

- `fluss-common/src/main/java/org/apache/fluss/metadata/TableChange.java`
  — untouched. Option C avoids `ReplaceColumn`.
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/SchemaUpdate.java`
  — untouched. The §5 evolve path uses only the `AddColumn(nullable,
  LAST)` subset that already works.

If a later phase needs intra-typed re-typing (e.g. Avro `int` → `long`
on an existing column), that phase picks this decision up again and
lands Option A or Option B then.

## 11. Verification: `KafkaTypedAlterITCase`

Driven by an embedded Fluss cluster plus Kafka Schema Registry
client and `kafka-clients`. Every case starts from a fresh topic
created by the Kafka admin path.

1. Register Avro v1 for topic `alice` with record
   `{id: int, name: string}`. Assert HTTP 200 and a numeric SR schema id.
2. `Admin.describeTable(<kafkaDatabase>, "alice")` reports schema
   `(record_key BYTES, id INT NULL, name STRING NULL, event_time
   TIMESTAMP_LTZ(3) NOT NULL, headers ARRAY<...> NULL)`. Catalog
   `format` equals `KAFKA_TYPED_AVRO__NONE`.
3. Produce one v1 record with `KafkaAvroSerializer`; wait until end
   offset advances; `Table.newScan().createLogScanner()` returns a row
   whose `id` and `name` columns contain the produced values.
4. Register Avro v2 adding nullable `email` (`["null", "string"]`).
   Assert 200 and a new SR schema id distinct from v1.
5. `describeTable` now includes `email STRING NULL` at the end of the
   user-field region.
6. Produce one v2 record; re-fetch v1's record → `email IS NULL`;
   fetch v2's record → `email` equals the produced value. Demonstrates
   that the v1 decoder pads the new column with null (§9).
7. Attempt Avro v3 renaming `id` to `user_id`. Assert 409 with body
   containing `column rename not supported on typed tables`. Call
   `describeTable` again; shape is unchanged; v2's SR schema id still
   resolves.
8. Attempt Avro v3' adding a non-null field `score: int`. Assert 409
   with body `new column must be nullable: score`.
9. Repeat steps 1–3 with a Protobuf schema whose field is a
   `google.protobuf.Timestamp`; assert the resulting typed column is
   `TIMESTAMP_LTZ(6) NULL`. One Protobuf case is enough — the JSON
   Schema case is covered by doc 0007's existing IT augmented with the
   `describeTable` assertion from step 2.

All assertions use AssertJ. The produce/consume side uses randomised
payloads seeded once per case.

## 12. Scope

Approximately 800 LOC split roughly 300 (`TypedTableEvolver`), 120
(`SchemaRegistryService` wiring), 80 (catalog format constants and
plumbing), 300 (IT). One PR. **Zero fluss-core edits** — nothing in
`fluss-common`, nothing in `fluss-server/coordinator/SchemaUpdate.java`,
nothing in `fluss-client/admin`. The format enum extensions live in
`fluss-catalog` and `fluss-kafka` only.

## 13. Out of scope

- `TopicRecordNameStrategy` typed storage. Requires a per-record
  envelope-tagging read path (§6).
- Intra-typed re-typing. Switching a topic's registered format (Avro →
  Protobuf) or narrowing a typed column requires drop-and-recreate-
  with-data, which is a data-migration feature not a registration
  feature. If and when that lands, Option A or Option B from §3
  becomes the right answer and this doc's "zero fluss-core edits"
  promise ends.
- JSON Schema recursive `$ref`. Translator rejects already (doc 0007).
- Protobuf `Any`. Translator rejects already (doc 0008).
- First-register on a topic that already has records. §4.7 returns 409
  and points at manual drop-and-recreate. A later phase may replay
  existing `payload` bytes through the new translator to backfill typed
  columns, but that is its own lifecycle.
- Alter-triggered codec-cache invalidation. T.2 already keys on
  `(tableId, schemaId)`; §9 establishes that those keys survive a
  reshape. No cache-eviction work is required.
