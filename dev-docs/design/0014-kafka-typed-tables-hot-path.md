# Design 0014 — Typed-tables hot-path wiring (Phase T.2)

**Status:** Draft, 2026-04-24
**Owner:** Ben Gamble
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0003-kafka-produce-fetch-kafka-shape-table.md`,
`0007-json-schema-translator.md`, `0008-protobuf-translator.md`,
`0009-multi-format-registry.md`.
**Supersedes:** nothing.

## 1. Context

Design 0002 fixed the principle: Fluss's own schema model is canonical,
and Kafka SR-framed wire bytes for a registered subject must land in a
typed Fluss row — not in a `payload BYTES` column. Design 0009 extended
the registry to three formats (Avro, JSON Schema, Protobuf) behind a
common `FormatTranslator` SPI, each paired with a compiled `RecordCodec`
in `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/`. Designs
0007 and 0008 specified the per-format translators that turn submitted
schema text into a canonical Fluss `RowType`. Phase T.1 shipped every
piece above except the last wire: the Produce and Fetch transcoders
still copy `KafkaRecord.value()` byte-for-byte into the `payload BYTES`
column of `KafkaDataTable`. Registering an Avro schema against topic
`alice` today does not change what `SELECT * FROM kafka.alice` returns.

Phase T.2's job is to flip that. For a topic whose catalog row has a
typed format (`KAFKA_TYPED_AVRO` / `KAFKA_TYPED_JSON` /
`KAFKA_TYPED_PROTOBUF`), the Produce path must strip the Kafka SR
frame, look up the compiled codec, and decode directly into the
`BinaryRow` the Fluss append path expects. The Fetch path must do the
reverse: read the Fluss row as the typed row type, re-prepend the
Kafka SR frame, and hand the bytes back to the Kafka consumer. The
hot path stays zero-allocation on the happy path — the codec cache
resolves by a packed 64-bit key, the decoder writes straight into an
`IndexedRowWriter`, and the byte-copy branch remains available (and
remains the default) behind a feature flag until T.3 lands.

## 2. Status at time of writing

### 2.1 What T.1 left in-tree

| Component | Path | State |
|---|---|---|
| Registry | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/FormatRegistry.java` | Stable; ServiceLoader-discovered |
| Codec cache | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/CompiledCodecCache.java` | Stable; `computeIfAbsent` with packed key |
| Codec SPI | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/RecordCodec.java` | Stable; `decodeInto` / `encodeInto` / `rowType()` |
| Avro compiler | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/AvroCodecCompiler.java` | Janino, per-field specialised |
| JSON compiler | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/JsonCodecCompiler.java` | Janino |
| Protobuf compiler | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/ProtobufCodecCompiler.java` | Janino |
| Translators | `AvroFormatTranslator`, `JsonSchemaFormatTranslator`, `ProtobufFormatTranslator` in the same package | Registered via `META-INF/services/` |
| SR catalog write | `SchemaRegistryService.java:909` | Writes `format="KAFKA_PASSTHROUGH"` unconditionally |

The codec interface fixed at T.1 is:

```java
int decodeInto(ByteBuf src, int offset, int length, IndexedRowWriter dst);
int encodeInto(BinaryRow src, ByteBuf dst);
short schemaVersion();
RowType rowType();
```

See `RecordCodec.java:51-85`. That contract is load-bearing for T.2 and
does not change.

### 2.2 What Produce and Fetch do today

`KafkaProduceTranscoder.writeRow` (`KafkaProduceTranscoder.java:450-485`)
writes four fixed columns: `record_key`, `payload` (raw Kafka value
bytes), `event_time`, and `headers`. The `buildFlussRecords` call at
`KafkaProduceTranscoder.java:228` and the compacted variant at
`KafkaProduceTranscoder.java:291` both copy `kafkaRecord.value()` into
column 1 via `byteBufferToBytes(...)` at
`KafkaProduceTranscoder.java:463`. Schema id carried on the Fluss
`MemoryLogRecordsIndexedBuilder` (`KafkaProduceTranscoder.java:420`) is
the **Fluss** schema id from `TableInfo.getSchemaId()`, not the
SR wire schema id.

`KafkaFetchTranscoder.encode` (`KafkaFetchTranscoder.java:294-405`)
decodes the Fluss row through `RowView.of` at
`KafkaFetchTranscoder.java:427-451`, which hard-codes the
`KafkaDataTable` column layout: `0=record_key`, `1=payload (BYTES)`,
`2=event_time`, `3=headers`. The `byte[] value = ...row.getBytes(1)`
call at `KafkaFetchTranscoder.java:431` is the reverse of the Produce
byte-copy. Both ends today assume a `payload BYTES` column.

### 2.3 Registry reach today

`KafkaProduceTranscoder` and `KafkaFetchTranscoder` do not reference
`FormatRegistry` or `CompiledCodecCache`. T.1 left both wired only to
the SR HTTP surface, never to the data path — the registry is
effectively dormant until T.2.

`SchemaRegistryService.ensureCatalogEntities`
(`SchemaRegistryService.java:893-918`) writes `format="KAFKA_PASSTHROUGH"`
for every topic row. The typed-format literals
(`KAFKA_TYPED_AVRO`, `KAFKA_TYPED_JSON`, `KAFKA_TYPED_PROTOBUF`) do
not appear anywhere in-tree; this document introduces them.
`CatalogTableEntity.format()` is a plain `String`
(`CatalogTableEntity.java:37,74`) — no enum to extend.

## 3. Typed-path selection

The decision is per-topic, made exactly once per Produce/Fetch call
when the handler resolves `KafkaTopicInfo → TableInfo`.

| `__tables__.format` | Route |
|---|---|
| `KAFKA_PASSTHROUGH` (and legacy rows without a catalog entry) | Byte-copy path, unchanged from today |
| `KAFKA_TYPED_AVRO` | Typed path, Avro codec |
| `KAFKA_TYPED_JSON` | Typed path, JSON codec |
| `KAFKA_TYPED_PROTOBUF` | Typed path, Protobuf codec |

The decision is cached on the existing per-partition context. On the
fetch side that is the `PartitionContext` inner class at
`KafkaFetchTranscoder.java:219`; on the produce side it is inlined per
call in `produceTopic` (`KafkaProduceTranscoder.java:117`). Both grow a
new field of the form:

```java
private final RouteKind route;  // PASSTHROUGH | TYPED_AVRO | TYPED_JSON | TYPED_PROTOBUF
private final @Nullable String formatId;  // "AVRO" | "JSON" | "PROTOBUF"; null for passthrough
```

The lookup is one catalog fetch per batch (already performed — see
`KafkaFetchTranscoder.java:115-133`), so the incremental cost is a
string comparison.

**Opt-in semantics.** For Kafka Connect sinks, DLQ topics, passthrough
mirroring, or any topic whose value bytes are not a registered schema,
operators keep the row at `KAFKA_PASSTHROUGH` and the hot path never
touches a codec. Registering a schema through
`SchemaRegistryService.register` is what flips the catalog row to
`KAFKA_TYPED_*`. Until T.3 lands the ALTER flow, registration on a
topic that already has data rejects with HTTP 409, so there is no
mid-life migration surface to reason about in T.2.

## 4. Produce path (Kafka bytes → Fluss row)

**Wire frame.** Kafka SR-framed producer records carry:

```
[0x00]               // magic byte
[int32 schemaId]     // big-endian; globally unique across topics
[body]               // Avro binary / protobuf binary / JSON text
```

For every record batch whose topic route is typed, the Produce path:

1. Reads the magic byte. If it is not `0x00`, reject the record with
   `Errors.INVALID_RECORD` (not `CORRUPT_MESSAGE` — the batch itself
   decompressed correctly).
2. Reads the big-endian `int32` schema id. On a length < 5 frame, reject
   with `Errors.CORRUPT_MESSAGE`.
3. Resolves the codec:

   ```java
   long key = CompiledCodecCache.packKey(tableInfo.getTableId(), schemaId);
   RecordCodec codec = cache.get(key, () -> compileFor(tableId, schemaId));
   ```

4. Calls `codec.decodeInto(buf, frameOffset + 5, frameLength - 5, rowWriter)`.
   The codec writes into the same `IndexedRowWriter` that today's
   `writeRow` writes to (`KafkaProduceTranscoder.java:454`). The
   column mapping is: codec fields fill the typed-value columns, while
   the wrapper writes `record_key`, `event_time`, and `headers` exactly
   as the passthrough path does.
5. Calls `MemoryLogRecordsIndexedBuilder.append(...)` with the
   resulting `IndexedRow`. The rest of the Produce path — compacted vs.
   log, `putRecordsToKv` vs. `appendRecordsToLog`, `acks` / `timeoutMs`
   handling — is unchanged.

**Codec miss.** On a cache miss for a live schema id the codec has to
be compiled:

1. Fetch the `SchemaVersionEntity` for the global SR schema id via the
   catalog (`catalog.getSchemaBySrSchemaId(schemaId)`). The entity
   carries the schema text and the `format` string.
2. Sanity-check the entity's format against the topic's catalog-row
   format. A typed topic receiving a record framed with a schema id
   that belongs to a different topic's subject is `INVALID_RECORD` —
   reject the record, do not poison the cache.
3. Look up the format's `FormatTranslator` via
   `FormatRegistry.instance().translator(entity.format())`.
4. Translate the schema text to a `RowType`. Sanity-check that the
   derived `RowType`'s prefix matches the typed-value columns on the
   table's current `RowType` (from `TableInfo.getRowType()`). A
   mismatch means the catalog and the table have drifted — this is an
   invariant violation that T.3's ALTER flow is responsible for
   preventing. Until then, reject with `KAFKA_STORAGE_ERROR` and log
   at ERROR.
5. Hand the translated `RowType` + raw schema text to the format's
   codec compiler. Today that is `AvroCodecCompiler.compile(...)` and
   its siblings in `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/`.
6. Insert the compiled codec into the cache via
   `CompiledCodecCache.get(packedKey, supplier)`; concurrent decoders
   for the same `(tableId, schemaId)` compile exactly once by the
   contract at `CompiledCodecCache.java:73-88`.

## 5. Fetch path (Fluss row → Kafka bytes)

Fetch is structurally the mirror of Produce, but `KafkaFetchTranscoder`
today is one monolithic method (`encode` at
`KafkaFetchTranscoder.java:294`). T.2 extracts a seam:

```java
package org.apache.fluss.kafka.fetch;

interface KafkaFetchCodec {
    LogRecordReadContext readContext(int schemaId);
    KafkaRecordView rowToKafkaRecord(InternalRow row, ChangeType type, long logOffset);
}
```

`KafkaRecordView` is the existing `RowView` at
`KafkaFetchTranscoder.java:408-466`, lifted to package scope with
`offset` / `timestamp` / `key` / `value` / `headers` fields plus
`estimatedSize()`.

Two implementations:

| Class | Behaviour |
|---|---|
| `PassthroughKafkaFetchCodec` | Today's logic, verbatim. `readContext(int)` returns `LogRecordReadContext.createReadContext(tableInfo, …, schemaGetter)`. `rowToKafkaRecord` is the body of `RowView.of` with the `payload BYTES` column read at column 1. |
| `TypedKafkaFetchCodec` | Calls `codec.encodeInto(row, scratchBuf)` where `codec` is resolved from `CompiledCodecCache` by `(tableId, rowSchemaId)`. Prepends the 5-byte Kafka SR frame. `record_key`, `event_time`, and `headers` columns are still read from fixed column indices — the typed codec only owns the value bytes. |

`KafkaFetchTranscoder.encode` becomes a thin driver: pick the codec
once per batch based on the `PartitionContext.route`, iterate rows,
call `rowToKafkaRecord`, append to the Kafka `MemoryRecordsBuilder`.
The existing ChangeType filter (`KafkaFetchTranscoder.java:365-371`:
`UPDATE_BEFORE` skipped, `DELETE` → tombstone) moves into the driver
because both codec impls share it.

**Schema id on the wire.** The typed Fluss row carries a Fluss schema
id — not the SR schema id the producer framed with. The catalog's
`SchemaVersionEntity.srSchemaId()` is the mapping. `TypedKafkaFetchCodec`
looks it up once per batch via the topic's current schema id and
prepends that integer. For topics whose schema is static for the life
of the cluster this is a single lookup per fetch; T.3 addresses the
evolution case.

## 6. Codec cache shape

The cache is `CompiledCodecCache` exactly as shipped in T.1
(`CompiledCodecCache.java:45-104`). T.2 does not modify its shape. Key
packing is:

```
packedKey = (tableId << 32) | (schemaId & 0xFFFFFFFFL)
```

`tableId` is the Fluss table id (`long`); `schemaId` is the Kafka SR
global id (`int`). Collisions are structurally impossible: a `tableId`
is unique per cluster, a `schemaId` is unique across all subjects, and
the product space is 96 bits of which we keep 64 — enough for
`2^32 − 1 ≈ 4.3B` schemas per table.

Concurrent compile races for the same key are suppressed by
`computeIfAbsent` (`CompiledCodecCache.java:65-88`). Different keys can
compile in parallel because each compile gets a fresh `SimpleCompiler`
instance (`AvroCodecCompiler.java:80+`).

Eviction policy: **none.** A schema is immutable by definition;
`schemaId` is globally unique and the total ever registered is
bounded at the schema-registry level, typically in the low thousands
per cluster. A compiled Janino class is ≈ 40 KiB of metaspace; 10k
schemas is ≈ 400 MiB of class metadata. That is acceptable; when it is
not, T.7 (not in this document) introduces a pinned-set + LRU over an
idle tail.

## 7. Feature flag

A new `ConfigOption` gates the entire typed path:

| Key | Type | Default | Scope |
|---|---|---|---|
| `kafka.typed-tables.enabled` | `boolean` | `false` | Cluster; read once at server start |

Behaviour:

- `false` (default): Produce and Fetch always take the passthrough
  route regardless of `__tables__.format`. T.2 ships in this state so
  the change is safe to merge before T.3's ALTER flow exists.
- `true`: per-topic decision from §3 applies.

The flag is read at `KafkaServerContext` construction and cached as a
`boolean` field. Toggling requires a restart. The flag is orthogonal
to schema registration: with it off, the SR still accepts
registrations and records typed formats in the catalog; the hot path
simply ignores them.

## 8. Error handling

| Stage | Failure | Kafka error | Log level |
|---|---|---|---|
| Produce | Frame shorter than 5 bytes | `CORRUPT_MESSAGE` per record | DEBUG (expected from non-SR producers) |
| Produce | Magic byte ≠ `0x00` | `INVALID_RECORD` per record | DEBUG |
| Produce | Schema id unknown in catalog | `INVALID_RECORD` per record | INFO |
| Produce | Schema id belongs to a different topic's subject | `INVALID_RECORD` per record | INFO |
| Produce | Translator compile fails | `KAFKA_STORAGE_ERROR` for the partition | ERROR (should not happen after T.1 registration validation) |
| Produce | Codec compile fails | `KAFKA_STORAGE_ERROR` for the partition | ERROR |
| Produce | `codec.decodeInto` throws | `CORRUPT_MESSAGE` per record | WARN |
| Fetch | Codec cache miss + compile fail | Skip row; log once per `(tableId, schemaId)` | ERROR |
| Fetch | `codec.encodeInto` throws on a single row | Skip row, continue batch | ERROR (catastrophic; investigate) |
| Either | Feature flag off but catalog says typed | Route as passthrough; log once per topic at INFO on first contact | INFO |

Two invariants drive these choices: a bad record never takes down a
partition (T.2 narrows the batch-level exception at
`KafkaProduceTranscoder.java:229-239` to per-record errors on the
typed path); and a codec compile error is a server problem (the
client framed bytes correctly — log `ERROR`, bail on the partition,
let the SRE queue pick it up). The connection is never closed on a
decode error.

## 9. Observability

Four new metrics, reported under the existing Kafka bolt-on metric
group (design 0012):

| Metric | Type | Description |
|---|---|---|
| `typedProduceRate` | Meter | Records/sec taking the typed Produce path |
| `typedFetchRate` | Meter | Records/sec taking the typed Fetch path |
| `codecCompileRate` | Meter | Codec compiles/sec (should tend to zero in steady state) |
| `codecCacheSize` | Gauge | `CompiledCodecCache.size()` |

All four live on `KafkaMetricGroup` (or equivalent from doc 0012) and
are scoped per broker. `typedProduce|FetchRate` split by topic is out
of scope here — topic cardinality explodes metric storage in the
worst case. Cluster-wide rates are enough for T.2's "is typed wired?"
signal; per-topic breakdowns become a T.7 ask.

## 10. Files

### 10.1 New

| Path | Purpose |
|---|---|
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/KafkaFetchCodec.java` | The SPI from §5 |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/PassthroughKafkaFetchCodec.java` | Extracted byte-copy codec |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/TypedKafkaFetchCodec.java` | `codec.encodeInto` + frame-prepend |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/KafkaRecordView.java` | Lifted from `KafkaFetchTranscoder.RowView` |
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTypedHotPathITCase.java` | End-to-end IT |
| `fluss-jmh/src/main/java/org/apache/fluss/jmh/kafka/ProduceTypedThroughputBench.java` | JMH gate (if `fluss-jmh` exists; otherwise sibling `*Bench` in `fluss-kafka` test sources) |

### 10.2 Modified

| Path | Change |
|---|---|
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/produce/KafkaProduceTranscoder.java` | Thin driver + typed branch in `buildFlussRecords` / `buildFlussKvRecords`; `writeRow` gains a codec overload |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/fetch/KafkaFetchTranscoder.java` | Dispatch via `KafkaFetchCodec`; ChangeType filter + batch assembly stay here |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaServerContext.java` | New `boolean typedTablesEnabled()` accessor; cached from config at constructor time |
| `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` | New `KAFKA_TYPED_TABLES_ENABLED` option |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryService.java` | `ensureCatalogEntities` writes the typed format string (not `"KAFKA_PASSTHROUGH"`) when a schema is registered against the topic; already-existing passthrough rows stay (see T.3 for migration) |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/FormatRegistry.java` | No change expected; if T.1 did not expose `CompiledCodecCache` on the registry, add a single getter |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/typed/CompiledCodecCache.java` | No change expected; if a compile-fail cache is needed to avoid thrash, add `getOrCompile(tableId, schemaId, Supplier<RecordCodec>)` with a negative-result side table |

`KafkaDataTable.java` is unchanged: the passthrough shape still exists
for passthrough topics, and typed topics have their own `RowType` on
the table itself (produced by the translator at registration time).

## 11. Verification

### 11.1 Integration test — `KafkaTypedHotPathITCase`

Uses `kafka-clients 3.9` + Kafka SR `KafkaAvroSerializer` /
`KafkaAvroDeserializer` pointed at the SR HTTP listener. Fluss cluster
is the standard `FlussClusterExtension`.

Scenario A (typed): feature flag on.

1. Register Avro schema `{"type":"record","name":"Order","fields":[...]}`
   for subject `alice-value`.
2. Confirm the catalog row for `kafka.alice` now has
   `format="KAFKA_TYPED_AVRO"`.
3. Produce 10 records with random field values via
   `KafkaProducer<String, Order>`.
4. Assert via the Fluss client:
   ```java
   Table t = connection.getTable(TablePath.of("kafka", "alice"));
   List<InternalRow> rows = readAll(t);
   assertThat(rows).hasSize(10);
   assertThat(t.getDescriptor().getSchema().getColumnNames())
       .contains("order_id", "sku", "quantity")
       .doesNotContain("payload");
   ```
5. Consume via `KafkaConsumer<String, Order>`, assert byte-equal with
   the produced records (Avro is a canonical-binary format so equality
   is well-defined).

Scenario B (passthrough): feature flag off, same schema.

1. Same steps 1-3.
2. Assert the Fluss schema still has `payload BYTES` (registration
   flips the catalog row, but with the flag off we ignore it).
3. Kafka round-trip still works byte-equal (the bytes never left the
   column).

Scenario C (error paths):

1. Produce a record with an unknown schema id → `INVALID_RECORD`.
2. Produce a raw-bytes record to a typed topic → `CORRUPT_MESSAGE`.
3. Produce to a compacted typed topic with null value → tombstone; row
   deleted in the Fluss KV table.

### 11.2 JMH — `ProduceTypedThroughputBench`

| Parameter | Value |
|---|---|
| Schema | 12-field Avro record (boolean, int×2, long×2, double, string×3, bytes, timestamp-millis, nullable-int) |
| Records per batch | 1000 |
| Warmup | 3 × 5s |
| Measurement | 5 × 10s |
| Fork | 2 |

Two benchmarks:
- `passthroughRecord()` — baseline, feature flag off.
- `typedRecord()` — feature flag on, Avro codec.

**Gate:** typed path within **20%** of passthrough throughput. On an
Apple M2 Pro reference machine T.1's `AvroCodecCompiler` hits ~3.2M
records/sec for the same shape in isolation; the driver overhead
budget is generous at 20%.

### 11.3 Grep gate

```
grep -rn "payload BYTES\|COL_PAYLOAD\|getBytes(1)" fluss-kafka/src/main/java/
```

Must return matches only inside `KafkaDataTable.java` and
`PassthroughKafkaFetchCodec.java`. No typed-path code may reference
the `payload` column index.

## 12. Scope

Roughly 700 LOC of source + 1 IT (~350 LOC) + 1 JMH (~150 LOC). One
PR. Reviewable in a sitting.

## 13. Out of scope for T.2

| Deferred to | Subject |
|---|---|
| T.3 (doc 0015) | `ALTER TABLE` to drop `payload BYTES` and add typed columns on an existing passthrough topic; backfill strategy for in-flight data |
| T.3 (doc 0015) | Schema-evolution diff: when a new Avro version adds a nullable field, which typed columns are added / renamed on the Fluss table |
| T.6 (deferred) | `RecordNameStrategy` / `TopicRecordNameStrategy` — T.2 assumes `TopicNameStrategy` (one schema per topic value) |
| T.4 | Array / map / nested record / enum / fixed support inside codecs — T.1 compilers reject these with `SchemaTranslationException` and T.2 inherits that limitation |
| T.7 | Per-topic typed-rate metrics; compiled-codec class eviction |
| T.7 | Cross-topic subject sharing (one Avro schema powering multiple topics via `RecordNameStrategy`) |
| out-of-scope | Any change to `fluss-client`, `fluss-server`, or `fluss-common` APIs: T.2 is a bolt-on change inside `fluss-kafka` plus one `ConfigOption` |
