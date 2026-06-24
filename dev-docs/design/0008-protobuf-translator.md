# Design 0008 — Protobuf translator, codec and compatibility checker

**Status:** Draft (T-MF in progress), 2026-04-24
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0007-json-schema-translator.md`, `0009-multi-format-registry.md`,
and the structural plan at
`~/.claude/plans/check-which-branch-should-encapsulated-karp.md` §27.5.

## 0. Why this document exists

Phase T-MF is the multi-format sibling of Phase T.1. T.1 landed the
Avro translator + codec compiler. This document specifies the
Protobuf counterpart: how a submitted `.proto` text is translated
to a Fluss `RowType`, how the hot-path encoder/decoder is compiled,
what compatibility rules apply, and what we deliberately reject.

The translator-side is straightforward: Google's own
`protobuf-java` + `protobuf-java-util` give us a parser from
`.proto` text to `FileDescriptorProto` and onward to `Descriptors`.
The codec side is not — we compile per-schema decoders and encoders
that read/write directly from Netty `ByteBuf` using manual
varint/zigzag logic, avoiding the per-call allocations in
`CodedInputStream`. The compat-checker side is a clean-room
implementation of the Protobuf schema-diff semantics, and §8
explains why we chose that over reusing Kafka SR's
`kafka-protobuf-provider`.

Design constraints from §27.1 of the plan are the same as the JSON
document (full type coverage, no fluss-core edits, Kafka SR wire
compatibility). The wire-compat target specifically is:
`io.confluent:kafka-protobuf-serializer:7.5.x`.

## 1. Scope

### 1.1 Syntax versions

We accept:

- **proto3 (full).** All proto3 constructs except `Any`, recursive
  messages, and `oneof` (§3). `map<K, V>` is supported for the
  Protobuf-defined key types. Well-known types (`google.protobuf.*`)
  are mapped per §4.
- **proto2 (Kafka SR subset).** Kafka SR's protobuf serializer
  emits a strict proto2 subset: `optional` and `repeated` fields,
  no `required`. We accept that subset. `proto2 required` is
  rejected with a clear message ("Kafka SR's protobuf serializer
  does not emit `required`; Fluss refuses to accept a shape that
  can't round-trip"). Groups are rejected.

Detection via the `syntax` line at the top of the `.proto`; missing
`syntax` defaults to proto2 per the spec.

### 1.2 Parsing

`.proto` text is not parseable by `protobuf-java` alone —
`protobuf-java` reads serialised descriptor bytes, not `.proto`
source. We use `protobuf-java-util:3.25.5`, which bundles the protoc
text parser. The translator never shells out to an external
`protoc`.

```java
FileDescriptorProto fileProto = Protoc.parseSource(protoText);
Descriptors.FileDescriptor file =
    Descriptors.FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
Descriptors.Descriptor root = file.getMessageTypes().get(0);  // first message
```

If there are zero top-level messages, we reject. If there are
multiple, we take the one whose name matches the subject's record
FQN (Kafka SR does the same); absent a match, we reject with
"ambiguous message root".

### 1.3 What "translator" means

A translator is a pure function `(String protoText) -> RowType`. It
performs:

- Parsing via `Protoc.parseSource`.
- `Descriptor` resolution (including well-known-type imports).
- A recursive walk of the root `Descriptor` producing a canonical
  Fluss `RowType`, with nullability and column properties.

The translator does not evaluate wire bytes; it does not require an
instance message; it does not touch the registry.

## 2. Type mapping

The mapping below is the authoritative table; every row is
exercised by `ProtobufFormatTranslatorTest` (§7). It is a superset
of the table in plan §27.5 — expanded with the concrete
well-known-type aliases we accept and the exact key-type
constraint on `map`.

| Protobuf construct                                    | Fluss `DataType`                                  | Notes                                                                                                        |
|-------------------------------------------------------|---------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `bool`                                                | `BOOLEAN`                                         | —                                                                                                            |
| `int32` / `sint32` / `sfixed32`                       | `INT`                                             | `int32` is varint; `sint32` is zigzag-varint; `sfixed32` is fixed 4 bytes.                                   |
| `int64` / `sint64` / `sfixed64`                       | `BIGINT`                                          | Same wire distinctions; all collapse to `BIGINT` in Fluss.                                                   |
| `fixed32`                                             | `INT` + property `unsigned=true`                  | Unsigned 32-bit on the wire; we promote to `INT` with a flag rather than drop range.                         |
| `fixed64`                                             | `BIGINT` + property `unsigned=true`               | Same reasoning.                                                                                              |
| `uint32`                                              | `BIGINT`                                          | Promotes to preserve unsignedness without a property flag; callers see values ≥ 0.                           |
| `uint64`                                              | `BIGINT` + property `unsigned=true`               | Values ≥ 2^63 are representable but signed-negative; the flag lets callers opt into wrapping semantics.      |
| `float`                                               | `FLOAT`                                           | IEEE 754 single.                                                                                             |
| `double`                                              | `DOUBLE`                                          | IEEE 754 double.                                                                                             |
| `string`                                              | `STRING`                                          | UTF-8; invalid UTF-8 on decode → `INVALID_RECORD`.                                                           |
| `bytes`                                               | `BYTES`                                           | —                                                                                                            |
| `enum Foo { ... }`                                    | `STRING` + property `allowedValues=[...]`         | Symbol name on the wire is an int tag; codec looks up the symbol on decode. Unknown tags default to symbol `"__UNKNOWN_<n>__"`. |
| nested `message Bar { ... }`                          | `ROW<...>`                                        | Recurses. Recursive message types (A uses A transitively) are rejected at translate time.                    |
| `repeated T`                                          | `ARRAY<T>`                                        | Packed encoding is mandatory for proto3 primitive `repeated`; the codec emits both packed and unpacked read paths for proto2 compatibility. |
| `map<K, V>`                                           | `MAP<K, V>`                                       | Protobuf restricts `K` to `string`, `int32`, `int64`, `uint32`, `uint64`, `sint32`, `sint64`, `fixed32`, `fixed64`, `sfixed32`, `sfixed64`, `bool`. We accept all of these. |
| `oneof Choice { ... }`                                | — rejected                                        | Sum types not in `RowType`. See §3.1.                                                                        |
| `proto3 optional T`                                   | nullable `T`                                      | proto3 `optional` is implemented as a synthetic single-field oneof; the translator normalises and surfaces it as nullability. |
| `google.protobuf.Timestamp`                           | `TIMESTAMP_LTZ(6)`                                | Well-known-type shortcut; seconds + nanos → microseconds since epoch.                                        |
| `google.protobuf.Duration`                            | `BIGINT` + property `duration=true`               | Nanos as a single long (seconds × 1e9 + nanos).                                                              |
| `google.protobuf.StringValue`                         | nullable `STRING`                                 | Wrapper-unwrap.                                                                                              |
| `google.protobuf.Int32Value` / `Int64Value`           | nullable `INT` / nullable `BIGINT`                | Wrapper-unwrap.                                                                                              |
| `google.protobuf.FloatValue` / `DoubleValue`          | nullable `FLOAT` / nullable `DOUBLE`              | Wrapper-unwrap.                                                                                              |
| `google.protobuf.BoolValue`                           | nullable `BOOLEAN`                                | Wrapper-unwrap.                                                                                              |
| `google.protobuf.BytesValue`                          | nullable `BYTES`                                  | Wrapper-unwrap.                                                                                              |
| `google.protobuf.UInt32Value` / `UInt64Value`         | nullable `BIGINT`                                 | Wrapper-unwrap; unsigned promotion identical to `uint32` / `uint64`.                                         |
| `google.protobuf.Empty`                               | `ROW<>`                                           | An empty row; marker presence.                                                                               |
| `google.protobuf.Any`                                 | — rejected                                        | Dynamic typing (`type_url` + `value`); no sensible `RowType`. See §3.2.                                      |
| `google.protobuf.Struct`                              | `MAP<STRING, STRING>` (JSON-stringified)          | Best-effort projection; values are JSON-serialised on encode, parsed back on decode.                         |
| `google.protobuf.FieldMask`                           | `ARRAY<STRING>`                                   | Paths list.                                                                                                  |
| recursive `message`                                   | — rejected                                        | `RowType` is tree-shaped.                                                                                    |
| `proto2 required T`                                   | — rejected                                        | Kafka SR does not emit; refusing stays consistent with the Kafka SR-compat contract.                       |
| `extensions`, `extend`, `group`                       | — rejected                                        | Not in Kafka SR's emit surface; deferred indefinitely.                                                      |
| `reserved 1 to 5;`                                    | — honoured at compat check only                   | Reserved field numbers are tracked to enforce field-number immutability (§6).                                |
| `service`, `rpc`                                      | — ignored                                         | Service definitions are skipped; only messages project.                                                      |

### 2.1 Worked example

Input `.proto`:

```proto
syntax = "proto3";
package acme.orders;

import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

message Order {
  int64  id          = 1;
  string customer_id = 2;
  google.protobuf.Timestamp placed_at = 3;
  google.protobuf.StringValue note = 4;
  repeated LineItem items = 5;
  map<string, string> tags = 6;
}

message LineItem {
  string sku       = 1;
  int32  quantity  = 2;
  double unit_price = 3;
}
```

Projects to:

```
ROW<
  id          BIGINT NOT NULL,
  customer_id STRING NOT NULL,
  placed_at   TIMESTAMP_LTZ(6) NOT NULL,
  note        STRING,
  items       ARRAY<ROW<sku STRING NOT NULL,
                        quantity INT NOT NULL,
                        unit_price DOUBLE NOT NULL>> NOT NULL,
  tags        MAP<STRING, STRING> NOT NULL
>
```

Note: proto3 scalar fields default to non-null (`NOT NULL`) because
the wire has no presence bit — the default value is indistinguishable
from "not set". proto3 `optional` (§3.3) restores presence tracking
and projects to nullable.

## 3. Rejection set

### 3.1 `oneof`

A `oneof` block describes a set of at-most-one fields; exactly one
is present in the serialised message. Projecting this to a
structural `RowType` would require either:

- a sum type (`ROW<A | B | C>` — not expressible in Fluss), or
- a product of nullables (`ROW<a A, b B, c C>`) with a side
  invariant "at most one is non-null" — enforceable only at the
  codec boundary, not at the schema boundary.

Kafka SR's behaviour is the product-of-nullables projection. We
could match, but the invariant loses meaning on the read side (Fluss
SQL queries can freely `WHERE a IS NOT NULL AND b IS NOT NULL`,
which is a valid row under the projection but violates the original
`oneof` semantics). We reject at translate time with
`SchemaTranslationException: oneof is a sum type; not expressible
in RowType`.

The exception: the **synthetic oneof** that proto3 `optional` lowers
into. `protobuf-java` exposes this via `OneofDescriptor.isSynthetic()`
(added in 3.15). The translator inspects that flag and treats
synthetic oneofs as nullability (§3.3), not as sum types.

### 3.2 `google.protobuf.Any`

`Any` is `{ type_url: string, value: bytes }` — a dynamically-typed
escape hatch. To decode we'd need registry lookup at codec time
(the `type_url` names another schema by subject), which turns the
Produce hot path into a second registry round-trip per field. We
reject at translate time.

Future: if Phase SR-X.5 (schema references) lands, `Any` could be
supported by static reference declaration at register time.
Deferred.

### 3.3 Recursive messages

```proto
message Tree {
  string label = 1;
  repeated Tree children = 2;  // ← recursive
}
```

Rejected. Detection is a DFS over the `Descriptor` graph, marking
each visited descriptor in an `IdentityHashMap<Descriptor, Boolean>`;
a revisit throws `SchemaTranslationException: recursive message at
<descriptor full name>`. Mutual recursion (A contains B contains A)
is caught the same way.

### 3.4 proto3 `optional` (accepted, not rejected — spec clarification)

Protobuf 3.15 reinstated explicit `optional` on proto3 scalar fields
by lowering to a synthetic one-field `oneof`. The Kafka SR
serializer emits this shape for any field marked `optional`. The
translator:

1. Walks oneofs.
2. If the oneof has `isSynthetic() == true` and contains exactly
   one field, it unwraps the field and marks the projected column
   nullable.
3. Otherwise the oneof falls to §3.1 (rejected).

## 4. Well-known types

`google.protobuf.*` messages used at Fluss row boundaries map to
native Fluss types where possible. We consume the imported
descriptor (not the message name — a user could define
`message Timestamp` in their own package, which would shadow the
well-known type).

| Well-known type                        | Projection                                 | Wire behaviour                                                                |
|----------------------------------------|--------------------------------------------|-------------------------------------------------------------------------------|
| `google.protobuf.Timestamp`            | `TIMESTAMP_LTZ(6)`                         | `{ seconds: int64, nanos: int32 }` → `seconds * 1_000_000 + nanos / 1_000`.   |
| `google.protobuf.Duration`             | `BIGINT` + `duration=true`                 | Same pair, combined into nanoseconds total.                                   |
| `google.protobuf.StringValue` et al.   | nullable primitive                         | Presence of the wrapper message = non-null; absence = null.                   |
| `google.protobuf.Empty`                | `ROW<>`                                    | Presence-only marker.                                                         |
| `google.protobuf.Struct`               | `MAP<STRING, STRING>` (JSON-stringified)   | On encode, values are JSON-stringified; on decode, parsed via Jackson.        |
| `google.protobuf.FieldMask`            | `ARRAY<STRING>`                            | Paths list.                                                                   |
| `google.protobuf.ListValue`            | `ARRAY<STRING>` (JSON-stringified)         | Same best-effort JSON projection as `Struct`.                                 |
| `google.protobuf.Value`                | `STRING` (JSON-stringified)                | Dynamic; projected as JSON text, as a concession to the wire.                 |
| `google.protobuf.NullValue`            | `STRING` (constant `"NULL_VALUE"`)         | An enum with one symbol.                                                      |
| `google.protobuf.Any`                  | — rejected                                 | See §3.2.                                                                     |

`Struct`, `ListValue`, and `Value` project to JSON-stringified
representations because they are dynamically typed and `RowType`
cannot accommodate them structurally. On encode the codec produces
a wire `Struct` whose field values are JSON-valued; on decode the
codec parses those back. This is a concession, flagged at translate
time with a column property `dynamic=json`.

## 5. Wire-format codec

### 5.1 Design

`ProtobufCodecCompiler.compile(RowType, schemaId, Descriptor)` emits
a `RecordCodec` subclass via Janino. Unlike JSON (which uses
Jackson's streaming parser), we do **not** use
`CodedInputStream` — its internal `int` / `long` readers allocate
wrappers on some paths, and its `readByteArray` copies. Instead we
emit code that reads directly from the Netty `ByteBuf`.

The emitted class holds a static field-tag dispatch table:

```java
// Pseudocode (emitted by Janino)
public final class ProtobufCodec_17 implements RecordCodec {
  public int decodeInto(ByteBuf src, int off, int len,
                        BinaryRowWriter dst) {
    int end = off + len;
    int pos = off;
    while (pos < end) {
      long tag = readVarint(src, pos); pos += varintLen(tag);
      int fieldNum = (int) (tag >>> 3);
      int wireType = (int) (tag & 0x7);
      switch (fieldNum) {
        case 1: {  // id (int64, wire=VARINT)
          long v = readVarint(src, pos); pos += varintLen(v);
          dst.writeLong(0, v);
          break;
        }
        case 2: {  // customer_id (string, wire=LEN)
          int sLen = (int) readVarint(src, pos); pos += varintLen(sLen);
          dst.writeString(1, src, pos, sLen);  // zero-copy slice
          pos += sLen;
          break;
        }
        // ... one case per declared field ...
        default: pos = skipField(src, pos, wireType);
      }
    }
    return len;
  }

  // encodeInto mirrors: writeTag + writePayload per non-null column.
}
```

The per-field case knows the field's wire type at compile time, so
there is no runtime branch on `wireType`. If a wire record arrives
with an unexpected wire type for a declared field number, we fall
through to `skipField` — matches the "unknown fields are tolerated"
proto contract.

### 5.2 Varint, zigzag, packed

`ProtobufCodecRuntime` provides:

- `readVarint(ByteBuf, int pos) -> long` — unrolled 10-byte read.
- `varintLen(long) -> int` — to advance `pos` without a second pass.
- `zigzagDecode64(long)` / `zigzagDecode32(int)` — for `sint*`.
- `readPackedVarints(ByteBuf, int pos, int len, LongConsumer sink)`
  — for `repeated int32` etc. packed encoding.
- Write counterparts.

Packed `repeated` is the proto3 default for scalar repeateds; the
codec emits packed-read. For proto2 compat we also emit the
unpacked-read path (each element tagged): if the incoming tag has
wire type 2 (`LEN`), it's packed; any other wire type is unpacked
and the codec iterates until the field number changes.

### 5.3 Nested messages

A nested message field is encoded as `{ tag, length, body }` where
the body is the recursive message's wire bytes. The emitted code
reads `length`, pulls a slice, and dispatches to a precompiled
sub-codec (generated once per distinct nested `Descriptor`).
Sub-codecs are registered in a static array keyed by nesting
position; no runtime lookup.

```java
case 5: {  // items (repeated LineItem)
  int mLen = (int) readVarint(src, pos); pos += varintLen(mLen);
  ByteBuf slice = src.slice(pos, mLen);
  arrayBuilder[4].addElement(
      SUB_CODECS[0].decodeInto(slice, 0, mLen));  // LineItem sub-codec
  pos += mLen;
  break;
}
```

`arrayBuilder[4]` is a stack-allocated `BinaryArrayWriter` reset per
row. `SUB_CODECS[0]` is the pre-compiled `LineItem` codec.

### 5.4 Map fields

Protobuf `map<K, V>` is wire-encoded as `repeated MapEntry { K key
= 1; V value = 2; }`. The emitted code reads each entry, decodes
`key` and `value`, and appends to a `BinaryMapWriter`. No
intermediate `HashMap` is allocated.

### 5.5 Zero-copy slices

`BinaryRowWriter` accepts byte ranges by slice for `STRING` and
`BYTES` columns; the codec uses this to avoid copying the payload
out of the source `ByteBuf`. This matches design 0002's
"zero intermediate object allocations" non-negotiable.

### 5.6 Performance gate

Plan §27.11 cites the same 20 % gate as Avro. In practice Protobuf
is slightly faster than Avro for fixed-width fields and slightly
slower for varint-heavy shapes. Benchmarks in `fluss-jmh`
(`ProtobufCodecDecodeBench`) are deferred to Phase T.2 when the
hot path is actually exercised. The cross-check that matters at
T-MF is **correctness**: `ProtobufCodecTest` round-trips against
`com.google.protobuf.DynamicMessage` as an oracle.

## 6. Compatibility rules

`ProtobufCompatibilityChecker` implements the `CompatibilityChecker`
SPI (defined in 0009). It compares two `FileDescriptorProto` trees
post-normalisation.

Normalisation:

- Sorts fields within a message by field number (protoc emits
  declaration order; we canonicalise).
- Canonicalises `reserved` ranges.
- Expands imports to their transitive closure.
- Unwraps synthetic oneofs (proto3 `optional`).

### 6.1 Field-number immutability

**Rule.** Once a field number is used, it must keep the same name
*and* the same wire type *and* the same declared Fluss-projection
type for the life of the subject. Removing a field requires adding
it to `reserved`.

**BACKWARD fail example:**

```proto
// S_old
message User { int32 id = 1; string name = 2; }

// S_new — tag 2 changed from string to int32 (same wire-type VARINT? no, LEN vs VARINT)
message User { int32 id = 1; int32 name = 2; }
```

Old data with a LEN-wire `name` field (`string`) cannot be read
under the new schema's VARINT-wire `name` (`int32`). Reject.

**BACKWARD pass example (same wire-type, Fluss-equivalent projection):**

```proto
// S_old
message User { int32 id = 1; int32 age = 2; }

// S_new — tag 2 renamed from age to years, same type
message User { int32 id = 1; int32 years = 2; }
```

The tag-level wire shape is preserved; the name changed. Whether
this is accepted depends on the Kafka SR rule we're matching —
see §6.6 for the naming exception.

### 6.2 Type immutability on an existing tag

Tighter than "same wire type": the *Fluss projection type* must
match. `int32 → int64` keeps the VARINT wire type but widens the
projection; old data's 32-bit values still fit, so this is
**BACKWARD-compatible**. The reverse (`int64 → int32`) is a
narrowing and breaks BACKWARD if any historical value exceeded
`INT_MAX`.

The checker applies a conservative rule: narrowing of projected
type is always a BACKWARD break regardless of historical values,
because we don't scan the log.

### 6.3 Adding new optional fields

Adding a new field with a new number and proto3 `optional` (or
proto2 `optional`) is **BACKWARD-compatible**: old data lacks the
field, new consumers read it as null.

It is **not** `FORWARD`-compatible if old consumers have
`additionalProperties`-style strictness — but Protobuf consumers
traditionally tolerate unknown fields, so in practice this is
forward-compatible too, and our FORWARD rule treats "add field with
new number" as a pass.

### 6.4 Removing fields

**Removing a field without reserving its number** is a BACKWARD
break (old data has the field, new consumer drops it — that's
silently tolerated by Protobuf consumers, so actually OK on the
wire, *but* we additionally check that the field was not part of
any prior BACKWARD-committed consumer interface).

**Removing a field *with* `reserved N;` for its number** is the
canonical safe removal; it's BACKWARD-compatible.

### 6.5 Compat levels

Seven levels, same as JSON:

- `NONE`: always pass.
- `BACKWARD`: new consumer reads old data. Rules above.
- `BACKWARD_TRANSITIVE`: BACKWARD against every prior version.
- `FORWARD`: old consumer reads new data. Symmetric rules.
- `FORWARD_TRANSITIVE`: FORWARD against every prior version.
- `FULL`: intersection of BACKWARD + FORWARD against immediate prior.
- `FULL_TRANSITIVE`: intersection against every prior version.

Each level has a pair of passing + failing tests; see §7.

### 6.6 Field-name sensitivity

Kafka SR's Protobuf provider does **not** break on field renames
(renaming is a source-compatibility concern, not a wire-compatibility
one). We match that: renaming a field is compatible at every level
if the tag number and wire type are unchanged. The column name in
the catalog changes, which surfaces at the SQL boundary, but that's
upstream of the compat check.

### 6.7 `reserved` enforcement

If `S_old` has `reserved 7;` and `S_new` reintroduces field number
7 with a new name, that's a **BACKWARD break** regardless of type.
The `reserved` marker is a hard contract; old data may carry a
field 7 with wire bytes of some prior type, and reinterpreting
those under a new projection is a data integrity violation.

### 6.8 Worked BACKWARD pass

```proto
// v1
syntax = "proto3";
message Event {
  int64  id         = 1;
  string kind       = 2;
}

// v2 — added a new optional field at tag 3
syntax = "proto3";
message Event {
  int64  id         = 1;
  string kind       = 2;
  google.protobuf.Timestamp occurred_at = 3;  // new optional
}
```

BACKWARD: new reads old — `occurred_at` is absent, projects to
null. Pass.

### 6.9 Worked BACKWARD fail

```proto
// v1
syntax = "proto3";
message Event {
  int64  id    = 1;
  string kind  = 2;
}

// v2 — type change at tag 2
syntax = "proto3";
message Event {
  int64 id    = 1;
  int32 kind  = 2;  // was string
}
```

Old wire bytes had wire type 2 (LEN) at tag 2. New consumer expects
wire type 0 (VARINT). Decode fails on every old record. Reject at
compat time.

## 7. Testing

### 7.1 Translator tests

`ProtobufFormatTranslatorTest` covers:

- Every row in §2's mapping table (~25 rows), each as an individual
  `.proto` input with a golden `RowType` assertion.
- Every rejection vector from §3 — `oneof` (non-synthetic),
  `google.protobuf.Any`, recursive `message`, `proto2 required`.
- Well-known-type mapping for every row in §4.
- proto3 `optional` unwrap (synthetic oneof).

### 7.2 Codec tests

`ProtobufCodecTest` round-trips on a 12-field record including
`repeated LineItem`, `map<string,string>`,
`google.protobuf.Timestamp`, and a nested `repeated` of enums.
Test shape:

1. Build a `DynamicMessage` oracle with random values (using a
   randomised fixture helper — not hardcoded sample data per the
   AGENTS.md directive).
2. Serialise the oracle to bytes.
3. Decode via `ProtobufCodec` to `BinaryRow`.
4. Encode via `ProtobufCodec` back to bytes.
5. Assert bytes round-trip (either equal, or decode-to-same-oracle
   if canonical-byte equality fails due to packed/unpacked
   differences).

32 random trials per shape.

### 7.3 Compat tests

`ProtobufCompatibilityCheckerTest` is a 7-level × 8-case matrix
(4 pass + 4 fail per level), using hand-written `.proto` pairs for
readability. Cases cover:

- Add optional field at new tag.
- Remove field with `reserved`.
- Remove field without `reserved`.
- Rename field at unchanged tag.
- Widen `int32 → int64` at unchanged tag.
- Narrow `int64 → int32` at unchanged tag.
- Change wire type at unchanged tag.
- Reintroduce a reserved tag.

## 8. Why clean-room the compat checker

`io.confluent:kafka-protobuf-provider:7.5.x` contains
`ProtobufSchemaDiff`, which already implements these rules. We do
**not** shade it. Three reasons:

1. **License.** The Confluent Community License (CCL) on
   `kafka-protobuf-provider` forbids redistributing the library in
   a competing managed-service product, and is not on the Apache
   license whitelist. Compile-scope dependencies in Fluss must be
   Apache-2.0, BSD or MIT; CCL fails this gate. See plan §27.7's
   licensing sweep.

2. **Transitive weight.** `kafka-protobuf-provider` pulls
   `kafka-schema-registry-client`, which pulls the Avro SR
   semantics, which we already re-implement in Fluss. Shading it
   would double our SR surface.

3. **Semantic divergence.** We want the compat checker to slot
   into the `CompatibilityChecker` SPI (0009) and emit rejection
   messages that cite both the Protobuf rule and the corresponding
   Fluss evolution rule. The Kafka SR implementation doesn't
   speak Fluss's language. Wrapping it would be ~200 LOC of
   translation anyway; implementing cleanly is ~600 LOC.

The upshot is a ~600-LOC clean-room implementation in
`ProtobufCompatibilityChecker` alongside the Avro and JSON
siblings. The `kafka-protobuf-serializer` *is* used (test-scope
only) in `SchemaRegistryProtobufITCase` to prove wire compat —
the CCL does not restrict test-scope use.

## 9. Dependencies

New compile-scope additions (plan §27.9):

```xml
<protobuf.version>3.25.5</protobuf.version>
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java</artifactId>
  <version>${protobuf.version}</version>
</dependency>
<dependency>
  <groupId>com.google.protobuf</groupId>
  <artifactId>protobuf-java-util</artifactId>
  <version>${protobuf.version}</version>
</dependency>
```

Both are Apache-2.0. `protobuf-java-util` carries the `.proto` text
parser; `protobuf-java` alone reads serialised descriptors only.

Test-scope:

```xml
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-protobuf-serializer</artifactId>
  <version>7.5.x</version>
  <scope>test</scope>
</dependency>
```

CCL, test-scope only. Drives `SchemaRegistryProtobufITCase`.

## 10. Deferred

- `extensions` / `extend` (proto2). Not emitted by Kafka SR.
- `group` (proto2). Deprecated, not emitted.
- `service` + `rpc`. Not payload-relevant; currently ignored on
  parse, not erroring.
- `Any` support via schema references (Phase SR-X.5 prerequisite).
- Custom options (`option (my.foo) = ...`). Ignored silently; if
  an option changes a wire behaviour (`deprecated`, `packed`) we
  honour it; purely-annotative options are dropped.
- Stream-of-records compaction (a message with a single
  `google.protobuf.Any` payload field). Could be supported if/when
  we lift the `Any` rejection.

## 11. Open questions

1. **uint64 as signed BIGINT with a property**. Do we want a
   dedicated `UNSIGNED_BIGINT` in Fluss core? Currently no — that's
   a fluss-core edit, and T-MF is bolt-on-only. Revisit when Phase
   T.2 / T.3 gets the green light.
2. **Packed vs unpacked on encode**. We always emit packed for
   proto3 scalar repeateds (per the spec). Should we emit unpacked
   when a downstream claims proto2? No, because we don't know;
   protocol version selection is a per-subject decision, not
   per-message.
3. **Eviction of sub-codecs**. Each nested message descriptor gets
   its own compiled sub-codec. A deeply-nested schema can inflate
   the metaspace footprint. Currently we count sub-codecs per
   (tableId, schemaId) and cache only the root; sub-codecs are
   heap-referenced through the root. Good enough for now.

## 12. References

- Plan §27.5, §27.7, §27.11, §27.13.
- Design 0002 (Fluss-first SR + hot-path codec).
- Design 0007 (JSON Schema sibling).
- Design 0009 (multi-format registry SPI).
- Protobuf language guide:
  https://protobuf.dev/programming-guides/proto3/
- Kafka SR Protobuf serializer:
  `io.confluent:kafka-protobuf-serializer:7.5.x` — test scope only.
