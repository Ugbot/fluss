# Design 0007 — JSON Schema translator, codec and compatibility checker

**Status:** Draft (T-MF in progress), 2026-04-24
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0008-protobuf-translator.md`, `0009-multi-format-registry.md`,
and the structural plan at
`~/.claude/plans/check-which-branch-should-encapsulated-karp.md` §27.4.

## 0. Why this document exists

Phase T-MF extends the Fluss-first Schema Registry (design 0002) from
Avro-only to three formats: Avro, JSON Schema, Protobuf. The Avro
translator landed in Phase T.1 (`AvroFormatTranslator`,
`AvroCodecCompiler`, `CompiledCodecCache`). This document specifies
the JSON Schema side: how a submitted JSON Schema text is translated
to a Fluss `RowType`, how the hot-path encoder/decoder is compiled,
what compatibility rules apply at each of the seven Confluent levels,
and what we deliberately reject.

The design is anchored to three user-locked constraints from §27.1
of the plan:

1. Full type coverage on the translator side. If the translator
   accepts a shape, the codec compiler must emit working code for it.
2. No fluss-core edits. Everything here lives in `fluss-kafka/`.
3. Confluent wire compatibility. Bytes produced by
   `io.confluent:kafka-json-schema-serializer:7.5.x` must decode
   without loss, and bytes this codec emits must decode through the
   Confluent deserializer without modification.

## 1. Scope

### 1.1 Draft versions

We accept JSON Schema drafts 4, 6 and 7. This is exactly the set
that Confluent's `kafka-json-schema-serializer` emits in practice;
drafts 2019-09 and 2020-12 add dynamic keywords (`$dynamicRef`,
`$vocabulary`, `unevaluatedProperties`) that have no stable
projection onto a static `RowType`, and they are not emitted by any
Confluent-shipped serializer today. They are rejected at register
time with a clear 422 citing the draft version.

Draft detection uses, in order:

1. The `$schema` keyword at the top of the document
   (`http://json-schema.org/draft-07/schema#` etc.).
2. Absence of `$schema` — treated as draft 7 (Confluent's default).
3. A mixed-draft document (`$schema` inside a nested subtree
   differing from the root) — rejected.

### 1.2 What "translator" means here

A translator is a pure, total function
`(String text) -> RowType`. It performs:

- Lexical parsing via the shaded Jackson `ObjectMapper`.
- A recursive walk of the resulting `JsonNode` tree.
- Production of a canonical Fluss `RowType`, including nullability
  and column properties (`logicalType`, `precision`, `scale`,
  `allowedValues`).

The translator does **not** validate sample instances against the
schema. It does not negotiate drafts; each document is interpreted
under exactly one draft.

## 2. Type mapping

The mapping below is the source of truth. Each row is exercised by a
passing + failing test in `JsonSchemaFormatTranslatorTest` (§6). The
table is a superset of plan §27.4 — it was expanded during drafting
to cover every distinct JSON Schema construct emitted by Confluent's
serializer.

| JSON Schema construct                                          | Fluss `DataType`                                     | Notes                                                                                                 |
|----------------------------------------------------------------|------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `{"type": "boolean"}`                                          | `BOOLEAN`                                            | —                                                                                                     |
| `{"type": "integer"}`                                          | `BIGINT`                                             | Default width; JSON Schema `integer` is unbounded.                                                    |
| `{"type": "integer", "format": "int32"}`                       | `INT`                                                | Confluent's serializer emits `format: int32` for Java `int`.                                          |
| `{"type": "integer", "minimum": x, "maximum": y}`              | `INT` / `BIGINT`                                     | If `[min, max]` fits in 32-bit signed, narrow to `INT`; otherwise `BIGINT`.                           |
| `{"type": "number"}`                                           | `DOUBLE`                                             | —                                                                                                     |
| `{"type": "number", "format": "float"}`                        | `FLOAT`                                              | —                                                                                                     |
| `{"type": "number", "multipleOf": 0.01}`                       | `DECIMAL(p,s)`                                       | `scale` inferred from the exponent of `multipleOf`; precision capped at 38. See §4.3.                 |
| `{"type": "string"}`                                           | `STRING`                                             | UTF-8.                                                                                                |
| `{"type": "string", "format": "byte"}`                         | `BYTES`                                              | Base64-encoded on the wire; codec decodes with `java.util.Base64`.                                    |
| `{"type": "string", "format": "date"}`                         | `DATE`                                               | ISO-8601 `YYYY-MM-DD`.                                                                                |
| `{"type": "string", "format": "date-time"}`                    | `TIMESTAMP_LTZ(3)`                                   | RFC 3339 / ISO-8601. See §4.2.                                                                        |
| `{"type": "string", "format": "uuid"}`                         | `STRING` + property `logicalType=uuid`               | No native UUID type; serialised as 36-char string.                                                    |
| `{"type": "string", "enum": [...]}`                            | `STRING` + property `allowedValues=[...]`            | Enum values recorded, not enforced at codec boundary; enforcement is translator-only at register.     |
| `{"type": "array", "items": T}`                                | `ARRAY<T>`                                           | —                                                                                                     |
| `{"type": "array", "items": T, "minItems": n, "maxItems": m}`  | `ARRAY<T>` + properties `minItems`, `maxItems`       | Bounds recorded as column properties; not enforced on encode.                                         |
| `{"type": "object", "properties": {...}}`                      | `ROW<...>`                                           | Fields typed by the sub-schema; `required` list drives nullability (see §3.1).                        |
| `{"type": "object", "additionalProperties": T}` (no `properties`) | `MAP<STRING, T>`                                  | JSON object keys are always strings.                                                                  |
| `{"type": "object", "properties": {...}, "additionalProperties": true}` | `ROW<...>` (additional ignored)             | We emit only declared columns; unknown keys are dropped on decode (logged at DEBUG).                  |
| `{"oneOf": [{"type": "null"}, T]}` (and mirrored 2-form)       | nullable `T`                                         | The nullable-union idiom; equivalent to Avro `[null, T]`.                                             |
| `{"oneOf": [...]}` (any other shape)                           | — rejected                                           | Sum types not expressible as `RowType`.                                                               |
| `{"anyOf": [...]}`                                             | — rejected                                           | Same reason.                                                                                          |
| `{"type": ["null", T]}` (draft-4 style)                        | nullable `T`                                         | Equivalent to the `oneOf` idiom.                                                                      |
| `{"type": [A, B, ...]}` (3+ non-null)                          | — rejected                                           | No `UNION` type in `RowType`.                                                                         |
| `{"$ref": "#/definitions/X"}`                                  | resolved                                             | In-file only; see §5.                                                                                 |
| `{"$ref": "other.json#/..."}`                                  | — rejected                                           | Cross-file refs deferred; no retrieval surface in T-MF.                                               |
| Recursive `$ref` (node references itself transitively)         | — rejected                                           | `RowType` is tree-shaped; cycle breaks the encoder.                                                   |
| `{"const": v}`                                                 | `STRING` / `INT` / … + property `const=v`            | Type inferred from value; enforcement translator-only.                                                |
| `{"if": ..., "then": ..., "else": ...}`                        | — rejected (deferred)                                | Schema-dependent typing is draft-7; deferred. See §9.                                                 |
| `{"patternProperties": {...}}`                                 | — rejected (deferred)                                | Regex-keyed maps have no stable `RowType`; deferred.                                                  |
| `{"contains": ..., "minContains": n}`                          | — rejected (deferred)                                | Array contents predicate has no structural projection.                                                |
| `{"not": ...}`                                                 | — rejected                                           | Negation does not project.                                                                            |

Whenever a row says "rejected", the translator throws
`SchemaTranslationException` with a clear message citing the
offending JSON pointer (`/properties/foo/oneOf` etc.). The SR HTTP
handler maps this to HTTP 422.

### 2.1 Comparison with Avro

Where an Avro shape and a JSON Schema shape project to the same
Fluss type (e.g. `record` ↔ `object+properties` both map to `ROW`,
Avro `[null, T]` ↔ JSON `oneOf[null,T]` both map to nullable `T`),
a schema evolution across formats is meaningful: a subject can
evolve from one format to another if the projection is equal. This
is not exercised in T-MF (we forbid format changes on an existing
subject, §5 of 0009) but keeps the door open.

## 3. Nullability and the nullable-union idiom

### 3.1 Required list drives nullability

In JSON Schema a property is nullable unless it appears in the
enclosing object's `required` list. The translator treats absence
from `required` as nullability:

```json
{
  "type": "object",
  "properties": {
    "id":   {"type": "integer", "format": "int32"},
    "name": {"type": "string"}
  },
  "required": ["id"]
}
```

maps to `ROW<id INT NOT NULL, name STRING>`.

### 3.2 Explicit nullable union

Confluent's serializer emits nullable fields using `oneOf` with
`null`:

```json
{"oneOf": [{"type": "null"}, {"type": "integer", "format": "int32"}]}
```

This is the only `oneOf` the translator accepts. Position does not
matter — both `[null, T]` and `[T, null]` are normalised. The field
projects to nullable `INT`.

### 3.3 Mixed nullable-union + required

If a field is declared nullable via `oneOf` *and* included in the
parent `required` list, we apply the stricter rule: the column is
nullable, because JSON Schema evaluates null as a valid instance of
the union even when the key is present. This matches Confluent's
validator.

### 3.4 Rejection set (sum types)

Three distinct constructs all imply a sum type and are all rejected:

```json
// (a) 3-way oneOf
{"oneOf": [{"type": "string"}, {"type": "integer"}, {"type": "boolean"}]}

// (b) Non-null 2-way oneOf
{"oneOf": [{"type": "string"}, {"type": "integer"}]}

// (c) type array with 3+ entries
{"type": ["string", "integer", "boolean"]}

// (d) anyOf (any arity)
{"anyOf": [{"type": "string"}, {"type": "integer"}]}
```

All four throw `SchemaTranslationException: sum type not expressible
in RowType at <JSON pointer>`.

## 4. Logical-type handling

JSON Schema signals logical types through the `format` keyword (a
string hint) and through `multipleOf` (a numeric hint). Both are
advisory in draft 7; Confluent's serializer treats them as binding.
So do we.

### 4.1 `format: date`

```json
{"type": "string", "format": "date"}
```

Projects to `DATE`. Encode: ISO-8601 `YYYY-MM-DD`. Decode parses via
`java.time.LocalDate.parse`; any non-ISO string raises
`CodecEncodingException` at the row level, which the transcoder
maps to `INVALID_RECORD`.

### 4.2 `format: date-time`

```json
{"type": "string", "format": "date-time"}
```

Projects to `TIMESTAMP_LTZ(3)`. Encode writes a canonical form
`YYYY-MM-DDTHH:MM:SS.sssZ`. Decode accepts any RFC-3339 variant
(fractional-second precision 0–9, offset `Z` or `±HH:MM`), rounds
to milliseconds. Nanoseconds beyond millisecond precision are
silently truncated, matching Confluent's behaviour.

### 4.3 `format: byte` and `multipleOf` decimals

- `format: byte` on `type: string` projects to `BYTES`. Wire uses
  Base64. Codec uses `java.util.Base64.getDecoder()` (no dependency
  on commons-codec).
- `multipleOf: 10^-n` on `type: number` is the decimal idiom.
  Precision `p` is inferred from the schema's `minimum` / `maximum`
  bounds if present (`ceil(log10(max(|min|, |max|)))` plus scale),
  capped at 38. If bounds are missing, default to `DECIMAL(38, n)`.
  A `multipleOf` whose exponent is not a non-positive integer
  (`0.01` yes, `0.015` no) is rejected.

Worked example:

```json
{"type": "number", "multipleOf": 0.0001,
 "minimum": -99999.9999, "maximum": 99999.9999}
```

Projects to `DECIMAL(9, 4)`: the largest-magnitude bound is 99999.9999,
which has 9 significant digits; scale is 4.

## 5. `$ref` resolution

### 5.1 In-file only

JSON Schema allows `$ref` to point anywhere via a JSON pointer. We
support two idioms:

- `#/definitions/Name` (draft-4/6 style).
- `#/$defs/Name` (draft-7 style).

Both are resolved by inlining: the translator keeps a map
`JsonPointer -> JsonNode`, and when it encounters a `$ref` it
substitutes the pointed-to sub-tree and continues the walk. A
`$ref` stack is kept alongside the walk to detect cycles; any
`$ref` whose target is an ancestor in the walk stack raises
`SchemaTranslationException: recursive $ref at <pointer>`.

### 5.2 Cross-file deferred

`$ref: "other.json#/..."` would require the translator to fetch a
second document. The SR HTTP surface does not expose a retrieval
for sibling subjects in the same format (that's scheduled for Phase
SR-X.5, schema references). Rather than bake half a resolver into
T-MF, we reject cross-file refs with a 422. The error message
points operators at the `references` array on `POST
/subjects/{s}/versions`, which is accepted-but-ignored today.

### 5.3 Pointers that leave the schema

`{"$ref": "#/not/a/real/path"}` yields
`SchemaTranslationException: $ref target not found`. The walk is
aborted; no partial `RowType` is returned.

### 5.4 Worked example

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "customer": {"$ref": "#/definitions/Customer"},
    "billing":  {"$ref": "#/definitions/Customer"}
  },
  "required": ["customer"],
  "definitions": {
    "Customer": {
      "type": "object",
      "properties": {
        "id":   {"type": "integer", "format": "int32"},
        "name": {"type": "string"}
      },
      "required": ["id", "name"]
    }
  }
}
```

Projects to:

```
ROW<
  customer ROW<id INT NOT NULL, name STRING NOT NULL> NOT NULL,
  billing  ROW<id INT NOT NULL, name STRING NOT NULL>
>
```

Both references are inlined; the resulting tree is acyclic.

## 6. Compatibility rules (seven levels)

Confluent SR exposes seven compatibility levels: `NONE`, `BACKWARD`,
`BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`,
`FULL_TRANSITIVE`. `JsonCompatibilityChecker` implements the SPI
defined in 0009 and applies these rules in the JSON Schema domain
(not the Fluss domain — that's a deliberate choice; see §6.8).

The rules compare two normalised `JsonNode` trees. Normalisation
inlines `$ref`, sorts `required` arrays, and normalises
`oneOf[null,T]` to a canonical `[null, T]` order.

Notation: `S_old` and `S_new` are the prior and proposed schemas; a
change is BACKWARD-compatible iff a consumer using `S_new` can read
data produced under `S_old`.

### 6.1 `NONE`

Always returns `Compatible`. Every pair of schemas passes. Used for
dev / experimental subjects.

**Pass**: any change at all.
**Fail**: none.

### 6.2 `BACKWARD`

Consumer using `S_new` reads data produced under `S_old`.
Compatible changes (rule-by-rule):

- **Removing a property from `required`**: old data had the field,
  new consumer is tolerant of its absence (well, doesn't require
  it). OK.
- **Adding a new optional property**: old data lacked it, new
  consumer accepts null. OK.
- **Widening an integer to a wider integer**: `format: int32` →
  (no format), `int32` → `int64`. OK.
- **Relaxing bounds**: raising `maximum`, lowering `minimum`. OK.

Incompatible changes:

- **Adding a new property to `required`** (without providing a
  default): old data lacks the field; new consumer rejects.
- **Removing a property that was previously emitted**: old data has
  an unknown field; new consumer with `additionalProperties: false`
  rejects.
- **Narrowing a type**: `string` → `integer`; `number` →
  `integer` (even with `multipleOf: 1`).
- **Tightening bounds**: raising `minimum`, lowering `maximum`.

Pass example:

```json
// S_old
{"type": "object",
 "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
 "required": ["id"]}
// S_new  (added optional field "email")
{"type": "object",
 "properties": {"id":    {"type": "integer"},
                "name":  {"type": "string"},
                "email": {"type": "string"}},
 "required": ["id"]}
```

Fail example:

```json
// S_old
{"type": "object",
 "properties": {"id": {"type": "integer"}},
 "required": ["id"]}
// S_new  (added required "email" without default → BACKWARD break)
{"type": "object",
 "properties": {"id": {"type": "integer"}, "email": {"type": "string"}},
 "required": ["id", "email"]}
```

### 6.3 `BACKWARD_TRANSITIVE`

Same rules as `BACKWARD`, but `S_new` is compared against *every*
prior non-deleted version, not just the most recent. A subject with
versions `v1 → v2 → v3` registering `v4` must pass BACKWARD against
`v1`, `v2`, *and* `v3`.

Pass example: the same optional-field addition from 6.2, applied
consistently across all prior versions.

Fail example: if `v2` dropped the `id` field, a `v4` that reintroduces
`id` as required is BACKWARD against `v3` (no `id` there, new has
`id` optional — fine) but fails against `v1` (old `id` as integer
might not match new `id` as string).

### 6.4 `FORWARD`

Old consumer using `S_old` reads data produced under `S_new`.
Compatible changes:

- **Removing a property**: data no longer has it; old consumer
  reads null if it's in its `required` list — depends on presence
  semantics.
- **Narrowing an enum or bound**: old consumer sees a subset of
  values it already tolerates.
- **Adding a new property to `required`**: only if every new
  value fits under the old consumer's schema. In practice, adding
  required fields is `FORWARD`-compatible.

Incompatible changes:

- **Widening a type**: old consumer rejects the broader domain.
- **Adding an optional property without default**: old consumer
  (if strict) may reject unknown.
- **Relaxing bounds**: old consumer rejects new values outside old
  bound.

Pass example:

```json
// S_old
{"type": "object",
 "properties": {"status": {"type": "string",
                           "enum": ["NEW", "OLD", "DONE"]}}}
// S_new  (narrowed enum)
{"type": "object",
 "properties": {"status": {"type": "string",
                           "enum": ["NEW", "DONE"]}}}
```

Fail example:

```json
// S_old
{"type": "object",
 "properties": {"count": {"type": "integer", "format": "int32"}}}
// S_new  (widened to int64 — old consumer overflow)
{"type": "object",
 "properties": {"count": {"type": "integer"}}}
```

### 6.5 `FORWARD_TRANSITIVE`

`FORWARD` against every prior version. Same shape as 6.3.

### 6.6 `FULL`

`FULL = BACKWARD ∧ FORWARD` between `S_new` and the immediately
preceding version. Compatible changes are the intersection:
essentially only no-op edits (whitespace, doc changes, `$id`
renames) or purely-structural reorderings.

Pass example: reordering properties in an object (JSON Schema
object semantics are unordered, but the checker normalises
property-key order).

Fail example: adding an optional field is `BACKWARD`-only and
fails `FULL`.

### 6.7 `FULL_TRANSITIVE`

`FULL` against every prior version. Rarely achievable in practice;
useful as a discipline for stable public contracts.

### 6.8 Why JSON Schema compat, not Fluss compat

Design 0002 §Core Principle 4 says evolution rules are Fluss's.
For JSON Schema this is technically wrong in one direction:
Confluent's JSON compat is a *weaker* set of rules than Fluss's
strictly-additive evolution, because JSON Schema has no column
types in the Fluss sense (it has property schemas + required
lists, and the compatibility rules are woven through both). We
apply the JSON Schema rules at the wire-interface layer so that
our error messages match what Confluent tooling surfaces, and we
additionally enforce Fluss's evolution rules when we translate the
accepted schema to a Fluss `alterTable`. That double-check is in
0009; a schema that passes `JsonCompatibilityChecker` but would
violate Fluss's additive evolution is rejected at the `alterTable`
boundary with a 422 citing both rules.

## 7. Hot-path codec

### 7.1 Compilation

`JsonCodecCompiler.compile(RowType, schemaId)` emits a
`RecordCodec` subclass via Janino. The emitted source:

```java
public final class JsonCodec_42 implements RecordCodec {
  public int decodeInto(ByteBuf src, int off, int len,
                        BinaryRowWriter dst) throws IOException {
    JsonParser p = JSON_FACTORY.createParser(
        new ByteBufferBackedInputStream(src, off, len));
    if (p.nextToken() != JsonToken.START_OBJECT) { ... }
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      String name = p.currentName();
      p.nextToken();  // into value
      switch (name) {
        case "id":
          dst.writeInt(0, p.getIntValue());
          break;
        case "name":
          dst.writeString(1, p.getText());
          break;
        default:
          p.skipChildren();  // unknown, drop
      }
    }
    return len;
  }

  public int encodeInto(BinaryRow src, ByteBuf dst) throws IOException {
    try (JsonGenerator g = JSON_FACTORY.createGenerator(
             new ByteBufOutputStream(dst))) {
      g.writeStartObject();
      if (!src.isNullAt(0)) { g.writeNumberField("id", src.getInt(0)); }
      if (!src.isNullAt(1)) { g.writeStringField("name", src.getString(1)); }
      g.writeEndObject();
      return dst.writerIndex() - startMark;
    }
  }

  public short schemaVersion() { return 7; }
  public RowType rowType() { return ROW_TYPE; }
}
```

The switch on field name is compiled to a tableswitch by javac /
Janino for ≥ 8 cases, dense-indexed by `String.hashCode()`; for
smaller records it stays a linear if-chain and the JIT inlines it
after a few invocations.

### 7.2 Streaming, not tree-materialised

No `JsonNode` is allocated on the hot path. `JsonParser` produces
tokens one at a time, the emitted `switch` dispatches to a
primitive reader, the `BinaryRowWriter` writes directly to its
backing buffer. Unknown fields are skipped in one pass via
`p.skipChildren()`.

### 7.3 Per-format runtime

`JsonCodecRuntime` carries:

- Base64 decode/encode for `format: byte`.
- `LocalDate.parse` / `Instant.parse` for temporal formats.
- `BigDecimal.setScale(s, HALF_UP)` for `DECIMAL` scale adjustment.
- A pooled `JsonFactory` (Jackson objects are thread-safe when
  shared).

### 7.4 Performance gate

Phase T.1 locked the Avro codec benchmark gate at 20 % of
hand-written baseline (design 0002 §Benchmark gates). JSON is
slower than Avro by construction — JSON parsing is O(n) with
character-class branching; Avro is O(n) with fixed integer reads.
We do **not** gate JSON at 20 % of the Avro baseline; we gate it at
**20 % of a hand-written Jackson `JsonParser` → POJO decoder** of
the same record shape. Benchmarks live in `fluss-jmh` under
`JsonCodecDecodeBench`, deferred to Phase T.2 when the hot path is
actually exercised (T-MF does not wire the codec into produce /
fetch yet; see §27.13 of the plan).

## 8. Codec cache and compilation cost

`CompiledCodecCache` (introduced in T.1) is keyed by
`(tableId, schemaId)` — unchanged for JSON. The entry is a
`RecordCodec` cast from the format-specific subclass. Compilation is
~40 ms for a 12-field flat record on a modern JVM, dominated by
Janino. First request pays the cost; subsequent requests are a
concurrent-hashmap lookup.

Eviction: none in T-MF. Design 0002 flags this as an open question
(compiled classes are ~10 KB, 10k cached = 100 MB of metaspace).
Revisit if operators report pressure.

## 9. Deferred keywords

The following JSON Schema keywords have no stable `RowType`
projection under this design. They are rejected at translate time
with a clear error, and collected here so a future reviewer can find
them:

- `if` / `then` / `else` — schema-dependent typing. A principled
  projection would unfold into sum types, which we don't support.
- `patternProperties` — regex-keyed maps. Our `MAP<STRING, V>` has no
  key constraint; accepting `patternProperties` would require
  per-row key validation at the codec boundary.
- `contains` / `minContains` / `maxContains` — array-contents
  predicates. These are runtime assertions; we don't run them.
- `not` — negation. Has no structural projection.
- `const` with > 32 enumerated values on a different parent — we
  accept up to 32 for `enum`; beyond that we project to the base
  type and drop the allowed-values property (a warning is logged).
  Rationale: storing a 10k-item allowed-values list in column
  metadata bloats the catalog row.
- `dependentRequired` / `dependentSchemas` — conditional required.
  Same reasoning as `if`/`then`/`else`.
- `$anchor` / `$defs` cross-anchor — we support `#/$defs/Name`
  only.
- `readOnly` / `writeOnly` — advisory hints; ignored silently.
- `title` / `description` / `examples` — copied to column comments
  where the first two match, the rest ignored.

When Fluss adds union types (if ever), `oneOf` and `anyOf` can be
lifted; this document would then be revised.

## 10. Open questions

1. **Decimal precision inference from `maximum` alone**. Today we
   infer from both bounds; missing either falls back to
   `DECIMAL(38, s)`. Operators emit schemas without bounds often;
   is 38 too wide? Benchmark before tightening.
2. **Unknown-field logging cadence**. On decode, unknown fields are
   silently skipped. Should they surface as a metric
   (`kafka.sr.json.unknown_field_total`)? Low cost, probably yes;
   T-MF.5 adds the meter.
3. **Confluent's magic-byte envelope**. The JSON codec decodes the
   body only; the 5-byte `[0x00][id_be_i32]` prefix is stripped by
   the transcoder. Does the codec need to assert the prefix
   matches its `schemaId()`? Currently no — the transcoder already
   resolved the id to pick this codec. Revisit if produce-side
   routing becomes more speculative.

## 11. References

- Plan §27.4, §27.11, §27.13.
- Design 0002 (Fluss-first SR + hot-path codec).
- Design 0009 (multi-format registry SPI).
- `AvroFormatTranslator.java` — sibling implementation, same SPI.
- JSON Schema draft-07:
  https://json-schema.org/draft-07/json-schema-release-notes.html
- Confluent JSON Schema serializer:
  `io.confluent:kafka-json-schema-serializer:7.5.x` — test-scope only.
