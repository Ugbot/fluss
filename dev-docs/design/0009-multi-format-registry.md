# Design 0009 — Multi-format registry, SPI, and HTTP dispatch

**Status:** Draft (T-MF in progress), 2026-04-24
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0007-json-schema-translator.md`, `0008-protobuf-translator.md`,
and the structural plan at
`~/.claude/plans/check-which-branch-should-encapsulated-karp.md` §27.3.

## 0. Purpose

Design 0002 specified a Fluss-first Schema Registry with Avro as the
day-one format. Phase T.1 landed the Avro translator + compiled
codec. Phase T-MF extends the registry to three formats (Avro, JSON
Schema, Protobuf) behind a common SPI. This document specifies:

1. The `FormatRegistry` discovery mechanism.
2. The `FormatTranslator` and `CompatibilityChecker` SPIs each
   format implements.
3. How the SR HTTP surface dispatches on `schemaType` through the
   registry.
4. How the catalog's `SchemaVersionEntity.format` column drives
   read-path dispatch.
5. The caching strategy: per-format codec compilation keyed by
   `(tableId, schemaId)`, with format baked into the compiled class
   so the hot path has no runtime dispatch.
6. What it takes to add a fourth format.

The design answers a single constraint: **extending the SR to three
formats must not break the Avro hot path.** Everything here is
structured to keep that promise.

## 1. SPIs

### 1.1 `FormatTranslator`

Unchanged from Phase T.1. Full signature (plan §27.3):

```java
package org.apache.fluss.kafka.sr.typed;

public interface FormatTranslator {
  /** Stable format id used by the registry and HTTP surface. */
  String formatId();   // "AVRO" | "JSON" | "PROTOBUF" | ...

  /** Translate submitted text to a canonical Fluss RowType. */
  RowType translateTo(String schemaText);

  /** Project a Fluss RowType back to this format's text (for GET). */
  String projectFrom(RowType rowType);
}
```

The contract is that `projectFrom(translateTo(text))` is
deterministically equal to the normalised form of `text` (whitespace
dropped, field order canonicalised). This equality gives us the
"no stored Avro/Proto/JSON schema text" invariant from 0002:
`GET /schemas/ids/{id}` re-derives the text rather than retrieving
it.

Each format implements this interface:

- `AvroFormatTranslator` — landed in T.1, commit `9d476651`.
- `JsonSchemaFormatTranslator` — landed in T-MF.2, spec 0007.
- `ProtobufFormatTranslator` — landed in T-MF.3, spec 0008.

### 1.2 `CompatibilityChecker`

New in T-MF.1 (plan §27.3). Compatibility checking was collapsed
inside `AvroCompatibilityChecker` in T.1; we extract the SPI so
every format plugs in.

```java
package org.apache.fluss.kafka.sr.compat;

public interface CompatibilityChecker {
  /** Same stable id as the sibling FormatTranslator. */
  String formatId();

  /**
   * Check whether {@code proposed} is compatible with every schema
   * in {@code priorTexts} under the given compat level. Returns a
   * {@link CompatibilityResult} — either Compatible or Incompatible
   * with a list of rule violations.
   */
  CompatibilityResult check(
      String proposedText,
      List<String> priorTexts,
      CompatLevel level);
}
```

`CompatLevel` and `CompatibilityResult` already exist (T.1); they
move from `sr/typed/` to `sr/compat/` alongside the SPI. Every
format implements this interface:

- `AvroCompatibilityChecker` — existed in T.1, refactored to
  implement SPI in T-MF.1.
- `JsonCompatibilityChecker` — landed in T-MF.2.
- `ProtobufCompatibilityChecker` — landed in T-MF.3.

### 1.3 Why two SPIs, not one

A translator is called on every register (once per subject version);
a compat checker is called on every register and every
`POST /compatibility/subjects/.../versions/latest`. They have
different lifetimes and different test surfaces. Bundling would
make rate-limiting, error reporting, and caching policies
harder to express separately. The registry exposes both.

## 2. `FormatRegistry`

### 2.1 Shape

```java
package org.apache.fluss.kafka.sr.typed;

public final class FormatRegistry {
  public static final FormatRegistry INSTANCE = bootstrap();

  public Optional<FormatTranslator> translator(String formatId);
  public Optional<CompatibilityChecker> checker(String formatId);
  public List<String> formatIds();   // case-insensitive; canonical uppercase

  private static FormatRegistry bootstrap() {
    Map<String, FormatTranslator> translators = new HashMap<>();
    for (FormatTranslator t :
         ServiceLoader.load(FormatTranslator.class)) {
      translators.put(t.formatId().toUpperCase(Locale.ROOT), t);
    }
    Map<String, CompatibilityChecker> checkers = new HashMap<>();
    for (CompatibilityChecker c :
         ServiceLoader.load(CompatibilityChecker.class)) {
      checkers.put(c.formatId().toUpperCase(Locale.ROOT), c);
    }
    return new FormatRegistry(translators, checkers);
  }
}
```

Lookup is `Optional`-returning — callers decide what to do with an
unknown format (the HTTP handler returns 422, the
`SchemaRegistryService` throws).

Keys are normalised to uppercase on both insert and lookup. The
wire surface is case-insensitive (`"avro"` and `"AVRO"` resolve the
same translator), matching Confluent.

`formatIds()` drives `GET /schemas/types`. After T-MF this returns
`["AVRO", "JSON", "PROTOBUF"]` in stable order.

### 2.2 `ServiceLoader` discovery

`META-INF/services/` layout (plan §27.3):

```
fluss-kafka/src/main/resources/META-INF/services/
├── org.apache.fluss.kafka.sr.typed.FormatTranslator
└── org.apache.fluss.kafka.sr.compat.CompatibilityChecker
```

Contents of the first:

```
org.apache.fluss.kafka.sr.typed.AvroFormatTranslator
org.apache.fluss.kafka.sr.typed.JsonSchemaFormatTranslator
org.apache.fluss.kafka.sr.typed.ProtobufFormatTranslator
```

Contents of the second:

```
org.apache.fluss.kafka.sr.compat.AvroCompatibilityChecker
org.apache.fluss.kafka.sr.compat.JsonCompatibilityChecker
org.apache.fluss.kafka.sr.compat.ProtobufCompatibilityChecker
```

The `FormatRegistry` builds once at class-load time. A duplicate
`formatId()` (two translators claiming `"JSON"`) throws at
bootstrap — we fail fast rather than pick an arbitrary one.

### 2.3 Why `ServiceLoader` and not an @ annotation-scan

Three reasons:

1. **Java 8 compatibility.** Per AGENTS.md §1,
   `fluss-kafka` must compile with JDK 8. Annotation-scanning
   solutions like Spring's classpath scanner or Jandex depend on
   Java 8 but bring transitive weight; `ServiceLoader` is in the
   JDK.

2. **Bolt-on discipline.** Design 0006 §Principles.1 says Kafka-
   compat code imports only `@PublicStable` / `@PublicEvolving`
   Fluss interfaces. `ServiceLoader` is JDK. An annotation-scanner
   would be a new dependency on the critical path.

3. **Test isolation.** Tests can swap service implementations by
   putting an alternate META-INF/services file on the test
   classpath. Useful for fault-injection tests against the
   registry.

### 2.4 Threading

`FormatRegistry.INSTANCE` is immutable after bootstrap.
`translator()` / `checker()` / `formatIds()` are lock-free reads on
unmodifiable maps. Every concrete translator/checker is
`@ThreadSafe` (see individual docs).

## 3. SR HTTP dispatch

The HTTP listener (`SchemaRegistryHttpHandler`, on CoordinatorServer
port 8081 per design 0002) routes on `schemaType`. Before T-MF, the
handler had three explicit `if (!"AVRO".equalsIgnoreCase(...))
reject` gates. T-MF.5 replaces those with a single registry lookup
each.

### 3.1 `POST /subjects/{s}/versions`

Request body:

```json
{
  "schema": "...",
  "schemaType": "JSON",
  "references": []
}
```

Handler logic (after T-MF.5):

```java
String formatId = req.schemaType != null ? req.schemaType : "AVRO";
FormatTranslator translator = FormatRegistry.INSTANCE
    .translator(formatId)
    .orElseThrow(() -> new SchemaTypeNotSupportedException(formatId));
CompatibilityChecker checker = FormatRegistry.INSTANCE
    .checker(formatId)
    .orElseThrow();  // always present if translator is; bootstrap invariant

RowType rowType = translator.translateTo(req.schema);
if (!priorTexts.isEmpty()) {
  CompatibilityResult result = checker.check(
      req.schema, priorTexts, subject.compatLevel());
  if (!result.isCompatible()) {
    throw new CompatibilityFailureException(result);
  }
}
// ... proceed to alterTable / createTable via MetadataManager ...
SchemaVersionEntity entity = catalog.registerSchemaVersion(
    subject, rowType, req.schema, formatId);  // format column stamped
return new RegisterResponse(entity.globalId());
```

The `SchemaTypeNotSupportedException` maps to HTTP 422 with the
message `"Unsupported schemaType: X; registered formats are Y"`,
listing `FormatRegistry.INSTANCE.formatIds()`.

### 3.2 `POST /compatibility/subjects/{s}/versions/{v}`

Confluent's "would this be compatible?" endpoint. Same dispatch:

```java
String formatId = req.schemaType != null ? req.schemaType : "AVRO";
CompatibilityChecker checker = FormatRegistry.INSTANCE
    .checker(formatId)
    .orElseThrow(() -> new SchemaTypeNotSupportedException(formatId));
CompatibilityResult result = checker.check(
    req.schema, priorTextsForVersion(v), subject.compatLevel());
return new CompatResponse(result.isCompatible());
```

No state is mutated; this is the pre-check endpoint.

### 3.3 `GET /subjects/{s}/versions/{v}`

Read path. The stored `SchemaVersionEntity.format` column drives
the translator lookup:

```java
SchemaVersionEntity entity = catalog.getSchemaVersion(subject, v);
FormatTranslator translator = FormatRegistry.INSTANCE
    .translator(entity.format())
    .orElseThrow();   // format stamped at register; guaranteed registered
String schemaText = translator.projectFrom(entity.rowType());
return new VersionResponse(
    entity.globalId(), entity.version(), entity.format(), schemaText);
```

The `orElseThrow` without a fallback is deliberate: if we stored a
format we can't re-translate, something has gone catastrophically
wrong at bootstrap (e.g. a translator was removed from the
classpath between versions). That's an operator-visible bug, not a
recoverable condition.

### 3.4 `GET /schemas/types`

Returns `FormatRegistry.INSTANCE.formatIds()`:

```json
["AVRO", "JSON", "PROTOBUF"]
```

Order is deterministic (insertion order from the META-INF/services
file, normalised to uppercase).

### 3.5 `GET /schemas/ids/{id}`

Uses the deterministic id reservation from design 0002 to find
`(tableId, schemaVersion, format)`, then projects:

```java
IdReservation r = catalog.resolveGlobalId(id);
FormatTranslator translator = FormatRegistry.INSTANCE
    .translator(r.format())
    .orElseThrow();
RowType rowType = catalog.getSchemaVersion(r.tableId(), r.schemaVersion())
    .rowType();
return new SchemaResponse(r.format(), translator.projectFrom(rowType));
```

## 4. Catalog integration

### 4.1 `SchemaVersionEntity.format`

Per plan §27.1 and design 0002 §ZK layout, the catalog already
carries a `format` column on the schema-version entity. Before
T-MF, the column was stamped to `"AVRO"` unconditionally. T-MF.5
changes the register path to stamp `formatId` from the request,
and the read path to honour the stored value.

The catalog schema change is **zero-column** — the column exists;
we just stop ignoring it. No catalog migration required.

### 4.2 Format is immutable per subject version

A given `SchemaVersionEntity.format` is fixed at register time and
never changes. A subject may in principle evolve across formats
(version 1 is AVRO, version 2 is JSON), but:

- The `CompatibilityChecker` SPI receives only same-format priors
  (the service filters `priorTexts` by format).
- The read path dispatches per-version based on the stored format.
- A format change on an existing subject is rejected at register
  time with HTTP 409 unless the compat level is `NONE`. Rationale:
  a consumer using the "latest" version expects a stable format
  family; silently changing JSON↔Avro mid-subject would break
  every downstream.

## 5. Codec caching — format baked at compile time

### 5.1 Key

`CompiledCodecCache` (landed in T.1, design 0002 §Cache layout) is
keyed by `(tableId, schemaId)`. Format is **not** in the key —
because a given `(tableId, schemaId)` has a single stored format,
and the cache entry's class already encodes it.

```java
// Pseudocode
public final class CompiledCodecCache {
  private final ConcurrentHashMap<Long, RecordCodec> byKey =
      MapUtils.newConcurrentMap();

  public RecordCodec getOrCompile(long key, Supplier<RecordCodec> compile) {
    return byKey.computeIfAbsent(key, k -> compile.get());
  }
}
```

### 5.2 Format baked into the compiled class

This is the core design point for zero hot-path dispatch.

Each translator owns its own codec compiler:

- `AvroCodecCompiler` emits `AvroCodec_<schemaId>` subclasses.
- `JsonCodecCompiler` emits `JsonCodec_<schemaId>` subclasses.
- `ProtobufCodecCompiler` emits `ProtobufCodec_<schemaId>` subclasses.

All three implement the same `RecordCodec` interface. A cache
lookup returns a `RecordCodec` reference; the JVM dispatches through
`invokeinterface` on first call and inlines after warmup.

**The hot path does not branch on format.** It gets a `RecordCodec`
and calls `decodeInto(src, off, len, dst)`. The concrete
implementation is whatever the translator compiled. This is the
"format baked into the compiled class" guarantee from plan §27.8's
bullet on extensibility.

### 5.3 Where the format *is* checked

The format is consulted exactly once per produce/fetch:

1. Kafka wire envelope: `[0x00][schema_id_be_i32][body]`.
2. The transcoder resolves `schema_id` to `(tableId, schemaVersion,
   format)` via the catalog.
3. The transcoder fetches or compiles the `RecordCodec` for
   `(tableId, schemaVersion)` using the format-specific compiler.
4. `codec.decodeInto(...)` runs — format-agnostic from here.

Step 3 incurs the format dispatch; step 4 does not.

### 5.4 Recompilation cost

Each translator's compile is ~30–50 ms (Janino dominating). Cache
entries are long-lived. A worst-case cold path (cache miss +
compile) is bounded by the compile cost; subsequent requests are a
concurrent-hashmap lookup.

Eviction is still an open question from design 0002 §Open questions.
T-MF makes the cost per entry slightly higher (per-format
compilation) but per-entry memory (~10 KB) is unchanged because
each compiled class is equivalent weight to the Avro baseline.

## 6. Error surfaces

### 6.1 `SchemaTypeNotSupportedException`

Thrown by the HTTP handler when `schemaType` resolves to an empty
`Optional` from the registry. Maps to HTTP 422 with error code
`SCHEMA_TYPE_NOT_SUPPORTED` and the list of registered formats.

### 6.2 `SchemaTranslationException`

Thrown by a `FormatTranslator.translateTo` when the submitted text
is syntactically valid but semantically unprojectable (e.g.
recursive `$ref`, `oneof`, 3-way union). Maps to HTTP 422 with
error code `SCHEMA_NOT_TRANSLATABLE` and the offending JSON pointer
/ descriptor full name.

### 6.3 `CompatibilityFailureException`

Thrown by the HTTP handler when `CompatibilityChecker.check`
returns incompatible. Maps to HTTP 409, matching Confluent.

### 6.4 Error precedence

The three exceptions can all fire on `POST /subjects/{s}/versions`.
Precedence (evaluated top-down):

1. `SchemaTypeNotSupportedException` — if the format isn't
   registered, we never try to translate.
2. `SchemaTranslationException` — if the text can't be translated
   to `RowType`, we never compat-check.
3. `CompatibilityFailureException` — only reaches here if the text
   translated and we had priors.

## 7. Extensibility — adding a fourth format

Say we want to add Thrift. The plan file uses Thrift as the
worked-example; we echo that here.

### 7.1 What you add

1. **`ThriftFormatTranslator.java`** (in `fluss-kafka/.../sr/typed/`).
   - Implements `FormatTranslator`.
   - `formatId()` returns `"THRIFT"`.
   - `translateTo(String)` parses Thrift IDL (using some Thrift
     parser library — probably `org.apache.thrift:libthrift`).
   - `projectFrom(RowType)` emits canonical Thrift IDL.
   - Has its own type mapping, nullability handling, rejection set
     — essentially a new `0010-thrift-translator.md` document.

2. **`ThriftCodecCompiler.java`** (in `sr/typed/`).
   - Emits `ThriftCodec_<schemaId>` subclasses that decode from
     Thrift binary / compact protocol directly into
     `BinaryRowWriter` without allocating a `TBase`.

3. **`ThriftCompatibilityChecker.java`** (in `sr/compat/`).
   - Implements `CompatibilityChecker`.
   - Thrift's field-number immutability rules are near-identical
     to Protobuf's; a similar clean-room ~400 LOC.

4. **META-INF/services additions** — two lines, one per SPI.

### 7.2 What you do *not* touch

- `FormatRegistry` — zero edits.
- `SchemaRegistryHttpHandler` — zero edits (dispatch is already
  registry-driven after T-MF.5).
- `CompiledCodecCache` — zero edits (the format is already baked
  into each compiled class).
- Anything under `fluss-common` / `fluss-rpc` / `fluss-server` —
  the bolt-on discipline from design 0006 §0 holds.
- Design 0002 — zero edits (its abstractions are format-agnostic).

### 7.3 What you write

- One translator design doc (sibling to 0007 / 0008).
- One set of per-format unit tests.
- One E2E IT driven by a Thrift serializer (if one exists
  upstream).

### 7.4 The "fourth format" lower bound

The smallest possible new format is ~600 LOC of implementation plus
a design doc. T-MF demonstrates this path is clean; each of JSON
and Protobuf came in under 1200 LOC of production code.

## 8. Module placement

The registry and SPI live in `fluss-kafka/`. They are not hoisted
to `fluss-common` despite being useful-looking general-purpose
types, because:

- Design 0006 §Principles.5 — minimise Fluss-core churn. The SPIs
  have no callers outside Kafka SR today.
- `fluss-common` has no Janino dependency; hoisting the codec
  compiler SPI would drag Janino into core.
- The SPIs reference `RowType` and the HTTP transport, both of
  which are already Kafka-adjacent concerns in the existing
  design.

If a second transport (say, a future gRPC-based registry) wants to
reuse the SPIs, we'll hoist at that point. Premature now.

## 9. Security and RBAC

Registry operations go through the existing SR authorizer (design
0002 §HTTP auth + Phase G). `FormatRegistry` adds zero new
permissions — a principal that could register an Avro schema can
register a JSON or Protobuf schema in the same subject.

If a future deployment wants per-format restrictions (e.g. "only
platform team may register Protobuf"), the enforcement point is the
authorizer, not the registry. The registry itself is
format-agnostic in the auth dimension.

## 10. Observability

Per-format metrics added under `kafka.sr.<format>.*`:

- `register_total{format}` — counter.
- `register_error_total{format, reason}` — counter (reasons:
  `translation_failed`, `compat_failed`, `type_not_supported`).
- `translate_latency_ms{format}` — histogram.
- `compat_check_latency_ms{format}` — histogram.
- `codec_compile_latency_ms{format}` — histogram.

The observability spine from Phase M (design 0006 §Area M) picks
these up without changes; the metric group is tagged by format so
operators can spot a format-specific regression.

## 11. Testing

### 11.1 Unit

- `FormatRegistryTest` — lookup by formatId (case-insensitive),
  unknown format returns empty, bootstrap rejects duplicate
  formatId.
- `CompatibilityCheckerSpiTest` — all three registered checkers
  implement the SPI contract (basic smoke: `formatId()`
  non-null, `check()` tolerates empty priors).

### 11.2 Integration

- `SchemaRegistryAvroITCase` — existing (T.1). Stays green.
- `SchemaRegistryJsonSchemaITCase` — new (T-MF.2). Drives the
  real `io.confluent:kafka-json-schema-serializer`.
- `SchemaRegistryProtobufITCase` — new (T-MF.3). Drives
  `io.confluent:kafka-protobuf-serializer`.
- `SchemaRegistryMultiFormatRoundTripITCase` — new. Three subjects,
  one per format, register-list-lookup-delete through each under
  one registry instance. Proves no cross-format state leakage.

## 12. Open questions

1. **Default format when `schemaType` is absent.** Confluent
   defaults to `"AVRO"`. We do the same for wire-compatibility.
   Open question: should we surface the default in `GET /config`?
   Yes, probably — operators need to know. T-MF.5.
2. **Format aliases.** Should `"json"` resolve to the same
   translator as `"JSON"`? Yes, case-insensitive. Should
   `"JSONSCHEMA"` resolve to `"JSON"`? Confluent uses `"JSON"` on
   the wire; we match and do not support aliases.
3. **Per-format compat-level defaults.** Currently the subject's
   `compatibilityLevel` applies regardless of format. In principle
   Protobuf could default to a more permissive level than Avro
   (Protobuf's semantic rules are weaker). Deferred; revisit if
   operators complain.

## 13. References

- Plan §27.3, §27.7, §27.8, §27.9, §27.11.
- Design 0002 (Fluss-first SR, hot-path codec, cache).
- Design 0006 (bolt-on discipline, module boundaries).
- Design 0007 (JSON Schema translator).
- Design 0008 (Protobuf translator).
- AGENTS.md (Java 8 source level, `ServiceLoader` idiom).
