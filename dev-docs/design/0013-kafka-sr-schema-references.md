# Design 0013 — Kafka SR schema references

**Status:** Draft (SR-X.5), 2026-04-24
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0009-multi-format-registry.md`, `0007-json-schema-translator.md`,
`0008-protobuf-translator.md`.

## Context

Kafka Schema Registry lets one subject-version import other
subject-versions by name. A register payload carries a `references`
array; each entry is a triple `(name, subject, version)` where `name`
is the symbolic token the referrer uses to point at the referent (an
Avro fullname, a JSON Schema `$ref` string, a `.proto` import path).
At read time the registry round-trips the array in
`GET /subjects/{s}/versions/{v}` and `GET /schemas/ids/{id}`, and
exposes a reverse index via `GET /schemas/ids/{id}/referencedby`. The
reference graph is used by every format-aware Kafka SR serializer:
without it the consumer cannot reassemble a Protobuf descriptor that
imports `google/protobuf/timestamp.proto`, cannot resolve a JSON
Schema `$ref`, cannot validate an Avro schema that reuses a named
type defined in a sibling subject.

Today our registry silently drops the array. `SchemaRegistryHttpHandler`
reads only `schema` and `schemaType` on the register POST body (see
`SchemaRegistryHttpHandler.java:214` — `JsonNode body =
MAPPER.readTree(readUtf8(request))`, then `body.get("schema")` and
`body.get("schemaType")`); `SchemaRegistryService.register` has no
references parameter; the catalog has no storage for the edge set;
no HTTP endpoint exposes the reverse index. A client that registers
a referent followed by a referrer gets a 200 on both, the referrer's
stored text mentions a name the registry has no record of, and every
consumer that expects the reference array to round-trip silently
falls back to an un-hydrated parser. This document specifies
SR-X.5: the entity that holds the reference edges, the HTTP surface
change on register / read / compat / referenced-by, the integrity
rules on hard-delete, and the resolver contract each format's compat
checker consumes.

## Status at time of writing

The register HTTP arm lives at `SchemaRegistryHttpHandler.java:209-226`:

```java
if (HttpMethod.POST.equals(method)
        && path.startsWith("/subjects/")
        && path.endsWith("/versions")) {
    String subject =
            path.substring("/subjects/".length(), path.length() - "/versions".length());
    JsonNode body = MAPPER.readTree(readUtf8(request));
    String schemaType =
            body.hasNonNull("schemaType") ? body.get("schemaType").asText() : "AVRO";
    if (!body.hasNonNull("schema")) {
        throw new SchemaRegistryException(
                SchemaRegistryException.Kind.INVALID_INPUT,
                "'schema' field is required in POST body");
    }
    int id = service.register(subject, body.get("schema").asText(), schemaType);
    ObjectNode response = MAPPER.createObjectNode();
    response.put("id", id);
    return jsonResponse(HttpResponseStatus.OK, response);
}
```

`body.get("references")` is never read. The service signature it
dispatches into is `SchemaRegistryService.java:327`:

```java
public int register(String subject, String schemaText, String formatId) { ... }
```

The catalog-facing call sites inside that method are
`SchemaRegistryService.java:378` (`catalog.registerSchema(...)`) and
`SchemaRegistryService.java:384` (`catalog.bindKafkaSubject(...)`).
Neither knows about references. The compat pre-check at
`SchemaRegistryService.java:590` feeds raw text through
`checkerFor(subjectFormat).check(proposed, priorTexts, level)` — the
checker SPI (`CompatibilityChecker.java:48`) takes no resolver, so an
Avro referent's named-type table never reaches the
`Schema.Parser` inside `AvroCompatibilityChecker.java:81`.

The read arms on `SchemaRegistryHttpHandler.java:175-207` and
`:251-279` construct their response body from the `RegisteredSchema`
tuple only: `id`, `subject`, `version`, `schemaType`, `schema`. No
`references` field is ever emitted. The exists-probe arm
(`:353-380`) and the compat arm (`:313-352`) have the same gap.

The `callerPrincipal()` helper that SR-X.5 will reuse for the audit
field on the reference rows lives at `SchemaRegistryService.java:278`.

## Entity design

A new entity table `_catalog.__schema_references__`, registered the
same way as every other catalog entity via `SystemTables.java`
(sibling to `KAFKA_BINDINGS` at `SystemTables.java:109-118`). Shape:

```
PK: (referrer_schema_id STRING, reference_name STRING)
fields:
  referenced_subject    STRING NOT NULL
  referenced_version    INT    NOT NULL
  referenced_schema_id  STRING NOT NULL   -- pinned at write time
  created_at            TIMESTAMP_LTZ(3) NOT NULL
```

Keys are the catalog's internal UUID `schema_id` (from
`SchemaVersionEntity.schemaId()`, matching the storage style of
every other catalog entity — see
`SchemaVersionEntity.java:61`), not the Kafka SR 31-bit global id.
SR schema ids are a derived projection; the primary edge storage
joins on the same UUID column that `_schemas.schema_id` publishes.

### Why pin `referenced_schema_id`

The Kafka SR API exposes only `(subject, version)` on the wire, but
a subject's version can be hard-deleted and re-registered (different
`schema_id`, different SR schema id, possibly identical text), or
the referent's catalog row may be mutated by a future migration. A
referrer row that stored only `(subject, version)` would
silently follow those changes; the referrer's compat semantics
depend on the exact referent text it was validated against. Pinning
the UUID makes each edge immutable — if the referent's `schema_id`
no longer exists in `_schemas`, every read of the referrer surfaces
that the edge is stale and the delete-path integrity check refuses
a confused cascade. The wire projection still echoes
`(subject, version)` (Kafka SR's shape); pinning is an internal
detail.

Both projections are cheap:

- The forward index (referrer → refs) is a PK prefix scan on
  `referrer_schema_id`.
- The reverse index (referenced → referrers) is a non-PK scan today
  and a secondary-index entity table tomorrow. Phase C.1's
  scan-based pattern (see `FlussCatalogService.java:450` for the
  same shape on `listKafkaSubjects`) is acceptable at SR-X.5 scale;
  plan §27 calls out indexing as a later line item.

## Catalog API additions

Four new methods on `CatalogService` (siblings to the existing
`registerSchema` / `getSchemaById` / `getSchemaBySchemaId` /
`deleteSchemaVersion` group on
`CatalogService.java:88-141`):

```java
/** Replace the full reference set for {@code referrerSchemaId}. */
void bindReferences(String referrerSchemaId, List<SchemaReference> refs) throws Exception;

/** Forward edges — the refs this referrer declared at register time. */
List<SchemaReference> listReferences(String referrerSchemaId) throws Exception;

/** Reverse edges — which referrers point at {@code referencedSchemaId}. */
List<String> listReferencedBy(String referencedSchemaId) throws Exception;

/** Cascade on hard-delete of the referrer row; no-op when absent. */
void deleteReferences(String referrerSchemaId) throws Exception;
```

`bindReferences` is a full replacement (not an append) so that a
re-registration path which resurrects a tombstoned row can re-apply
its reference list without leaving dangling edges. A caller that
wants additive semantics can call `listReferences` first and pass
the union.

The value type:

```java
public final class SchemaReference {
    private final String name;              // e.g. "com.example.Address"
    private final String subject;           // SR subject of the referent
    private final int version;              // SR version (1-based)
    private final String referencedSchemaId; // catalog UUID, pinned

    // accessors, equals/hashCode on all four fields
}
```

`SchemaReference` sits under `fluss-catalog/.../entities/`, the same
directory as `SchemaVersionEntity.java`. Implementation lives in
`FlussCatalogService`: `bindReferences` upserts N rows in the new
table after a delete of the prior set; `listReferences` scans with a
PK-prefix filter on `referrer_schema_id`; `listReferencedBy` does a
full-table scan (`FlussCatalogService.java:1079` pattern) and
filters in memory; `deleteReferences` uses the existing
`deletePlaceholder` idiom (`FlussCatalogService.java:980`).

## HTTP surface changes

| Endpoint | Change |
|---|---|
| `POST /subjects/{s}/versions` | reads `references: [{name, subject, version}]`; each is resolved against the catalog and pinned to the referent's `schema_id`; 422 if any referenced `(subject, version)` does not exist or is tombstoned |
| `GET /subjects/{s}/versions/{v}` | response body includes `references` array, echoing `(name, subject, version)` in insertion order |
| `GET /subjects/{s}/versions/latest` | same |
| `GET /schemas/ids/{id}` | response body includes `references` array |
| `POST /subjects/{s}` (exists-probe) | response body includes `references` array |
| `GET /schemas/ids/{id}/referencedby` | new; returns `[{subject, version}]` tuples for every referrer (Kafka SR shape) |
| `POST /compatibility/subjects/{s}/versions/{v}` | reads `references` off the request body and resolves them exactly like the register arm; resolved text is fed to the compat checker |
| `DELETE /subjects/{s}/versions/{v}?permanent=true` | 422 when `listReferencedBy(schemaId)` is non-empty, citing each blocking `(subject, version)` tuple |
| `DELETE /subjects/{s}?permanent=true` | 422 when any of its versions is still referenced; message iterates every offending `(subject, version)` |

The reverse-index arm is a new `GET` matcher in
`SchemaRegistryHttpHandler.dispatch`, slotted alongside the existing
`/schemas/ids/[0-9]+/subjects` and `/schemas/ids/[0-9]+/versions`
regex arms (`SchemaRegistryHttpHandler.java:146-174`). The register
arm grows a `body.get("references")` parse step before the service
call.

## Register flow

1. **Parse.** Handler reads `subject`, `schema`, `schemaType`, and
   the optional `references: [{name, subject, version}]` array from
   the body. Missing/empty `references` is indistinguishable from
   today's behaviour.
2. **Resolve each reference.** Inside the service, before any
   compat check, iterate the array and for each entry call
   `catalog.getSchemaVersion(kafkaDb, topicFrom(ref.subject),
   ref.version)`. A missing binding, a missing version, or a
   tombstoned version raises
   `SchemaRegistryException(INVALID_INPUT, "referenced subject X
   version Y does not exist")` → HTTP 422. The resolver captures
   each referent's text + `schema_id` for the following steps.
3. **Validate the proposed text with references bound.** Feed the
   referent texts to the format-specific parser (Avro:
   `Schema.Parser.parse(referent)` before `parse(proposed)`; JSON:
   register `$ref` base URIs against a local resolver; Protobuf:
   build a `FileDescriptorProto.Builder` with each referent added
   as a dependency). A parse failure with references resolved still
   yields a 422 with the format's parser message.
4. **Compat gate with references.** If the subject has a live prior
   history, call
   `checker.check(proposed, priorTexts, level, resolver)` where
   `resolver` has access to every referent text (both the new
   ones supplied on this register and, for transitive compat, the
   references that each prior version declared). The checker fails
   closed on a 409 exactly as today
   (`SchemaRegistryService.java:591`).
5. **Register and bind.** On pass, call
   `catalog.registerSchema(...)` as today
   (`SchemaRegistryService.java:378`), then
   `catalog.bindReferences(newSchemaId, refs)` with the resolved
   list (the version pins fixed at step 2), then
   `catalog.bindKafkaSubject(...)` as today
   (`SchemaRegistryService.java:384`). The three calls are not
   atomic (Fluss PK upserts are per-row); the existing registration
   path already tolerates partial failure by re-running, and the
   resurrect-tombstoned path (`SchemaRegistryService.java:617`)
   re-applies references via `bindReferences` so a second attempt
   converges.

The idempotent fast path
(`SchemaRegistryService.java:347-357`) requires a light extension:
two registrations with identical `schemaText` but different
reference arrays must not collapse to the same SR schema id. The
fast-path comparison becomes "same text AND same reference tuple
list" before returning the prior id; otherwise it falls through to
the compat gate and mints a new version. This matches Kafka SR's
behaviour.

## Compatibility checking with references

The checker SPI widens. Today
(`CompatibilityChecker.java:48`):

```java
CompatibilityResult check(String proposedText, List<String> priorTexts, CompatLevel level);
```

Phase SR-X.5 adds a resolver parameter:

```java
CompatibilityResult check(
        String proposedText,
        List<String> priorTexts,
        CompatLevel level,
        ReferenceResolver resolver);
```

Where:

```java
public interface ReferenceResolver {
    /** Look up the schema text for a reference {@code name}. */
    Optional<String> resolve(String name);

    /** All currently-bound names (mostly for error messages). */
    Collection<String> names();
}
```

The resolver is empty
(`ReferenceResolver.empty()`) when the caller has no references —
this keeps the call sites for compat-only paths (`POST
/compatibility/...`) uniform with the register path. The old
3-argument `check` stays as a default method that forwards to the
4-argument form with the empty resolver, preserving source
compatibility for any test that instantiated a checker directly
until every implementation migrates.

Per-format notes:

- **Avro.** `org.apache.avro.Schema.Parser` accepts multiple
  `parse(String)` calls and accumulates named types across them.
  Implementation in `AvroCompatibilityChecker` (see
  `AvroCompatibilityChecker.java:81`) pre-parses every referent
  the resolver surfaces, in the order Avro fullnames appear, then
  parses `proposedText`. Same for each prior text. The parser
  instance is per-call (already per-call today) — no threading
  change.

- **JSON Schema.** The Kafka SR convention is `$ref` strings that
  name the reference's `name` field (e.g.
  `"google/protobuf/timestamp.json#"`). `JsonCompatibilityChecker`
  wires the resolver into a
  `com.github.fge.jsonschema.core.load.SchemaLoader` equivalent
  backed by the resolver, rejecting unresolved `$ref`s with a
  rule-violation message.

- **Protobuf.** `.proto` imports (`import
  "google/protobuf/timestamp.proto";`). The
  `ProtobufCompatibilityChecker` builds a
  `FileDescriptorSet` by parsing each referent via
  `protobuf-java-util`'s text parser in dependency order, then
  parses the proposed file with the aggregated dependency list
  passed to the `FileDescriptor.buildFrom(proto, deps)` call.
  Unresolved imports surface as a 422 with the missing import path
  in the message.

Every concrete checker stays thread-safe (SPI contract on
`CompatibilityChecker.java:32`); the resolver parameter is the only
per-call state.

## Delete-path integrity

Referential integrity is enforced at the SR projection, not at the
catalog — the catalog stays a dumb row store. Rules:

- **Hard-delete of a schema version (referent).** Before deleting,
  call `listReferencedBy(schemaId)`. If non-empty, raise
  `SchemaRegistryException(CONFLICT, "version is still referenced
  by N schemas: {sub1 v1, sub2 v3, ...}")` → HTTP 422 (Kafka SR
  uses 422 for this class; we match). The message enumerates every
  blocking referrer as `(subject, version)` tuples resolved via
  the kafka-binding table.
- **Soft-delete of a referent.** Allowed even when referenced. The
  Kafka SR semantics here are "tombstoned but resolvable until
  hard-deleted"; a referrer's compat checker can still see the
  referent's text (soft-delete is a subject-scoped tombstone on
  `_sr_config`, not a row delete — see
  `SchemaRegistryService.java:450`). This matches Kafka SR's
  graph-is-still-walkable behaviour for soft-deleted referents.
- **Hard-delete of a referrer.** Cascades: the SR calls
  `catalog.deleteReferences(schemaId)` before
  `catalog.deleteSchemaVersion(schemaId)`. Order matters only for
  crash-consistency; a partial sequence leaves orphan reference
  rows which `listReferencedBy` naturally ignores once the
  referrer row is gone, and a re-run of the delete converges.
- **Subject-level hard-delete** (`DELETE /subjects/{s}?permanent=true`).
  Iterates every version in ascending order; for each version,
  apply the referent rule above. A single referenced version
  fails the whole subject delete with 422; the caller retries
  after clearing its referrers. This mirrors
  `SchemaRegistryService.java:478` where the current loop
  unconditionally hard-deletes each version.

The rule "cannot delete a referent that is still referenced" is
symmetric to the Kafka SR's and keeps the graph acyclic-by-age
(a referrer is always younger than its referents).

## Canonicalization interaction

Phase SR-X.2 introduces register-path canonicalization: the
`schemaText` is normalised (whitespace, field order per format
rules) before hashing, so whitespace-only edits don't mint a new
SR schema id. Phase SR-X.5 extends canonicalization over the
reference closure: the text being hashed is not the bare
`proposedText` but
`canonicalise(proposedText) || canonicalise(resolved_ref_1) ||
canonicalise(resolved_ref_2) || ...` — the referent contributions
are ordered by `name` (lexicographic) so the hash is
reference-set-stable.

Consequence: whitespace-only edits to a referent's next version, on
its own subject, do not appear as the same edit on the referrer.
The referrer's hash is stable against its pinned
`referenced_schema_id` — the referent's new version does not
propagate into the referrer's canonical form. A referrer that
wants to follow the referent must itself re-register against the
new version. This is the correct Kafka SR semantics.

## Files

**Adds**

- `fluss-catalog/.../entities/SchemaReference.java` — the value
  type (`(name, subject, version, referencedSchemaId)`).
- `fluss-catalog/.../entities/SchemaReferenceEntity.java` — the
  row-shaped entity mirror used by the scan paths in
  `FlussCatalogService`.
- `fluss-kafka/.../sr/references/ReferenceResolver.java` — SPI
  above.
- `fluss-kafka/.../sr/references/CatalogReferenceResolver.java` —
  the impl the service passes through to the checkers, wrapping
  `catalog.listReferences(id)` + `catalog.getSchemaBySchemaId(id)`.

**Modifies**

- `fluss-catalog/.../CatalogService.java` — four new methods
  (`bindReferences`, `listReferences`, `listReferencedBy`,
  `deleteReferences`).
- `fluss-catalog/.../FlussCatalogService.java` — implementations,
  plus a `deletePlaceholder(Handle.SCHEMA_REFERENCES, ...)` arm.
- `fluss-catalog/.../SystemTables.java` — new `SCHEMA_REFERENCES`
  constant + inclusion in `all()`.
- `fluss-kafka/.../sr/SchemaRegistryService.java` — widen the
  register / compat / exists-probe paths; widen
  `deleteVersion` / `deleteSubject` with the integrity check;
  emit references on the read paths.
- `fluss-kafka/.../sr/SchemaRegistryHttpHandler.java` — parse
  `references` on POST bodies; emit `references` on the GET / POST
  response bodies; add the `/schemas/ids/{id}/referencedby`
  matcher.
- `fluss-kafka/.../sr/compat/CompatibilityChecker.java` — widen
  the SPI; keep the 3-arg form as a defaulted bridge.
- `fluss-kafka/.../sr/compat/AvroCompatibilityChecker.java` —
  thread the resolver through `Schema.Parser`.
- `fluss-kafka/.../sr/compat/JsonCompatibilityChecker.java` — wire
  resolver into JSON Schema `$ref` resolution.
- `fluss-kafka/.../sr/compat/ProtobufCompatibilityChecker.java` —
  wire resolver into `FileDescriptor.buildFrom`.

## Verification

New IT: `fluss-kafka/src/test/java/org/apache/fluss/kafka/sr/SchemaRegistryReferencesITCase.java`.
Extends `FlussClusterExtension` per the Phase-C pattern. AssertJ
throughout (per CLAUDE.md §1.4). Covers:

1. Register `A/v1` (Avro schema defining `com.example.A`).
2. Register `B/v1` with
   `references: [{name: "com.example.A", subject: "A", version: 1}]`
   and a body that declares `com.example.A` as a named type inside
   `com.example.B`. Assert 200 + `{id: <bId>}`.
3. `GET /subjects/B/versions/1` body echoes the references array
   byte-for-byte under `AssertThatJson` equality.
4. `GET /schemas/ids/<bId>` body echoes the references array.
5. `GET /schemas/ids/<aId>/referencedby` returns
   `[{subject: "B", version: 1}]`.
6. `DELETE /subjects/A/versions/1?permanent=true` → 422; body
   message names `B/v1`.
7. `DELETE /subjects/B/versions/1` (soft), then `?permanent=true`
   (hard), then `DELETE /subjects/A/versions/1?permanent=true` →
   200.
8. Negative: register `C/v1` with `references:
   [{name: "missing", subject: "X", version: 9}]` → 422, message
   contains `"referenced subject X version 9 does not exist"`.
9. Canonicalization: register `A/v2` as a whitespace-only edit of
   `A/v1`; assert `B/v1` still pins `A/v1` and its SR schema id is
   unchanged.
10. Protobuf end-to-end: pre-register subject `well-known-timestamp`
    with the text of `google/protobuf/timestamp.proto`, then
    register subject `my-topic-value` via the real
    `io.confluent:kafka-protobuf-serializer` pointed at our HTTP
    endpoint; the serializer must surface the reference array in
    its register call, and the consumer must deserialize a payload
    that imports the timestamp. Test-scope only (Kafka SR
    Community License forbids redistribution — same constraint as
    the existing Protobuf IT per design 0009 §11.2).

## Scope

~600 LOC across ten files + the one IT above. Single PR. No
fluss-core changes; the work is entirely inside `fluss-catalog`
and `fluss-kafka`, in line with design 0006 §0's bolt-on
discipline.

## Out of scope

- **Cross-cluster reference resolution.** Kafka SR's SR supports
  references by a `{subject, version, clusterId?}` triple across
  federated registries. We target the single-cluster case; a
  reference always resolves inside the local catalog.
- **Anonymous references.** Kafka SR accepts `"subject": ""` as a
  shorthand for "the same subject I'm registering against". We
  reject empty subject strings in the resolver. Adding this is a
  later, purely projection-level, change.
- **Reference metadata.** SR 7.4+ adds a free-form `metadata`
  sub-object on each reference entry. The wire protocol tolerates
  unknown fields, so newer clients sending metadata won't break; we
  ignore it on the way in and do not emit it on the way out.
  Introducing the column is deferred until a consumer needs it.
- **Transitive reference compaction.** If `A` imports `B` which
  imports `C`, our resolver hands the checker only the directly-
  declared references; the format parser walks the transitive
  graph itself via recursive `resolver.resolve(name)` calls. A
  smarter implementation would pre-flatten the closure and cache
  it; not needed at SR-X.5 scale.
