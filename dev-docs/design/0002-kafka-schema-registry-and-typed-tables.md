# Design 0002 — Fluss-First Schema Registry + Fast Typed-Table Serialization

**Status:** Draft, 2026-04-20
**Owner:** Ben Gamble
**Supersedes:** nothing
**Related:** `dev-docs/design/0001-kafka-api-protocol-and-metadata.md`

## Core principles (governing all other choices)

1. **Fluss-first.** Fluss's own schema model is canonical. Subjects, SR schema IDs, Avro
   JSON texts — all of those are *projections* over Fluss table metadata. There is no
   parallel subject lifecycle storing "what the user submitted"; every external schema
   passes through a Fluss-native translator on the way in and is re-derived on the way
   out. Evolution rules are Fluss's rules, expressed to the outside world in whatever
   framing the client wants.
2. **Serialization is the hot path.** Produce/Fetch throughput lives or dies on the
   record-bytes pipeline. The design must let the wire bytes travel directly to Fluss's
   row format without any intermediate object model (`GenericRecord`, `DynamicMessage`,
   `JsonNode`). Codecs are compiled once per (schema-id × direction) pair, cached, and
   reused for the life of the cluster.
3. **No Kafka SR-shaped redundancy in our ZK.** A Kafka SR stores each submitted
   schema text verbatim plus its own id counter plus subject→version chains. We refuse
   to pay that cost. Fluss's existing schema versioning (in `/fluss/tables/.../schemas`)
   is the truth; SR-facing state is a deterministic view over it.

These are non-negotiable. Everything below is derived.

## Locked from prior interview

1. Wire API: Kafka-compatible REST.
2. Authority direction: Fluss-first. (Adjusted from "SR-first" per the core principles.
   The earlier interview answer still holds *in effect* — registering a subject is how
   external tools drive Fluss schema changes — but the internal representation is
   Fluss's, not Avro's.)
3. Payload formats day 1: Avro, Protobuf, JSON Schema, raw-bytes passthrough.
4. Deployment: HTTP listener on CoordinatorServer.
5. Subject naming strategies day 1: TopicNameStrategy, RecordNameStrategy,
   TopicRecordNameStrategy.
6. Key + value schemas day 1.
7. Default compatibility: BACKWARD strict.
8. Pre-existing-table conflict: HTTP 409 unless schemas are exactly equal (post
   translation into Fluss's canonical form).

## Architecture

```
                    ┌───────────────────────────────────────────────┐
                    │ CoordinatorServer                             │
                    │                                               │
 HTTP (8081)   ──►  │  [NEW: SR HTTP listener (Netty)]              │
                    │           │                                   │
                    │           ▼                                   │
                    │  SchemaRegistryService                        │
                    │   ├─► FormatTranslator (Avro/Proto/JSON/Raw)  │
                    │   │     ◄── compiled codec cache (hot)        │
                    │   └─► MetadataManager (existing)              │
                    │           │                                   │
                    │           ▼                                   │
                    │        ZK (Fluss tables, no SR-only state)    │
                    └───────────────────────────────────────────────┘

                    ┌───────────────────────────────────────────────┐
                    │ TabletServer                                  │
                    │                                               │
                    │  KAFKA listener ► KafkaRequestHandler         │
                    │                      │                        │
                    │                      ▼                        │
                    │              RecordCodec (shared with SR)     │
                    │                      │                        │
                    │                      ▼                        │
                    │              LogManager / KvManager           │
                    └───────────────────────────────────────────────┘
```

Same `RecordCodec` compiled on the coordinator is cached-and-reused on tablet servers
when Produce/Fetch land — we ship the compiled codec via the metadata push that already
exists for schema updates (`TabletServerMetadataCache.updateLatestSchema`). No second
compilation; no second wire format description.

## Fluss-first consequences (explicit)

1. **No stored Avro/Proto/JSON schema text.** When a subject is registered, we translate
   to Fluss's schema model and discard the submitted text. `GET /schemas/ids/{id}`
   re-derives the Avro/Proto/JSON projection on demand from the Fluss schema. This is
   deterministic (same Fluss schema → same Avro text, byte-for-byte) because the
   translator is total and pure.
2. **Global schema id is a deterministic function of `(tableId, schemaVersion, format)`.**
   We do NOT mint a separate monotonic counter. Given Fluss's existing `(tableId,
   schemaId)` and the chosen format, we derive a 31-bit SR schema id by
   `hash(tableId, schemaId, format) mod (2^31-1)`, rejecting collisions at register time
   (ZK CAS on `/fluss/kafka-sr/id-reservations/<id>`). This keeps SR global-id
   API stable without shadowing Fluss's id system.
3. **"Subject" is a view.** The canonical state is:
   - a Fluss table (`kafka.<topic>` for TopicNameStrategy)
   - plus a `subject_binding` property on the table (tells the SR which subject name
     to surface)
   Listing subjects = listing Fluss tables that carry a binding. Registering a subject
   = creating a table with that binding, or evolving one that has it.
4. **Evolution rules are Fluss's.** BACKWARD compatibility is checked in the Fluss
   schema domain (not the Avro domain). Avro's BACKWARD rules are a proper superset of
   Fluss's strictly-additive evolution today, so we reject more than Kafka SR does —
   that's a feature, not a bug. The rejection message cites both the Fluss and the
   Kafka SR rule for UX.
5. **RecordNameStrategy and TopicRecordNameStrategy store a record-type → table map**,
   not separate subjects. A topic under TopicRecordNameStrategy is backed by a
   `kafka_passthrough` table with `_kafka_key BYTES`, `_schema_id INT`, `_payload BYTES`
   columns. Typed querying is deferred to Phase D; passthrough works on day 1 without
   violating Fluss-first (the table schema is still Fluss-native; it just happens to be
   a generic passthrough shape).

## Hot-path serialization design

### Design goal

Given a byte buffer `[0x00][schema_id_be_i32][body]`, land a Fluss `BinaryRow` with zero
intermediate object allocations and one bounds-checked pass over the input.

### Codec model

One interface, one implementation per format:

```
interface RecordCodec {
  // Decode wire body directly into a pre-sized BinaryRow writer.
  // Returns the number of bytes consumed.
  int decodeInto(ByteBuf src, int offset, int length, BinaryRowWriter dst);

  // Encode a Fluss BinaryRow back to the wire format (Fetch path).
  int encodeInto(BinaryRow src, ByteBuf dst);

  short schemaVersion();
  RowType rowType();
}
```

No `GenericRecord`, no `DynamicMessage`, no `JsonNode` on the hot path. Each
implementation is generated per (Fluss schema × format) pair.

### Compilation

On first registration of a (tableId, schemaVersion, format), the translator emits a
specialized codec. For each format:

- **Avro**: walk the Avro schema tree once; emit a flat list of `FieldReader` ops
  (variable-length int decode, zig-zag, UTF-8, fixed). Compose into one `decodeInto`
  method. Use Janino to compile the Java source once per (schemaId × direction); the
  JIT will inline it. Fallback interpreter if Janino is unavailable (e.g., native
  images). **Benchmark gate**: within 20 % of hand-written Avro decoder for a 12-field
  record at steady state.
- **Protobuf**: same pattern. Pre-compute field tag → field index table. Avoid
  `CodedInputStream`'s internal allocations by reading directly from Netty `ByteBuf`.
- **JSON Schema**: slower by nature (JSON parsing is O(n) regex-ish). Use Jackson
  streaming (`JsonParser`) into the `BinaryRowWriter`. No tree materialization.
- **Raw bytes**: `dst.writeBinary(src.slice(offset, length))`. Zero work.

### Cache layout

```
CompiledCodecCache
  ConcurrentHashMap<Long, RecordCodec>   // key = packed (tableId<<32) | schemaId
  ConcurrentHashMap<Integer, Long>       // SR global schema id → packed key
```

- First lookup on Produce/Fetch path: direct array-backed table when id is dense.
- On cache miss, fall through to the compilation path (blocking for the first request
  for that id; later requests hit cache).
- Cache is shared across all channels in the same JVM.

### Zero-copy on Fetch

For Produce we must decode (wire → BinaryRow). For Fetch we encode (BinaryRow → wire).
When the stored row was originally wire bytes of the same format *and* the schema is
unchanged, we short-circuit: store the original bytes alongside the row and just copy
them back on Fetch. For cross-format reads (producer = Avro, consumer = JSON), we
re-encode using the compiled encoder.

### Memory

- Hot path allocates only the `BinaryRow`'s backing bytes (reused from a per-thread
  pool).
- All intermediate state in `decodeInto` is on the stack.
- String decoding: direct UTF-8 → offset-length into the row's variable-length area;
  no `String` allocation unless the row is later projected to a boxed type.

### Benchmarks (required before Phase A ships)

New JMH benchmarks in `fluss-jmh`:

- `AvroDecodeBench`: single-record, 12 fields, against hand-written baseline.
- `AvroFetchThroughputBench`: 1M rows/s target on a single core for a 100-byte record.
- `JsonParseBench`: compare streaming vs tree-materialization fallback.
- `CodecCacheContentionBench`: 64 threads, 1k distinct schema ids round-robin.

Benchmarks are gating — we don't ship Phase A if the codec path is slower than the
published Kafka SR Avro serde by more than 20 %.

## HTTP stack

**Netty** (not Jetty/Javalin). We already ship shaded Netty 4. ~400 lines of HTTP/1.1
routing + Jackson for JSON responses. Pulling Jetty costs ~2 MB and a second IO
thread model.

## ZK layout (final, Fluss-first)

```
/fluss
  /kafka-sr
    /global-config                 # compatibility default, other globals
    /id-reservations/<id>          # Kafka SR-style global id → (tableId, schemaId, format)
    /record-type-index/<fqn>       # RecordNameStrategy: record FQN → tableId
    /topic-bindings/<topic>        # value-subject + key-subject resolved to tableId

/fluss/tables/<dbId>/<tableId>/kafka-sr/
    /subject-name                  # "<topic>-value" or "<fqn>" etc.
    /naming-strategy               # TopicName / RecordName / TopicRecordName
    /format                        # Avro / Proto / Json / Raw
    /compatibility                 # per-subject override
```

Notice what's NOT here: no `/subjects/{s}/versions/N/schema` with Avro text. The schema
is always `Fluss table @ schemaVersion`, re-projected to Avro on demand.

## Kafka SR endpoints (unchanged from prior draft except semantic)

Same endpoint list as before. The semantic change: `GET /schemas/ids/{id}` returns a
projection, not storage. `POST /subjects/{s}/versions` translates into a Fluss
`createTable` or `alterTable`, not a subject-write.

## Phasing

### Phase A — Fluss-first MVP
- Netty HTTP listener.
- `POST /subjects/{s}/versions`, `GET /schemas/ids/{id}`, `GET /subjects`, `GET
  /subjects/{s}/versions/latest`, `GET /` ping.
- Avro only, TopicNameStrategy only, value-only.
- Fluss-table auto-creation via translator.
- Compiled Avro codec + benchmark gates.
- Deterministic SR schema id allocation + ZK CAS reservation.

Exit: a Kafka Avro producer sends SR-framed bytes, a Fluss query returns the rows
typed, and the hot-path decoder is within 20 % of the baseline.

### Phase B — compatibility + keys
- Key subjects → Fluss PK tables.
- BACKWARD strict enforcement in Fluss schema domain.
- `/config`, `/compatibility/subjects/.../versions/latest`, `DELETE /subjects`.
- 409 on pre-existing-table conflict.

### Phase C — more formats
- Protobuf codec (compiled) + Proto → Fluss translator.
- JSON Schema codec (streaming) + translator.
- Raw-bytes passthrough tables.
- All gated by benchmarks.

### Phase D — advanced naming + references
- RecordNameStrategy (one subject across topics; table discovered via record-type
  index).
- TopicRecordNameStrategy passthrough tables.
- Avro schema references.

## Open questions

1. **HTTP auth**: same authorizer as Fluss RPC, or separate? Phase A ships with the
   same principal propagation as the KAFKA listener.
2. **Codec compilation tool**: Janino vs ASM. Janino is ~1 MB and JIT-friendly; ASM is
   smaller but more invasive to maintain. Recommend **Janino** for Phase A.
3. **Benchmark baseline source**: Kafka SR's published Avro serde, run in-process in
   fluss-jmh? Needs a non-Apache-2.0 check on the baseline dep (SR client is Apache 2).
4. **Zero-copy Fetch short-circuit**: requires storing wire bytes alongside rows. Size
   overhead ~15-30 % for Avro. Worth it? Probably yes for pure-Kafka workloads,
   probably not for SQL-heavy workloads. Gate behind a per-table property.
5. **Codec eviction**: compiled codecs are expensive (class, possibly 10 KB). Evict
   after N minutes of inactivity? Never? Benchmark before choosing.

## What I need from you before coding

1. **Confirm Fluss-first rewrite is what you meant.** Specifically, the "no stored
   Avro text" stance — it's the right thing but it means `GET /schemas/ids/{id}` is
   a projection, not a retrieval.
2. **Janino for codec compilation, yes/no?**
3. **Phase A benchmark target: 20 % of Kafka SR serde — acceptable, or tighter?**
4. **Phase sequencing vs Kafka-protocol Produce/Fetch from design 0001** — do we ship
   Phase A SR before KAFKA Produce/Fetch, at the same time, or after? They're
   independent by construction (SR is coordinator, Produce/Fetch is tablet server)
   but both need the same compiled-codec cache.

## T-MF Status (2026-04-24)

Multi-format work is in flight. Summary of what has landed and what remains:

- **T.1 — Avro translator + compiled codec + codec cache + JMH benches.** Landed in
  commit `9d476651`. `AvroFormatTranslator`, `AvroCodecCompiler`, `AvroCodecRuntime`,
  `CompiledCodecCache`, and the 12-field JMH baseline all ship. Codec scope in T.1
  is the scalar subset — arrays, maps, nested records, enum, fixed, decimal are
  deferred to T-MF.4.
- **T-MF.1 — `FormatRegistry` + `CompatibilityChecker` SPI.** Landed /
  in progress. Extracts `AvroCompatibilityChecker` behind a common SPI and introduces
  `FormatRegistry` with `ServiceLoader`-based discovery. Zero behaviour change to
  the existing Avro path; the HTTP handler's `schemaType` gate is still present
  until T-MF.5 replaces it. Design details in **`0009-multi-format-registry.md`**.
- **T-MF.2 — JSON Schema translator + codec + compat checker.** Landed /
  in progress. Uses shaded Jackson; no new dependencies. Full mapping table,
  rejection set, seven-level compat matrix, and streaming `BinaryRowWriter` codec
  specified in **`0007-json-schema-translator.md`**.
- **T-MF.3 — Protobuf translator + codec + compat checker.** Landed /
  in progress. Adds `com.google.protobuf:protobuf-java:3.25.5` + `protobuf-java-util`
  (Apache-2.0). `ProtobufCompatibilityChecker` is a clean-room implementation; we
  explicitly avoid shading the Confluent-licensed `kafka-protobuf-provider`.
  Design details in **`0008-protobuf-translator.md`**.
- **T-MF.4 — Avro codec-compiler type expansion.** Landed / in progress.
  Closes the scalar-only gap from T.1: `array<T>`, `map<V>`, nested `record`,
  `enum`, `fixed`, `decimal(p,s)`. Zero dependency changes. JMH benches stay on the
  12-field scalar baseline until Phase T.2 actually exercises nested codecs on the
  hot path.
- **T-MF.5 — SR HTTP surface opens up.** Removes the three
  `if (!"AVRO".equalsIgnoreCase(schemaType)) reject` gates in
  `SchemaRegistryHttpHandler` and replaces them with `FormatRegistry` lookups.
  `GET /schemas/types` returns `["AVRO", "JSON", "PROTOBUF"]`. E2E ITs drive the
  real Kafka SR serializers for each format (test-scope only; Kafka SR Community
  License forbids redistribution).

**Deferred, still needs fluss-core edits (explicit user constraint at T-MF):**

- **T.2 — produce/fetch hot-path wiring.** The compiled codec is not yet invoked
  from `KafkaProduceTranscoder` / `KafkaFetchTranscoder`; those still copy opaque
  bytes. Wiring in the typed codec becomes useful only once typed columns exist,
  which needs T.3.
- **T.3 — ALTER-on-register.** Turning the topic's Fluss table from
  `(key BYTES, payload BYTES)` into a typed row shape requires
  `TableChange.ReplaceColumn` in `fluss-common` plus plumbing through
  `Admin.alterTable`. That is the explicit fluss-core edit excluded from T-MF.

See the plan at `~/.claude/plans/check-which-branch-should-encapsulated-karp.md`
§27 for the full decomposition.
