# ADR 0001 — Kafka Compatibility: Build On Existing Stubs

- **Status:** Proposed
- **Date:** 2026-04-20
- **Deciders:** Ben Gamble (ben.gamble@ververica.com)
- **Scope:** Local fork of apache/fluss; `feature/kafka-compat-shim` branch

## Context

Fluss is an Apache-licensed real-time storage engine. The goal of this work
is to make a Fluss cluster look like a Kafka broker on the wire, so that:

- Official Kafka SDKs (Java `kafka-clients`, librdkafka, Sarama, etc.) can
  produce and consume against Fluss without a sidecar.
- Kafka topics are projections of Fluss tables. A built-in, mandatory schema
  registry (itself a Fluss table) translates Avro / Protobuf / JSON payloads
  into Fluss columns on the way in, and back into the Kafka wire format on
  the way out. When no schema is supplied, the registry auto-generates a
  single-column `VALUE BYTES` schema.
- Kafka producer + Fluss reader, Fluss writer + Kafka consumer, and every
  combination in between, work concurrently on the same underlying table.
- No external components. Performance cost of translation is acceptable;
  duplicated storage is not.

Fluss already ships a partial Kafka-compat layer on `main`, landed by
`dao-jun` across issues #488, #591, #607, #609, #611 under umbrella #486.
The existing scaffolding covers:

- `NetworkProtocolPlugin` SPI and `KafkaProtocolPlugin` wiring
  (`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaProtocolPlugin.java`).
- Netty channel pipeline (`KafkaChannelInitializer.java`).
- Stateful request decoder that preserves Kafka client response ordering via
  a `ConcurrentLinkedDeque<KafkaRequest>` (`KafkaCommandDecoder.java`).
- Dispatcher for every Kafka `ApiKey` with only `API_VERSIONS` implemented;
  `METADATA` version-capped at v11 and `FETCH` at v12 to side-step
  TopicId (`KafkaRequestHandler.java`).
- Configs `kafka.enabled`, `kafka.listener.names`, `kafka.database`,
  `kafka.connection.max-idle-time` (`ConfigOptions.java`).
- Kafka-reserved default database created at coordinator startup.

PR apache/fluss#2027 (MERGED) removed Kafka compatibility from the Apache
Fluss roadmap. Quote:

> Kafka compatibility is always welcomed as part of contributions, however
> it won't be on our direct plans because we want Fluss to set the way
> beyond Kafka.

Comments on #486 (`polyzos`, `wuchong`) confirm: the core team will accept a
good contribution but will not drive or maintain it. `dao-jun` has stated on
#486 they are out of time to continue. A third-party sidecar project
(`fluss-cape`, https://github.com/gnuhpc/fluss-cape) exists as a Kafka
proxy in front of Fluss; the whole point of our approach is the opposite —
in-broker translation so we reuse Fluss's existing storage, metadata,
replication, and per-bucket monotonic-long offsets.

## Decision

Resume Kafka-compat work starting from the commits already on `main` rather
than deleting them and starting over.

- Base branch: fork of `apache/fluss`, topic branch
  `feature/kafka-compat-shim` cut from `main`.
- Inherit `dao-jun`'s scaffolding as-is: plugin SPI wiring, decoder, configs,
  reserved database, version caps.
- Proceed **without upstream coordination**. Collaboration with `dao-jun` or
  the #486 umbrella is not available right now. The open sub-tasks #607
  (MemoryRecords ↔ LogRecordBatch converter) and #611 (post-startup server
  registration) describe work we need anyway; we do that work on our fork
  under our own design, and cross-reference those issues only for context.
- Decide at PR-time — not now — whether to open PRs upstream or continue as
  a maintained fork.

## Consequences

**Positive**

- Keeps the already-reviewed Kafka scaffolding. No re-litigating settled
  wire-format or plugin-lifecycle decisions.
- Inherits the `METADATA` v11 / `FETCH` v12 caps that avoid the TopicId
  work until we choose to take it on.
- Aligns with the protocol target `dao-jun` picked — Kafka broker 3.9.0 —
  which matches the `kafka-clients:3.9.2` dependency already in
  `fluss-kafka/pom.xml`.
- Because the Kafka produce path writes genuine `LogRecord`s via
  `ReplicaManager.appendRecordsToLog`, a Fluss `LogScanner` and a Kafka
  consumer see the same rows by construction. The dual-read / dual-write
  requirement falls out of the architecture, not out of extra glue.
- Fluss's per-bucket contiguous long offsets map directly to Kafka's
  partition offsets — no offset-translation layer needed.

**Negative**

- We inherit design decisions we may disagree with. Example: the `TODO` in
  `KafkaRequestHandler.java` — *"we may need a new abstraction between
  TabletService and ReplicaManager to avoid affecting Fluss protocol when
  supporting compatibility with Kafka."* We revisit this per-handler, not up
  front.
- Features Fluss core hasn't built must be implemented on top:
  - Kafka SR-style schema registry (stored in a Fluss system table
    `kafka._schema_registry`).
  - Consumer group coordinator (`JOIN_GROUP` / `SYNC_GROUP` / `HEARTBEAT` /
    `LEAVE_GROUP` stubs are empty).
  - Consumer offset storage (`OFFSET_COMMIT` / `OFFSET_FETCH` stubs) in a
    Fluss system table `kafka._consumer_offsets`.
  - Kafka `MetadataResponse` builder stitched from `TableAssignment` +
    `Endpoint` (advertised listeners).
  - `MemoryRecords ↔ LogRecordBatch` converter with compression, headers,
    and timestamp-type handling (the #607 work, extended to be
    schema-aware).
- Maintenance burden sits on us, not on the Fluss core team. This shapes
  scope — the MVP has to be small enough for one person to own across
  Fluss releases, and migrations (bucket reassignment, TopicId, KRaft-style
  metadata changes upstream) are ours to track.
- Transactions (`INIT_PRODUCER_ID`, `ADD_PARTITIONS_TO_TXN`, ...) and SASL
  are explicitly out of MVP scope. They will fail with
  `UNSUPPORTED_VERSION` until dedicated follow-up work lands.

## Alternatives Considered

1. **Greenfield rewrite.** Rejected. Throws away review work already in
   `main` (SPI plugin, decoder, configs) and re-opens wire-format decisions
   that have already been settled.
2. **Sidecar proxy, in the style of `fluss-cape`.** Rejected. Violates the
   "no external components" constraint; forces a parallel offset/storage
   system; adds a second hop on the hot path.
3. **Base branch `release-0.9`.** Rejected. Release branches are
   stabilisation-only and do not carry the Kafka scaffolding. The Kafka
   stubs only exist on `main`.

## Next Steps

1. Produce the research deliverables (protocol matrix, schema-registry wire
   contract, record-conversion spec, metadata-response mapping,
   consumer-group storage layout, co-existence invariants, open questions)
   in this `dev-docs/` workspace — not upstream.
2. Land a local prototype that implements `METADATA` + a minimal `PRODUCE`
   round-trip end-to-end against the official `kafka-clients` producer,
   backed by a real Fluss table. Include a `FlussClusterExtension`-based
   integration test that asserts the produced record is visible via a
   `LogScanner` on the same table.
3. Reassess after the prototype: open a PR upstream, continue as a
   maintained fork, or both. No community outreach in this phase.

## References

- Umbrella issue: https://github.com/apache/fluss/issues/486
- De-roadmap PR: https://github.com/apache/fluss/pull/2027
- Open sub-tasks: #607 (MemoryRecords converter), #611 (post-startup
  registration).
- Closed sub-tasks that landed the scaffolding we inherit: #488, #591, #609.
- Adjacent, non-blocking: #498 (Kerberos), #2410 (schema evolution).
- Prior art: https://github.com/gnuhpc/fluss-cape
