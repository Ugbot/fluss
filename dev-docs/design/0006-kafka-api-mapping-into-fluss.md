# Design 0006 — Kafka API → Fluss Mapping: where every handler lands

**Status:** Implemented through Phase G.3 + SR-X.1 partial + Compacted-topic Phase 1,
2026-04-24.
**Supersedes:** (none — consolidates §3 of design 0001 and §5 of design 0002
with the reality of what has been built.)
**Related:** `0001-kafka-api-protocol-and-metadata.md`,
`0002-kafka-schema-registry-and-typed-tables.md`,
`0003-kafka-produce-fetch-kafka-shape-table.md`,
`0004-kafka-group-coordinator.md`,
`0005-catalog-and-projections.md`, and the structural plan at
`~/.claude/plans/check-which-branch-should-encapsulated-karp.md`.

## Why this exists

Six weeks of incremental commits have produced a Kafka bolt-on that
spans 32 Kafka wire APIs, a Kafka-compatible Schema Registry, and
an authorizer surface that enforces SASL-authenticated principals on
every handler. That work is covered by 80 ITs — across three
independent client stacks (Java `kafka-clients`, C `kcat`, Python
`confluent-kafka`). What's been missing is a single place an operator
or a new contributor can land and read "Kafka API X → Fluss call Y,
served by this file, enforced at that gate". This document is that
map.

The design intent — bolt-on discipline, Fluss-first authority, one
catalog for every projection — is set in designs 0001, 0002 and 0005.
This doc is the *current-state mapping*: what has actually shipped,
where it lives, and what still sits on a stub.

## 0. TL;DR — what lives where

At a glance, everything Kafka-shaped is contained to two new modules
and one small contract-surface change in the core. The rest is
additive.

| Area | Module / file | Purpose |
|---|---|---|
| Kafka wire protocol | `fluss-kafka/` | One jar. Decoder, ~30 per-API transcoders, SASL bridge, SR HTTP listener, Iceberg-REST bootstrap. |
| Catalog (metadata + RBAC + SR state + quotas) | `fluss-catalog/` | 9 PK tables in `_catalog` db; `CatalogService` interface + `FlussCatalogService` impl; consumed by every HTTP / wire projection. |
| Iceberg REST projection | `fluss-iceberg-rest/` (Phase E preview) | Validates that `CatalogService` isn't accidentally Kafka-shaped. |
| Stable SPIs in core | `fluss-rpc/.../gateway/ClusterMetadataProvider`, `.../replica/ReplicaSnapshot`, `CoordinatorLeaderBootstrap` | Added during Phase B; pinned down so fluss-server internals can change without cascading into bolt-ons. |
| One Fluss-core data-model edit | `fluss-common/.../security/acl/ResourceType.java`, `.../Resource.java`, `fluss-server/.../authorizer/DefaultAuthorizer.java` | Added `ResourceType.GROUP` for per-consumer-group ACLs (Phase G.2). ~60 LOC. |
| One pluggable SPI re-used | `fluss-common/.../security/auth/ServerAuthenticator.java` | Kafka SASL piggybacks on Fluss's native token-exchange interface — no new SPI, no `javax.security.sasl.*` reach-through in `fluss-kafka/`. |

**What this touched in the core vs everywhere else**:
`fluss-common` + `fluss-rpc` + `fluss-server` got exactly three
additions (ClusterMetadataProvider SPI in Phase B, ReplicaSnapshot DTO
in Phase B, ResourceType.GROUP in Phase G.2) plus two small
method-level surface additions (`ReplicaManager.deleteRecords` public
wrapper, `ReplicaManager.getProducerStates`). Everything else — 29 of
the 32 Kafka APIs, the full catalog, all of SR, all of ACL
enforcement — lives in the new modules.

**Design principles that drove it** (locked across designs 0001/2/5,
re-stated here because every decision below traces back to them):

1. **Bolt-on discipline.** Kafka-compat code imports only `@PublicStable` or
   `@PublicEvolving` Fluss interfaces. A Fluss-internal refactor must
   not ripple into `fluss-kafka/`. The reflection hack for `Authorizer`
   access (pulled off `TabletService`'s private field) is the one
   documented exception — a public accessor is a follow-up.
2. **Fluss-first authority.** Every projection (Kafka topics, SR
   subjects, Iceberg REST namespaces, client quotas) stores its state
   as Fluss PK rows in `_catalog`. There is no per-projection private
   store. If you need new state, add an entity table; don't hide data
   in ZK, in properties, or in a side file.
3. **One catalog, many projections.** Kafka SR HTTP, Iceberg REST HTTP
   and future Flink multi-format catalog are three *read/write
   projections* over the same `CatalogService`. Subjects, tables,
   grants, quotas all live in one namespace with one lifecycle.
4. **Wire contract portability.** Pass tests against kafka-clients
   (Java), librdkafka (C, via kcat), and librdkafka (Python, via
   confluent-kafka). Three independent protocol parsers make subtle
   wire-bytes divergence observable.
5. **Minimise Fluss-core churn.** Every change to `fluss-common` /
   `fluss-rpc` / `fluss-server` needs a specific reason to live there
   rather than in `fluss-kafka/`. ResourceType.GROUP is in core because
   authorization is a core concern; `KafkaGroupTranscoder` is in the
   bolt-on because it's Kafka-wire-shaped.

## 0.1 `fluss-kafka/` module design

```
fluss-kafka/src/main/java/org/apache/fluss/kafka/
├── KafkaProtocolPlugin.java       ← ServiceLoader entry; NetworkProtocolPlugin SPI
├── KafkaChannelInitializer.java   ← Netty pipeline: frame decoder + FlowControl +
│                                    KafkaCommandDecoder (+ SASL attr on channel)
├── KafkaCommandDecoder.java       ← per-connection state machine:
│                                    UNAUTHED → HANDSHAKE_DONE → AUTHED(principal),
│                                    intercepts SASL frames before they reach the handler
├── KafkaRequest.java              ← request wrapper; carries the authenticated FlussPrincipal
├── KafkaRequestHandler.java       ← single ~1.6k-line switch: api-key → handleXxx() method
├── KafkaServerContext.java        ← ctor-injected dependencies: metadataCache, metadataManager,
│                                    coordinatorGateway, replicaManager, zookeeperClient,
│                                    authorizer (nullable), clusterId, kafkaDatabase, own serverId
├── KafkaErrors.java               ← translator: Fluss exception → Kafka error code
│
├── admin/
│   ├── KafkaAdminTranscoder.java          ← CreateTopics, DeleteTopics
│   ├── KafkaConfigsTranscoder.java        ← DescribeConfigs, AlterConfigs, IncrementalAlterConfigs
│   ├── KafkaCreatePartitionsTranscoder    ← CreatePartitions
│   ├── KafkaDeleteRecordsTranscoder       ← DeleteRecords
│   ├── KafkaDescribeProducersTranscoder   ← DescribeProducers
│   ├── KafkaElectLeadersTranscoder        ← ElectLeaders
│   └── KafkaClientQuotasTranscoder        ← DescribeClientQuotas, AlterClientQuotas
│
├── auth/
│   ├── AuthzHelper.java               ← sessionOf(request), authorizeOrThrow, authorizeTopicBatch
│   ├── KafkaAclsTranscoder.java       ← DescribeAcls, CreateAcls, DeleteAcls
│   ├── KafkaListenerAuthConfig.java   ← per-listener protocol + enabled mechanisms,
│   │                                    resolved from SERVER_SECURITY_PROTOCOL_MAP
│   └── KafkaSaslTranscoder.java       ← per-connection SASL state machine; delegates to
│                                        Fluss ServerAuthenticator (no javax.security.sasl.*)
│
├── catalog/
│   ├── KafkaTopicsCatalog.java        ← SPI: topic-name → KafkaTopicInfo
│   ├── KafkaTopicInfo.java            ← Kafka-specific topic metadata (timestamp type,
│   │                                    compression, Kafka topicId)
│   ├── CustomPropertiesTopicsCatalog  ← default KafkaTopicsCatalog impl: reads Fluss
│   │                                    table custom properties set by KafkaTableFactory
│   └── KafkaTableFactory.java         ← builds the Fluss TableDescriptor for a Kafka
│                                        topic (log vs compacted; stamps PROP_BINDING_MARKER)
│
├── fetch/
│   ├── KafkaFetchTranscoder.java              ← Fetch
│   ├── KafkaListOffsetsTranscoder.java        ← ListOffsets
│   └── KafkaOffsetForLeaderEpochTranscoder    ← OffsetForLeaderEpoch
│
├── group/
│   ├── KafkaGroupTranscoder.java     ← JoinGroup/SyncGroup/Heartbeat/LeaveGroup/
│   │                                   FindCoordinator/OffsetCommit/OffsetFetch/
│   │                                   ListGroups/DescribeGroups/DeleteGroups/OffsetDelete
│   ├── KafkaGroupRegistry.java       ← in-memory group state machine (members, generation)
│   ├── OffsetStore.java              ← SPI: get/put consumer-group offsets
│   ├── FlussPkOffsetStore.java       ← default impl over __consumer_offsets__ PK table
│   ├── InMemoryOffsetStore.java      ← test impl
│   ├── ZkOffsetStore.java            ← legacy ZK impl (to be retired one release after
│   │                                   FlussPkOffsetStore has soaked)
│   ├── ConsumerOffsetsTable.java     ← schema + descriptor for __consumer_offsets__
│   └── OffsetStoreConnections.java   ← Fluss client Connection factory for the broker
│
├── metadata/
│   ├── KafkaDataTable.java            ← the Fluss-native schema for a Kafka topic
│   │                                    (log shape; compacted shape with record_key PK)
│   ├── KafkaMetadataBuilder.java      ← Metadata + DescribeCluster responses
│   └── KafkaTopicMapping.java         ← name-safety helpers (valid topic names)
│
├── produce/
│   └── KafkaProduceTranscoder.java    ← Produce; branches on hasPrimaryKey() for
│                                        compacted topics; log → appendRecordsToLog,
│                                        compact → putRecordsToKv + MergeMode.OVERWRITE
│
└── sr/
    ├── SchemaRegistryBootstrap.java       ← CoordinatorLeaderBootstrap impl, priority 100
    ├── SchemaRegistryHttpServer.java      ← Netty HTTP listener lifecycle
    ├── SchemaRegistryHttpHandler.java     ← ~20 Kafka SR REST endpoints → service calls
    ├── SchemaRegistryService.java         ← projection over CatalogService;
    │                                        RBAC hook (checkPrivilege) per endpoint
    ├── SchemaRegistryException.java       ← Kind → HTTP status translator
    └── SubjectResolver.java               ← SR subject naming (<topic>-value)
```

**Design rules inside the module**:

- **One transcoder per API family.** A transcoder takes the Kafka
  request data, does the Fluss work, returns the Kafka response data.
  It does not own Netty state, does not touch authz (handler does
  that), and does not know about other transcoders. This is why
  `KafkaRequestHandler` is big: all the cross-cutting work lives
  there, and transcoders stay boring.
- **Every transcoder is stateless** — one per request, GC-friendly.
  Per-connection state (SASL, inflight responses) lives on the
  decoder. Per-JVM state (offset store, group registry) lives on the
  handler.
- **Public-Fluss-interfaces-only rule.** Grep proves this:
  `fluss-kafka/` has exactly one reach into fluss-server internals —
  reflection on `TabletService.authorizer` — and it's tagged as
  a known follow-up (public accessor).
- **No `javax.security.sasl.*` in `fluss-kafka/`.** All SASL bytes
  flow through `ServerAuthenticator.evaluateResponse(byte[])`. Swapping
  mechanisms (SCRAM, GSSAPI) is a classpath change, not a Kafka-side
  change.

## 0.2 `fluss-catalog/` module design

```
fluss-catalog/src/main/java/org/apache/fluss/catalog/
├── CatalogService.java           ← the stable interface every projection consumes
├── CatalogServices.java          ← ServiceLoader-style registry (single live service per JVM)
├── CatalogException.java         ← { INVALID_INPUT, NOT_FOUND, ALREADY_EXISTS,
│                                    CONFLICT, UNSUPPORTED, INTERNAL }
├── FlussCatalogService.java      ← the impl, uses Fluss Connection + UpsertWriter + Lookuper
├── FlussCatalogBootstrap.java    ← CoordinatorLeaderBootstrap impl, priority 10
├── SystemTables.java             ← schemas + TableDescriptors for every _catalog PK table
└── entities/
    ├── NamespaceEntity.java
    ├── CatalogTableEntity.java
    ├── SchemaVersionEntity.java
    ├── PrincipalEntity.java
    ├── GrantEntity.java
    ├── KafkaSubjectBinding.java
    ├── ClientQuotaEntry.java
    ├── ClientQuotaFilter.java
    └── SrConfigEntry.java
```

**Nine PK tables** in the reserved `_catalog` db, all with
identical distribution (16 buckets). Read-after-write consistent
where the hot path needs it (subject → schema PK lookup chains); scan
on colder admin paths.

**Why a catalog module** rather than folding everything into
`fluss-kafka/sr`: the SR is one projection, Iceberg REST is another,
the Flink multi-format catalog bridge is a future third. A catalog
that only the SR could use would force the next two to either
duplicate the storage model or reach into SR-specific types. Keeping
`fluss-catalog` independent means the Iceberg REST scaffold (Phase E
preview) consumed the exact same `CatalogService` SPI on day one.

## 1. Three architectural layers, downward dependency

```
┌───────────────────────────────────────────────────────────────────┐
│ fluss-kafka (Kafka wire + SR HTTP + Iceberg REST projection)      │
│                                                                   │
│   Consumes only: Fluss public client APIs (Admin, Table,          │
│   Lookuper, UpsertWriter, Connection) + fluss-rpc SPI surface     │
│   (ClusterMetadataProvider, ReplicaSnapshot) + catalog service    │
│   + the Fluss authorizer (read-only) + the ServerAuthenticator    │
│   SPI (SASL bridge).                                              │
└───────────────────────────────────────────────────────────────────┘
                             ▲
┌───────────────────────────────────────────────────────────────────┐
│ fluss-catalog (CatalogService SPI + FlussCatalogService impl)     │
│                                                                   │
│   9 system PK tables in _catalog db:                              │
│     _namespaces _tables _schemas _principals _grants              │
│     _kafka_bindings _id_reservations _client_quotas _sr_config    │
│                                                                   │
│   Consumes: Fluss Connection / Admin / Table only. Does not       │
│   reach into fluss-server internals.                              │
└───────────────────────────────────────────────────────────────────┘
                             ▲
┌───────────────────────────────────────────────────────────────────┐
│ fluss-common / fluss-rpc / fluss-server / fluss-client            │
│                                                                   │
│   Storage, replication, ZK metadata, log + KV paths,              │
│   CoordinatorLeaderBootstrap SPI, ClusterMetadataProvider,        │
│   ServerAuthenticator / AuthenticationFactory, Authorizer,        │
│   ResourceType.GROUP (added 2026-04-24).                          │
└───────────────────────────────────────────────────────────────────┘
```

All file-level dependencies go downward. Fluss core has no knowledge
of Kafka; `fluss-kafka/` imports Fluss SPIs but never the reverse
except via one leader-scoped bootstrap hook loaded by `ServiceLoader`
(`CoordinatorLeaderBootstrap`).

## 2. Data-plane mapping

### 2.1 Kafka topic → Fluss table

| Kafka concept | Fluss shape | Where |
|---|---|---|
| Topic (default) | Log table, columns `(record_key BYTES, payload BYTES, event_time TIMESTAMP_LTZ(3) NOT NULL, headers ARRAY<ROW<name STRING, value BYTES>>)`, distributed by `numPartitions` | `KafkaDataTable.schema()`, `KafkaTableFactory.buildDescriptor` |
| Topic with `cleanup.policy=compact` | PK table, same columns but `record_key` is NOT NULL and the primary key. `KvFormat.INDEXED`. | `KafkaDataTable.schema(compacted=true)`. CreateTopics branches on `topic.configs().cleanup.policy`. |
| Kafka partition | Fluss table bucket within the same table | `TableBucket(tableId, partitionIndex)` |
| Kafka record | One log record (append-only) OR one KvRecord (upsert) | `KafkaProduceTranscoder.produceTopic` branches on `tableInfo.hasPrimaryKey()` |
| Kafka tombstone (`null` value on a compacted topic) | `KvRecordBatchBuilder.append(key, null)` → row-delete for the key | `KafkaProduceTranscoder.buildFlussKvRecords` |

**Consequence of the shape choice:** a topic's backing table is *either*
log-shaped *or* PK-shaped — decided at `CREATE_TOPICS` time and not
alterable. A later `AlterConfigs` that flips `cleanup.policy` gets
rejected (the shape can't migrate in place today). This mirrors
design 0002 §Phase B's "no in-place re-typing" precedent.

### 2.2 Produce → Fluss

| Layer | Log topic | Compacted topic |
|---|---|---|
| Entry | `KafkaRequestHandler.handleProduceRequest` (`src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:handleProduceRequest`) | same |
| Transcode | `KafkaProduceTranscoder.produceTopic` | `KafkaProduceTranscoder.producePartitionKv` |
| Row build | `buildFlussRecords` → `MemoryLogRecords` | `buildFlussKvRecords` → `DefaultKvRecordBatch` |
| Fluss call | `ReplicaManager.appendRecordsToLog(timeoutMs, acks, Map<TableBucket, MemoryLogRecords>, userContext, cb)` | `ReplicaManager.putRecordsToKv(timeoutMs, acks, Map<TableBucket, KvRecordBatch>, targetColumns=null, MergeMode.OVERWRITE, apiVersion, cb)` |
| Per-topic authz | `WRITE` on `TABLE(kafkaDatabase, topic)` | same |
| Idempotence (`enable.idempotence=true`) | producerId + sequence plumbed through `MemoryLogRecordsIndexedBuilder.setWriterState` (+ `WriterStateManager` on the replica side) | same plumbing applies on the KV side |

### 2.3 Fetch → Fluss

| Layer | Path |
|---|---|
| Entry | `KafkaRequestHandler.handleFetchRequest` |
| Transcode | `KafkaFetchTranscoder.fetch` |
| Fluss call | `ReplicaManager.fetchLogRecords(FetchSpecification, ...)` via `ClientFetchRequest` DTO (Phase B.4) |
| Per-topic authz | `READ` on `TABLE` |
| Compacted topic read | Same fetch path — Fluss PK tables expose a changelog log over the same offset space; consumers see individual changes, not the materialised snapshot |

`ClusterMetadataProvider` (`fluss-rpc/.../gateway/ClusterMetadataProvider.java`,
phase B.3) and `ReplicaSnapshot` (`fluss-rpc/.../replica/ReplicaSnapshot.java`)
are the only fluss-server types the fetch code reaches into today. Both are
stable, `@PublicEvolving`.

### 2.4 Consumer groups → Fluss

| Kafka resource | Fluss resource | Storage |
|---|---|---|
| Group state (generation, members, assignments) | In-memory, per-coordinator | `KafkaGroupRegistry` |
| Consumer-group offsets | Fluss PK table `<kafkaDatabase>.__consumer_offsets__` | `FlussPkOffsetStore` (Phase C earlier — config `kafka.offsets.store=fluss_pk_table`, default) |
| Group ACL | `Resource.group(groupId)` (Fluss `ResourceType.GROUP` added in Phase G.2) | `DefaultAuthorizer` grants table |

`ConsumerOffsetsTable` (Phase C) is the canonical Fluss-native shape
— `(group_id STRING, topic STRING, partition INT, offset BIGINT,
commit_timestamp, metadata)`, PK on `(group_id, topic, partition)`. A
Kafka `OffsetCommit` is an upsert; `OffsetFetch` is a lookup;
`OffsetDelete` is a row-delete. Read-after-write consistent.

### 2.5 Authorizer → Fluss `Resource`

The Kafka bolt-on passes through SASL to produce a `FlussPrincipal`,
then gates every handler against Fluss's native `Authorizer`. The
shape of the call is always
`authorizer.authorize(Session, OperationType, Resource)`; only the
`Resource` and `OperationType` change per handler.

| Kafka API | Fluss op | Resource |
|---|---|---|
| Produce | WRITE | `TABLE(kafkaDatabase, topic)` per topic |
| Fetch | READ | `TABLE` per topic |
| ListOffsets / OffsetForLeaderEpoch | DESCRIBE | `TABLE` per topic |
| DeleteRecords | DROP | `TABLE` per topic |
| CreatePartitions | ALTER | `TABLE` per topic |
| DescribeConfigs / AlterConfigs / IncrementalAlterConfigs (topic) | DESCRIBE / ALTER | `TABLE` per topic |
| DescribeProducers | READ | `TABLE` per topic |
| CreateTopics | CREATE | `DATABASE(kafkaDatabase)` |
| DeleteTopics | DROP | `TABLE` per topic |
| DescribeCluster | DESCRIBE | `CLUSTER` |
| ElectLeaders | ALTER | `CLUSTER` |
| DescribeClientQuotas / AlterClientQuotas | DESCRIBE / ALTER | `CLUSTER` |
| DescribeAcls | DESCRIBE | `CLUSTER` |
| CreateAcls / DeleteAcls | ALTER | `CLUSTER` |
| FindCoordinator | DESCRIBE | `GROUP(groupId)` |
| OffsetCommit / OffsetFetch | READ | `GROUP(groupId)` |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | READ | `GROUP(groupId)` |
| DeleteGroups / OffsetDelete | DROP | `GROUP(groupId)` |
| ListGroups / DescribeGroups | DESCRIBE | `CLUSTER` (coarse — batched APIs; per-group refinement is a later sub-phase) |
| API_VERSIONS / SASL_HANDSHAKE / SASL_AUTHENTICATE | — | pre-auth by design |

All per-topic / per-group enforcement uses the filter-request /
append-denied pattern so denied topics never reach the transcoder
(no side effects) and denied rows surface as
`TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED` /
`CLUSTER_AUTHORIZATION_FAILED` per Kafka-wire-convention. The
helper lives at `fluss-kafka/.../auth/AuthzHelper.java`.

### 2.6 SASL principal extraction

`KafkaCommandDecoder` owns a per-connection state machine (Phase F):

```
UNAUTHED
  ↓ SASL_HANDSHAKE(mechanism ∈ enabledMechanisms)
HANDSHAKE_DONE
  ↓ SASL_AUTHENTICATE(bytes)  (one or more rounds)
AUTHED(principal)
```

Mechanism-specific work is delegated to Fluss's `ServerAuthenticator`
SPI (`fluss-common/.../security/auth/ServerAuthenticator.java`) —
`fluss-kafka/` imports nothing from `javax.security.sasl.*`. PLAIN
ships today via `SaslAuthenticationPlugin` + `PlainSaslServer`.
SCRAM-SHA-256/512 is mechanism-agnostic on Fluss's side, so adding it
is a new provider on the classpath + nothing in the Kafka bolt-on
(Phase H).

On `AUTHED`, the decoder attaches the `FlussPrincipal` to every
subsequent `KafkaRequest`. `AuthzHelper.sessionOf(request)` wraps
that into the `Session` the Fluss authorizer expects.

## 3. Schema Registry → Fluss catalog

The SR is a projection, not a separate store. Every SR read / write is
a call on `CatalogService`.

| Kafka SR endpoint | Fluss catalog calls |
|---|---|
| `GET /subjects` | `listKafkaSubjects` |
| `POST /subjects/{s}/versions` | `resolveKafkaSubject` + `registerSchema` + `bindKafkaSubject` (idempotent on exact-match text) |
| `POST /subjects/{s}` (probe) | `resolveKafkaSubject` + `listSchemaVersions` scan + text-equality |
| `GET /subjects/{s}/versions` | `resolveKafkaSubject` + `listSchemaVersions` |
| `GET /subjects/{s}/versions/{v}` | `getSchemaVersion(db, topic, v)` |
| `GET /subjects/{s}/versions/latest` | `resolveKafkaSubject` + `getTableById.currentSchemaId` + `getSchemaBySchemaId` (PK chain) |
| `GET /schemas/ids/{id}` | `getSchemaById(srSchemaId)` |
| `GET /schemas/ids/{id}/subjects` | `getSchemaById` + scan `listKafkaSubjects` |
| `GET /schemas/ids/{id}/versions` | same + version from the `SchemaVersionEntity` |
| `GET /schemas/types` | hard-coded `["AVRO"]` today |
| `GET /config`, `PUT /config`, `GET /config/{s}`, `PUT /config/{s}`, `DELETE /config/{s}` | `getSrConfig` / `setSrConfig` / `deleteSrConfig` on `_sr_config` KV table |
| `GET /mode`, `PUT /mode`, `GET /mode/{s}`, `PUT /mode/{s}`, `DELETE /mode/{s}` | same |

`_sr_config` is a tiny KV table (`config_key STRING PK,
config_value STRING`). Keys follow `global_compatibility`,
`subject_compatibility:<s>`, `global_mode`, `subject_mode:<s>`. This
is the simplest way to store free-form SR state without introducing
another entity table per feature.

**Not yet wired** (on the stub today):
- `DELETE /subjects/{s}` / `DELETE /subjects/{s}/versions/{v}` — soft
  delete needs a `deleted BOOLEAN` column on `__schemas__` and
  `__kafka_bindings__`, or a tombstone entry in `_sr_config`.
- `POST /compatibility/subjects/{s}/versions/{v}` — real Avro / JSON
  / Protobuf compat checks. Lands with Phase SR-X.2.
- JSON Schema / Protobuf formats — lands with Phase T (typed tables)
  alongside format translators.

## 4. Deterministic SR schema id allocator

SR schema ids are a 31-bit integer keyed into `_catalog._id_reservations`.
Algorithm (Phase C.1):

```
id₀ = hash(tableId, schemaVersion, format) & 0x7fffffff
probe: upsert-if-absent into _id_reservations with (id₀, schema_id, format)
       if collision: id₁ = id₀ + 1, repeat up to 8 times, else fail.
```

The `_id_reservations` row is the authoritative mapping from
SR schema id → internal `schema_id`. The hash makes the id
*deterministic* on the inputs: re-registering the same Avro text
against the same subject on a fresh cluster produces the same
SR schema id. Linear probing keeps the 31-bit space dense.

## 5. Compacted topics: last-writer-wins via PK tables

New in 2026-04-24. `cleanup.policy=compact` on a Kafka topic creates
the Fluss backing table as a PK table with `record_key` as the primary
key. Produce writes go through a different path:

```
Kafka record → KvRecord(byte[] key, BinaryRow row)
                where key = kafkaRecord.key()  (must be non-null)
                      row = indexed row with (record_key, payload, event_time, headers)
             → KvRecordBatchBuilder.append(key, row)   (row=null for tombstones)
             → DefaultKvRecordBatch via BytesView → ByteBuffer
             → ReplicaManager.putRecordsToKv(..., MergeMode.OVERWRITE, ...)
```

**Semantics**:
- Per-key last-writer-wins on the snapshot view of the PK table.
- Kafka consumers tailing the topic see the changelog (all individual
  updates, not the materialised snapshot). This matches Kafka's own
  behaviour: a consumer of a compacted topic sees every write until
  compaction kicks in.
- Null-valued Kafka records are tombstones: `KvRecordBatchBuilder.append(key, null)`
  is the delete call Fluss's storage layer accepts.

**Constraints**:
- A compacted topic requires non-null keys. Kafka enforces this;
  records with null keys on a compacted topic are already rejected by
  the Kafka client.
- `cleanup.policy` is frozen at topic-create time. Switching via
  `AlterConfigs` is rejected (the backing table's PK cannot be added
  to an existing non-PK table).

**Known gap — fetch path for compacted topics**: Kafka-consumer read-
back from a PK-backed topic is not yet wired. `KafkaFetchTranscoder`
reads the log-offset space log tables use; the PK-table CDC changelog
uses different record framing. Attempting to consume a compacted
topic via `kafka-clients` today fails with *"Encountered corrupt
message"* because the Kafka client sees raw KV-format bytes. Produce
works; Fluss-native readers (Table API, Flink) see the snapshot
correctly. Follow-up: teach `KafkaFetchTranscoder` to detect
`hasPrimaryKey()` and read from the CDC stream, translating
`ChangeType.{APPEND, UPDATE_AFTER, DELETE}` into Kafka records
(DELETE → null-value tombstone). See
`KafkaCompactedTopicITCase` comment block for the planned test.

Tests: `KafkaCompactedTopicITCase` — `compact` topic accepts three
writes to the same key + one to a different key; default topic still
log-shaped. Consume-back is deferred to Phase 2 (see gap note above).

## 5.1 Why these decisions — the non-obvious calls

Seven places where the design forked and the reasoning isn't in the
code. Written down so the next edit doesn't silently undo them.

**1. Bolt-on rather than in-tree Kafka support.**
Landing Kafka API inside `fluss-server` would have meant Kafka-wire
types leaking through every internal API boundary — metadata, replica,
group coordination, authz. Instead we defined a narrow `@PublicEvolving`
SPI surface (`ClusterMetadataProvider`, `ReplicaSnapshot`,
`CoordinatorLeaderBootstrap`) and pushed everything Kafka-shaped into a
separate module. Payoff: Fluss-core refactors didn't ripple into
`fluss-kafka/` even once across 30+ commits. Cost: a few extra DTO
conversions at the interface, which the transcoders would do anyway.

**2. One table per topic, bucket = partition.**
Kafka partitions and Fluss buckets have the same role: the unit of
ordering and parallelism. Mapping 1:1 means Produce / Fetch / replica
assignment / leader election already work without a translation layer.
A topic with N partitions is a single Fluss table with N buckets, not
N tables. Alternative — one Fluss table per partition — would have
doubled metadata cost and made rebalance-on-reassignment a
table-graph problem.

**3. Consumer-group offsets in a Fluss PK table, not in ZK.**
Kafka stores offsets in `__consumer_offsets` — a normal Kafka topic
whose compaction gives last-writer-wins-per-key. Fluss's PK table
*is* that shape, with strong consistency. The offset store interface
lets us keep a legacy `ZkOffsetStore` for one release (retired soon)
while the `FlussPkOffsetStore` soaks. Both implementations are behind
`OffsetStore`; the handler never branches on which is active. Payoff:
offsets survive coordinator failover without a separate consensus
path — Fluss's replication is the consensus path.

**4. `cleanup.policy=compact` → Fluss PK table; no
cleanup-policy-flip.**
Kafka lets operators flip `cleanup.policy` on a live topic.
Structurally, that's "add a primary key to an existing log table",
which Fluss can't do today (bucket data is written without the key
index). Rather than pretend, `AlterConfigs` on `cleanup.policy` is
rejected — recreate the topic. Strictness over dishonesty; the error
message cites the reason.

**5. SR state in the catalog, not alongside topic properties.**
Phase A1 of the SR piggybacked on Kafka-table custom properties
(`kafka.sr.subject`, `kafka.sr.avro-schema`, `kafka.sr.sr-id`).
That stored Kafka SR-shaped data on a Kafka-shaped surface and did
not generalise to Iceberg-REST or Flink catalog. Phase C moved all
of it — subjects, versions, SR-id reservations, config — into
`_catalog` tables. The SR HTTP layer is now ~300 LOC of JSON
marshaling over `CatalogService` calls. Iceberg REST Phase-1 was
implemented in a week because the storage was already there.

**6. Deterministic SR schema ids via hash + linear probe.**
Operators expect `register(subject, text)` to be idempotent and
cheap. A counter-based allocator needs a coordinator round-trip per
registration; a hash avoids that round-trip as long as collisions
stay rare. 31-bit space with deterministic-hash + 8-slot linear
probe gives idempotence on re-registration and recoverability across
a fresh cluster (re-registering the same text produces the same id).
The `_id_reservations` row is still the authoritative mapping; the
hash is just the lookup seed.

**7. Reuse Fluss's `ServerAuthenticator` SPI for Kafka SASL, don't
build a parallel one.**
Fluss already has a pluggable auth SPI with a PLAIN provider. The
Kafka bolt-on's SASL work is a bridge: Kafka's `SASL_AUTHENTICATE`
frames carry opaque bytes; `ServerAuthenticator.evaluateResponse`
consumes opaque bytes. Pass-through, no re-encoding. The alternative
— a Kafka-shaped `KafkaSaslMechanism` SPI living in `fluss-kafka/` —
would have doubled the auth surface and forced a Fluss-native /
Kafka-wire split that benefits neither. Today a SCRAM provider added
on the classpath works for both traffic shapes.

## 6. Handler surface — implementation table

| Kafka API | Implemented | Handler file | Notes |
|---|---|---|---|
| API_VERSIONS | ✅ | `KafkaRequestHandler.handleApiVersionsRequest` | caps some API maxVersions (Metadata v11, Fetch v12) |
| METADATA | ✅ | `handleMetadataRequest` → `KafkaMetadataBuilder` | auto-create path honours ACLs |
| DESCRIBE_CLUSTER | ✅ | `handleDescribeClusterRequest` | |
| CREATE_TOPICS | ✅ | → `KafkaAdminTranscoder.createTopics` | reads `cleanup.policy` config |
| DELETE_TOPICS | ✅ | `KafkaAdminTranscoder.deleteTopics` | per-topic authz filter |
| PRODUCE | ✅ | `KafkaProduceTranscoder` | log + KV branch |
| INIT_PRODUCER_ID | ⚠️ stub | `handleInitProducerIdRequest` | mints a locally-unique id; real TX work deferred |
| FETCH | ✅ | `KafkaFetchTranscoder` | per-topic authz filter |
| LIST_OFFSETS | ✅ | `KafkaListOffsetsTranscoder` | |
| FIND_COORDINATOR | ✅ | `KafkaGroupTranscoder.findCoordinator` | `DESCRIBE on GROUP` |
| OFFSET_COMMIT | ✅ | `KafkaGroupTranscoder.offsetCommit` | |
| OFFSET_FETCH | ✅ | `KafkaGroupTranscoder.offsetFetch` | v0–v7 per-group gate; v8+ multi-group falls back to CLUSTER |
| JOIN_GROUP | ✅ | `KafkaGroupTranscoder.joinGroup` | |
| SYNC_GROUP | ✅ | `KafkaGroupTranscoder.syncGroup` | |
| HEARTBEAT | ✅ | `KafkaGroupTranscoder.heartbeat` | |
| LEAVE_GROUP | ✅ | `KafkaGroupTranscoder.leaveGroup` | |
| LIST_GROUPS | ✅ | `KafkaGroupTranscoder.listGroups` | coarse CLUSTER check |
| DESCRIBE_GROUPS | ✅ | `KafkaGroupTranscoder.describeGroups` | coarse CLUSTER check |
| DELETE_GROUPS | ✅ | `KafkaGroupTranscoder.deleteGroups` | |
| OFFSET_DELETE | ✅ | `KafkaGroupTranscoder.offsetDelete` | |
| DELETE_RECORDS | ✅ | `KafkaDeleteRecordsTranscoder` | trims via `ReplicaManager.deleteRecords` |
| OFFSET_FOR_LEADER_EPOCH | ✅ | `KafkaOffsetForLeaderEpochTranscoder` | no per-offset epoch history: returns `(-1, -1)` for older epochs |
| CREATE_PARTITIONS | ✅ | `KafkaCreatePartitionsTranscoder` | same-count = no-op; grow/shrink = INVALID_PARTITIONS (Fluss bucket count is fixed) |
| DESCRIBE_CONFIGS | ✅ | `KafkaConfigsTranscoder` | topic scope only |
| ALTER_CONFIGS | ✅ | same | |
| INCREMENTAL_ALTER_CONFIGS | ✅ | same | |
| ELECT_LEADERS | ✅ | `KafkaElectLeadersTranscoder` | classifies only (no elect call in Fluss core yet) |
| DESCRIBE_PRODUCERS | ✅ | `KafkaDescribeProducersTranscoder` | over `WriterStateManager` |
| DESCRIBE_CLIENT_QUOTAS | ✅ | `KafkaClientQuotasTranscoder` | |
| ALTER_CLIENT_QUOTAS | ✅ | same | storage only — no throttle enforcement yet |
| DESCRIBE_ACLS | ✅ | `KafkaAclsTranscoder` | LITERAL pattern only |
| CREATE_ACLS | ✅ | same | DENY rejected (Fluss ACL model is ALLOW-only) |
| DELETE_ACLS | ✅ | same | |
| SASL_HANDSHAKE | ✅ | `KafkaSaslTranscoder` (intercepted in decoder) | |
| SASL_AUTHENTICATE | ✅ | same | PLAIN mechanism ships; SCRAM is Phase H |
| ADD_PARTITIONS_TO_TXN | ❌ stub | → UNSUPPORTED_VERSION | Phase J |
| ADD_OFFSETS_TO_TXN | ❌ stub | same | Phase J |
| END_TXN | ❌ stub | same | Phase J |
| WRITE_TXN_MARKERS | ❌ stub | same | Phase J |
| TXN_OFFSET_COMMIT | ❌ stub | same | Phase J |

**Count**: 32 fully implemented + 1 stubbed (INIT_PRODUCER_ID) + 5
deferred (transactional). Three third-party clients
(`kafka-clients` Java, `kcat` C, `confluent-kafka` Python) all
round-trip, verified in-tree.

## 7. Persistence mapping for non-topic state

| Kafka concept | Fluss storage |
|---|---|
| Topic bytes | Log tablet (`_data`) or PK tablet (`_kv`) — see §2.1 |
| Consumer group offsets | `<kafkaDatabase>.__consumer_offsets__` PK table |
| Group metadata (generation, members) | In-memory on the coordinator (`KafkaGroupRegistry`). Survives reconnects but not coordinator failover — Phase J/TX needs a durable store here. |
| SR schema ids | `_catalog._id_reservations` PK table |
| Subject bindings | `_catalog._kafka_bindings` PK table |
| Schema versions | `_catalog._schemas` append-only |
| Principals | `_catalog._principals` |
| Grants (Fluss-native + Kafka-mapped) | `_catalog._grants` |
| Client quotas | `_catalog._client_quotas` (storage only, no throttle) |
| SR config (compat level, mode) | `_catalog._sr_config` (`global_compatibility`, `subject_compatibility:<s>`, `global_mode`, `subject_mode:<s>`) |

## 8. Bootstrap lifecycle

```
CoordinatorServer.initCoordinatorLeader
  └─ ServiceLoader<CoordinatorLeaderBootstrap>  (sorted by priority)
       ├─ 10   FlussCatalogBootstrap         (registers catalog service for later hooks)
       ├─ 100  SchemaRegistryBootstrap        (HTTP listener; consumes CatalogService)
       └─ 100  IcebergRestBootstrap           (HTTP listener; second projection over the same catalog)

Each bootstrap takes (Configuration, ZooKeeperClient, MetadataManager,
ServerMetadataCache, List<Endpoint>). Lazy Fluss Connection opening —
eager open races NotCoordinatorLeaderException.
```

System tables under `_catalog` are created lazily per handle. Each
fresh creation is followed by a `waitUntilReady` PK-lookup poll (Phase
C.1 worked around a replica-schema propagation race on batched
bulk-create).

## 9. Non-goals / explicit exclusions

- **Controller-to-broker APIs** (`LEADER_AND_ISR`, `STOP_REPLICA`,
  `UPDATE_METADATA`, `BROKER_HEARTBEAT`, `CONTROLLED_SHUTDOWN`,
  `UPDATE_FEATURES`, `UNREGISTER_BROKER`, `ALLOCATE_PRODUCER_IDS`).
  Kafka uses these for KRaft / ZK controller-to-broker plumbing;
  Fluss uses its own coordinator + ZK for equivalents.
- **Transactions** (6 APIs). Explicitly deferred.
- **SASL_SSL / SSL listener protocols.** Needs a TLS listener config
  on Fluss's side.
- **Delegation tokens.** 3 APIs. No demand signal yet.
- **Typed-table auto-conversion** — Kafka Avro payload → Fluss typed
  row. Stored as opaque bytes today; Phase T adds the format
  translators + compiled codecs.
- **Topic-level retention / size-based cleanup** — `retention.ms` and
  `retention.bytes` are accepted and stored on the table, but log
  segment trimming today only runs on explicit `DELETE_RECORDS`. The
  background retention-driven trimmer is Fluss-core work.

## 10. Verification matrix (current, 2026-04-24)

| Harness | Scope | File | Count |
|---|---|---|---|
| `mvn verify -pl fluss-kafka` | all Kafka ITs + units | | 80 ITs + 51 unit tests |
| kafka-clients Java | every implemented API | `Kafka*ITCase` | 60+ ITs |
| kcat (librdkafka) | metadata, produce, consume, watermark | `KafkaKcatSmokeITCase` | 3 scenarios |
| confluent-kafka (Python) | metadata, produce+consume, group resume, watermarks, headers, multi-partition | `KafkaPythonE2EITCase` + `fluss-kafka/src/test/python/fluss_kafka_e2e.py` | 6 scenarios |
| SASL/PLAIN end-to-end | SASL → principal → authorizer → TOPIC_AUTHORIZATION_FAILED | `KafkaSaslPlainITCase`, `KafkaTopicAuthzITCase` | 3 + 3 scenarios |
| Compacted topics | PK-table write path | `KafkaCompactedTopicITCase` | 2 scenarios |
| SR Kafka SR-REST | Avro register + schema-id lookup + list versions + config/mode | `SchemaRegistryHttpITCase` | 5 scenarios |
| Iceberg REST (Phase E preview) | /v1/config, /v1/namespaces round-trip | `IcebergRestHttpITCase` | 4 scenarios |
| Catalog (Phase C) | namespaces, tables, schemas, RBAC | `FlussCatalogServiceITCase` | 7 scenarios |

## 11. Follow-ups that unlock the next leverage point

1. **Phase H — SCRAM** (mechanism-agnostic SASL is wired; adds
   SCRAM-SHA-256/512 + user-credentials admin surface).
2. **Phase SR-X.2** — real compatibility check per format (Avro via
   `org.apache.avro.SchemaCompatibility`, JSON via clean-room, Protobuf
   via `kafka-protobuf-provider` shaded) + the missing
   `/compatibility/...` endpoint.
3. **Phase T — typed tables** — the biggest lift. See design 0002 +
   plan §22. Format translator + Janino-compiled codec cache +
   `ALTER TABLE` on first schema registration. Unlocks SQL / Flink /
   Spark reads over typed columns rather than opaque bytes.
4. **Phase J — transactions**. Deferred per user decision; ~3–4
   weeks of focused work + a coordinator state machine + a durable
   `__kafka_txn_state__` entity table + log-level TX markers.

None of these are blockers for the current non-transactional
producer / consumer / admin story.
