# FIP-NN: Kafka API compatibility for Fluss — minimal slice

**Status:** Draft
**Date:** 2026-05-14
**Authors:** Ben Gamble <ben.gamble@ververica.com>
**Discussion:** _(GitHub issue link goes here once filed)_
**Cwiki page:** _(link to Apache Confluence FIP once assigned a number)_

## Motivation

Many teams already speak Kafka and want a single substrate for both
streaming records and analytical tables. Fluss has the storage primitives
(Arrow log tables, KV tables, ZK coordination) but no front door for Kafka
clients. This FIP introduces that front door, deliberately scoped to the
*plain-log* case so reviewers can evaluate the protocol mapping in isolation
from schema-registry, typed-table, and transactional concerns.

The smallest end-to-end useful surface is: a stock `kafka-clients` producer
publishes records, a stock consumer (or `kafka-console-consumer` / kcat)
reads them back through a consumer group, and operators can list and create
topics via `kafka-topics.sh` or the Java `AdminClient`. Everything beyond
that — schema registry, typed columns, transactions, EOS, compaction,
SCRAM/ACL admin, quotas — is intentionally deferred so this proposal can
land as a single reviewable change.

## Goals

1. Speak the Kafka wire protocol on a dedicated listener.
2. Serve the data plane (`Produce` / `Fetch` / `ListOffsets`) backed by a
   Fluss Arrow log table per topic.
3. Serve consumer-group coordination (`Find/Join/Sync/Heartbeat/Leave` +
   `OffsetCommit`/`OffsetFetch` + `List`/`Describe`/`Delete` groups).
4. Serve the minimum admin surface a Kafka client needs to bootstrap and
   manage topics: `ApiVersions`, `Metadata`, `DescribeCluster`,
   `CreateTopics`, `DeleteTopics`, `DescribeConfigs`, `AlterConfigs`,
   `FindCoordinator`, `Sasl*`.
5. Authenticate Kafka clients via SASL/PLAIN on the Kafka listener.
6. Enforce per-topic and per-group ACLs through the existing Fluss
   `Authorizer.checkPrivilege` SPI.
7. Expose per-API/topic/group metrics with configurable cardinality caps.
8. Ship the entire feature as a **bolt-on module** — `fluss-kafka` — that
   plugs into the server via the existing `NetworkProtocolPlugin`
   ServiceLoader. **No changes to `fluss-common`, `fluss-rpc`, or
   `fluss-server` beyond SPIs that already exist on `main`.**

## Non-goals (will be follow-up FIPs)

- Compacted (`cleanup.policy=compact`) topics → Fluss PK tables. In this
  FIP, `CreateTopics` rejects `cleanup.policy=compact` with
  `INVALID_CONFIG`.
- Confluent Schema Registry HTTP endpoint + subject management.
- Typed Kafka topics (Avro / JSON / Protobuf decoded into typed columns).
- Transactions, EOS, read-committed isolation, idempotent producers
  (`InitProducerId` / `AddPartitionsToTxn` / `EndTxn` / `WriteTxnMarkers` /
  `TxnOffsetCommit` / `DescribeTransactions` / `ListTransactions`).
- ACL admin APIs (`CreateAcls` / `DeleteAcls` / `DescribeAcls`).
- SCRAM credential admin (`Describe/AlterUserScramCredentials`).
- Client quotas (`Describe/AlterClientQuotas`).
- Partition scaling (`CreatePartitions`), record deletion
  (`DeleteRecords`), leader election (`ElectLeaders`),
  `OffsetForLeaderEpoch`, `DescribeProducers`.
- Streams / Connect compatibility harness.
- Iceberg REST catalog endpoint.

## Public interfaces

### New listener type

Kafka clients connect on a dedicated `KAFKA://host:port` listener
configured via `bind.listeners`. The Fluss-native listener is unchanged.
Plugin discovery is via the standard `ServiceLoader` entry at
`META-INF/services/org.apache.fluss.rpc.protocol.NetworkProtocolPlugin`.

### Configuration

All Kafka config keys are declared in
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaConfigOptions.java`.
They have the `kafka.` prefix and are parsed by the standard
`org.apache.fluss.config.Configuration` machinery.

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `kafka.enabled` | boolean | false | Feature flag. The protocol plugin self-disables when false. |
| `kafka.listener-names` | string | `KAFKA` | Comma-separated listener names that speak Kafka. |
| `kafka.database` | string | `kafka` | Fluss database that backs Kafka topics. |
| `kafka.offsets.store` | enum | `IN_MEMORY` | One of `IN_MEMORY` / `ZK` / `FLUSS_PK`. |
| `kafka.log-format` | enum | `ARROW` | `ARROW` (default) or `INDEXED`. Pins on-disk shape for new log topics. |
| `kafka.metrics.topic-limit` | int | 1000 | Cardinality cap for per-topic metrics. |
| `kafka.metrics.group-limit` | int | 1000 | Cardinality cap for per-group metrics. |
| `kafka.sasl.enabled-mechanisms` | list | `[PLAIN]` | Listener-side SASL mechanisms. |
| `kafka.sasl.plain.jaas-config` | string | (none) | JAAS config for SASL/PLAIN. |

### Storage mapping

Each Kafka topic maps to a Fluss table in `kafka.database`.

**Schema (passthrough 4-tuple):**

| Column | Type | Source |
|--------|------|--------|
| `record_key` | `BYTES` | Kafka record key (nullable) |
| `value` | `BYTES` | Kafka record value (nullable, null → tombstone is not supported in this FIP since compacted topics are out of scope) |
| `event_time` | `TIMESTAMP_LTZ(3)` | Kafka record timestamp (`CreateTime` or `LogAppendTime` per topic config) |
| `headers` | `ARRAY<ROW<name STRING, value BYTES>>` | Kafka record headers (nullable, empty array when absent) |

**Log format:** `ARROW` by default. The same row layout used by Fluss-native
log tables, so anyone wanting to read these from Flink/SQL can — they'll
get raw bytes back for key/value and decode them themselves.

**Topic properties** (`NewTopic.configs()`):
- `message.timestamp.type` → controls how `event_time` is populated.
- `compression.type` → captured as a Fluss custom property, applied to the
  underlying log table.
- `cleanup.policy` → only `delete` is accepted. `compact` returns
  `INVALID_CONFIG` per the non-goal above.
- `retention.ms` → translated to `table.log.ttl`.

### Supported Kafka APIs

Cluster / handshake: `ApiVersions`, `Metadata`, `DescribeCluster`,
`FindCoordinator`, `SaslHandshake`, `SaslAuthenticate`.

Topic admin: `CreateTopics`, `DeleteTopics`, `DescribeConfigs`,
`AlterConfigs`.

Data plane: `Produce`, `Fetch`, `ListOffsets`.

Consumer groups: `OffsetCommit`, `OffsetFetch`, `JoinGroup`, `SyncGroup`,
`Heartbeat`, `LeaveGroup`, `ListGroups`, `DescribeGroups`, `DeleteGroups`.

Any other ApiKey returns `UNSUPPORTED_VERSION` (or the closest matching
Kafka error) via the existing dispatch fallthrough.

### Security

- **Authentication:** SASL/PLAIN on the Kafka listener. Server-side JAAS
  config maps Kafka principals to Fluss principals via
  `kafka.sasl.principal-mapping` (passed through to the existing
  `Authorizer` SPI).
- **Authorization:** topic-level ACLs (`READ` / `WRITE`) on `Produce`,
  `Fetch`, `CreateTopics`, `DeleteTopics`, `DescribeConfigs`,
  `AlterConfigs`, `Metadata`, and group-coordination APIs that target a
  topic. Group-scoped ACLs (`READ` on a consumer group) are deferred to a
  follow-up FIP so this slice does not need to add a new `ResourceType` to
  `fluss-common`. `Authorizer.checkPrivilege` is the existing SPI and is
  not modified.
- **ACL admin APIs are out of scope.** ACLs are managed via Fluss-native
  tooling for this FIP.

### Observability

- `KafkaMetricGroup` exposes per-API latency (p50/p95/p99) and request
  counts, plus per-topic and per-group counters. All per-topic/per-group
  series respect the `kafka.metrics.topic-limit` /
  `kafka.metrics.group-limit` caps; once exceeded, additional cardinality
  is bucketed into an `__other__` series so cardinality explosion is
  prevented.
- Structured logs at INFO/WARN/ERROR for: listener startup, topic
  create/delete, group rebalance, ACL deny, produce errors.

## Proposed changes (high-level)

- **New module:** `fluss-kafka/` (~22k LOC after the cuts) — protocol
  plugin, request handler, per-API transcoders, catalog projection, offset
  stores, metrics, auth wiring.
- **`fluss-common`, `fluss-rpc`, `fluss-server`:** no net changes. The
  Kafka plugin reads `Configuration` using `KafkaConfigOptions` (declared
  inside `fluss-kafka`), and owns its own `ArrowWriterPool` +
  `BufferAllocator` lifecycle inside `KafkaServerContext`. The plugin
  discovers `ReplicaManager`, the metric registry, and the `Authorizer`
  via `TabletService` reflection — already established pattern.

### Module layout

```
fluss-kafka/src/main/java/org/apache/fluss/kafka/
  KafkaProtocolPlugin.java
  KafkaChannelInitializer.java
  KafkaCommandDecoder.java
  KafkaRequestHandler.java
  KafkaRequest.java
  KafkaErrors.java
  KafkaServerContext.java
  KafkaConfigOptions.java          # <-- all kafka.* config keys live here

  admin/                           # CreateTopics, DeleteTopics, DescribeConfigs, AlterConfigs
  auth/                            # SASL/PLAIN, ACL helpers
  catalog/                         # topic ↔ Fluss table mapping, table-descriptor builder
  fetch/                           # Fetch transcoder, ListOffsets, passthrough codec
  group/                           # consumer-group coordinator, offset stores
  metadata/                        # Kafka topic metadata builders
  metrics/                         # KafkaMetricGroup + cardinality caps
  produce/                         # Produce transcoder (Arrow log appender)
```

No new types are added to `fluss-common` or `fluss-server`.

## Compatibility, deprecation, migration

- **Compatibility:** off-by-default (`kafka.enabled=false`). Existing
  Fluss clients are unaffected. Enabling the listener creates Kafka topics
  as ordinary Fluss tables under `kafka.database` — they can be read with
  Fluss SQL and Flink, but the row schema is opaque bytes for key/value.
- **Deprecation:** none.
- **Migration:** existing Kafka topics must be re-created against the
  Fluss-backed brokers; there is no in-place migration tool in this FIP.

## Test plan

End-to-end ITCases against a real `kafka-clients` 3.x driver, all under
`FlussClusterExtension`:

- `KafkaRequestITCase` — low-level dispatch.
- `KafkaAdminClientITCase`, `KafkaCreateTopicsITCase`,
  `KafkaAdminConfigsITCase` — admin surface.
- `KafkaProduceITCase`, `KafkaProducerITCase`, `KafkaRoundTripITCase` —
  Produce / Fetch round-trips with key/value/headers.
- `KafkaConsumerITCase`, `KafkaConsumerOffsetsITCase`,
  `KafkaMultiConsumerRebalanceITCase`, `KafkaSubscribeModeITCase` —
  consumer groups.
- `KafkaSaslPlainITCase`, `KafkaTopicAuthzITCase` — SASL listener + ACL.
- `KafkaMetricsITCase`, `KafkaObservabilityITCase` — metrics + logs.
- `KafkaArrowProduceITCase`, `KafkaIndexedLogFormatITCase` — log format
  selection.
- `KafkaKcatSmokeITCase` — third-party C client (kcat) round-trip.

Manual matrix:
- kcat produce → kcat consume on the same topic.
- `kafka-console-producer.sh` → `kafka-console-consumer.sh --group` with
  rebalance.
- SASL_PLAINTEXT connection with a wrong password (expect
  `SASL_AUTHENTICATION_FAILED`).
- `kafka-topics.sh --create` with `--config cleanup.policy=compact` →
  expect `INVALID_CONFIG`.

Performance baseline (informational, not a gate): produce 100k records/sec
on a 3-broker test cluster; compare against the existing Fluss-native
producer at the same record cadence.

## Rejected alternatives

- **Embed Kafka brokers as a sidecar.** Duplicates storage, adds a
  failure domain, doubles the operator burden. Rejected.
- **Map every Kafka topic to a Fluss PK table** (always upsert by key).
  Wastes the Arrow log format's columnar layout for the >90% case where
  the topic isn't compacted. Rejected for non-compacted topics.
- **Include schema registry in this FIP.** Would inflate the PR surface
  by 30+ files and conflate two design conversations. Rejected — separate
  follow-up FIP.
- **Edit `fluss-common` / `fluss-server` to host Kafka concerns** (config
  options, ACL resource types, Arrow pool accessors). Couples a
  protocol-bolt-on to core types. Rejected — everything goes in
  `fluss-kafka`.
- **Single-commit PR (squash all phased work).** Loses authorship trail
  and review granularity. Rejected — the PR ships as a handful of logical
  commits (config / catalog / data-plane / groups / auth / metrics / tests
  + docs).

## References

- Apache Fluss Improvement Proposals (cwiki):
  `https://cwiki.apache.org/confluence/display/FLUSS/Fluss+Improvement+Proposals`
- Apache Kafka protocol guide:
  `https://kafka.apache.org/protocol`
- `kafka-clients` source (the reference implementation Fluss must
  interoperate with): `https://github.com/apache/kafka`
