# Design 0012 — Kafka bolt-on observability: per-API, per-topic, per-group metrics + structured request log

**Status:** Phase M gap-fill, 2026-04-24.
**Supersedes:** none — narrows the observability promise in §3 of design 0001
and the metric surface referenced throughout design 0006 to a concrete
six-wire-point contract plus three sub-group types.
**Related:** `0001-kafka-api-protocol-and-metadata.md`,
`0006-kafka-api-mapping-into-fluss.md`, `0010-kafka-acl-enforcement.md`.

## Context

The Kafka bolt-on now advertises 32 wire APIs (see `IMPLEMENTED_APIS` at
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java:191`),
is SASL-authenticated end-to-end, and gates every data-plane handler
through `AuthzHelper`. An operator running this in anger needs the answer
to three real-deployment questions: *which API is burning CPU*, *which
topic is driving bytes-in*, and *which consumer group is rebalancing
repeatedly*. Today the aggregate surface lives — `kafka.bytesIn`,
`kafka.bytesOut`, `kafka.authSuccessRate`, and the rest come from
`BoltOnMetricGroup` — but the three dimension-split views that a Kafka
operator expects (`kafka.api.*`, `kafka.topic.*`, `kafka.group.*`) are not
published, and there is no per-request log line an operator can grep to
correlate a slow `JoinGroup` with the client that issued it.

What a Kafka-shop operator expects from a broker-shaped metric scope is
unambiguous: names under `kafka.server.*` that carry `api_name` /
`topic` / `group_id` variables, rates and histograms rather than
gauges-of-cumulative-counters, and a cardinality bound so a runaway test
harness cannot DoS the reporter. Fluss can reuse most of the shape —
`BoltOnMetricGroup` already has request-rate, error-rate, bytes-in/out,
auth-success/failure, authz-allow/deny, and a lazy sub-group factory
with cardinality caps baked in
(`fluss-rpc/src/main/java/org/apache/fluss/rpc/metrics/BoltOnMetricGroup.java:54`).
What it must add on the Kafka side is (1) the three named sub-group
flavours above, wired to the handlers that already know the API /
topic / group, (2) four new config keys that the cardinality caps read
from, and (3) a single per-request DEBUG line emitted from the one
try/finally in `KafkaRequestHandler.processRequest`.

## Status at time of writing

`KafkaMetricGroup`
(`fluss-kafka/src/main/java/org/apache/fluss/kafka/metrics/KafkaMetricGroup.java:37`)
extends `BoltOnMetricGroup` and is wired at server startup by the
Kafka-listener bootstrap. The aggregate metrics below land under the
scope `<cluster>.<host>.kafka.` and are queryable via every reporter
registered on the `MetricRegistry`. What is missing is the dimensioned
sub-group publication — the base class *allocates* `ApiMetricGroup`s and
`SessionEntityMetricGroup`s lazily, but the Kafka layer never asks for
them on the per-API path, only on the per-topic and per-group paths.

| Metric | Type | Source site | Notes |
|---|---|---|---|
| `activeConnections` | Gauge<Integer> | `BoltOnMetricGroup.java:101` | Incremented in `KafkaCommandDecoder.channelActive` L228, decremented in `channelInactive` L247. |
| `connectionsCreatedPerSecond` | Meter | `BoltOnMetricGroup.java:102` | Co-incremented with `activeConnections`. |
| `connectionsClosedPerSecond` | Meter | `BoltOnMetricGroup.java:103` | Co-incremented with `activeConnections` on close. |
| `requestsPerSecond` | Meter | `BoltOnMetricGroup.java:105` | Driven by `onRequest` at `KafkaRequestHandler.java:476`. |
| `errorsPerSecond` | Meter | `BoltOnMetricGroup.java:106` | Same call; `isError` derived from the response future. |
| `requestProcessTimeMs` | Histogram (window 1024) | `BoltOnMetricGroup.java:110` | Wall-clock from handler entry to future completion. |
| `bytesInPerSecond` | Meter | `BoltOnMetricGroup.java:107` | Kafka produce-side only today; does not include request headers. |
| `bytesOutPerSecond` | Meter | `BoltOnMetricGroup.java:108` | Kafka fetch-side only. |
| `messagesInPerSecond` | Meter | `BoltOnMetricGroup.java:109` | Rolled up across topics. |
| `authSuccessPerSecond` | Meter | `BoltOnMetricGroup.java:112` | `KafkaCommandDecoder.java:162` via `onSaslOutcome`. |
| `authFailurePerSecond` | Meter | `BoltOnMetricGroup.java:113` | Same site, `ok=false` branch. |
| `authzAllowPerSecond` | Meter | `BoltOnMetricGroup.java:114` | Five call sites in `AuthzHelper` (L123, L141, L174, L181, L185). |
| `authzDenyPerSecond` | Meter | `BoltOnMetricGroup.java:115` | Same five-site path, deny branch. |
| *per-API sub-group* | — | absent | `apiGroup(apiName)` is called from `onRequest` but `ApiMetricGroup`'s `requestBytes` / `responseBytes` / `totalTimeMs` slots are unpopulated today. |
| *per-topic sub-group* | — | partial | `SessionEntityMetricGroup` gets `onBytesIn/Out/onOperation/onError` from `recordProduce/recordFetch` (`KafkaMetricGroup.java:61/73`). No explicit `produceErrorRate`/`fetchErrorRate` split — errors collapse into one `errorsPerSecond` meter. |
| *per-consumer-group sub-group* | — | partial | `recordJoin/recordHeartbeat/recordOffsetCommit` call `onOperation` / `onError` but do not populate `memberCount`, `rebalanceRate`, or `joinLatencyMs`. |
| per-request DEBUG log | — | partial | A log line exists at `KafkaRequestHandler.java:484`; it lacks `clientId`, `requestBytes`, `responseBytes`, and `errorCode` (short form) and is not guarded under a single try/finally. |

Everything *under* `kafka.authz.*` / `kafka.sasl_mechanism.*` is already
published as dimension sub-groups by `onSaslOutcome` / `onAuthzOutcome`
(`KafkaMetricGroup.java:125/144`), so only the three primary dimensions
(`api`, `topic`, `group`) need filling in.

## Target sub-group design

Three sub-groups, one per operator question. Each one is a lazy child
of the Kafka `BoltOnMetricGroup` instance — no pre-allocation, no
per-tick sweep. Scope shape follows `makeScope(parent, dimension,
name)` from `BoltOnMetricGroup.java:97` so every entity group's path is
`<cluster>.<host>.kafka.<dimension>.<name>` and a single reporter
rewriter can flatten it.

### Per-API sub-group — `kafka.api.<api_name>`

One sub-group per observed `ApiKeys` enum value, keyed by the lowercase
enum name (`produce`, `fetch`, `join_group`, `describe_acls`, …). API
cardinality is bounded by the wire protocol — 32 today, upper-bound
~80 including txn APIs we acknowledge but do not serve — so no cap is
needed.

| Metric | Type | Wire point |
|---|---|---|
| `requestsPerSecond` | Meter | `BoltOnMetricGroup.ApiMetricGroup.onRequest` via `KafkaRequestHandler.processRequest` |
| `errorsPerSecond` | Meter | Same call, `isError` derived from `isErrorResponse(resp)` |
| `requestBytes` | Histogram (DescriptiveStatisticsHistogram, window 1024) | Wrap at `processRequest` — measure `KafkaRequest.requestSize()` |
| `responseBytes` | Histogram | Wrap at `processRequest` — measure the `AbstractResponse` encoded size |
| `requestProcessTimeMs` | Histogram | Handler-time only. Reuses the `REQUEST_PROCESS_TIME_MS` name — same meaning as the aggregate. |
| `totalTimeMs` | Histogram | Handler entry → `sendResponse` flush. Reuses `REQUEST_TOTAL_TIME_MS`. |

`ApiMetricGroup` today only populates three of the six slots
(`BoltOnMetricGroup.java:325`). Phase M extends it with the
`requestBytes`, `responseBytes`, and `totalTimeMs` histograms.

Lazy creation on first use: `apiGroup(apiName)` is keyed by the
pre-interned lowercase name; the `ConcurrentMap` put is a no-op after
the first request of each API. Per-API memory footprint is ~4
histograms + 2 meters ≈ 1.5 KiB — 32 × 1.5 KiB ≈ 48 KiB ceiling.

### Per-topic sub-group — `kafka.topic.<name>`

One sub-group per observed Kafka topic. Topic names are free-text and
user-supplied, so this dimension is capped.

| Metric | Type | Wire point |
|---|---|---|
| `bytesInPerSecond` | Meter | `recordProduce` in `KafkaRequestHandler.recordProduceMetrics` L977 |
| `bytesOutPerSecond` | Meter | `recordFetch` in `recordFetchMetrics` L2152 |
| `messagesInPerSecond` | Meter | `recordProduce` — records count |
| `produceErrorPerSecond` | Meter | New split off the single `onError` — error flag + op = PRODUCE |
| `fetchErrorPerSecond` | Meter | New split off the single `onError` — error flag + op = FETCH |

Cardinality cap: `kafka.metrics.per-topic.max-cardinality` (default
`1000`; see §8). Overflow rolls into `kafka.topic.__overflow__` — one
shared sub-group whose meters sum across every topic that arrived
after the cap. A single WARN is logged the first time overflow is
entered (`BoltOnMetricGroup.cappedLookup` L245 path), never again.

The `produceErrorPerSecond` / `fetchErrorPerSecond` split requires a
small widening of `SessionEntityMetricGroup` — today it has one
`errorsPerSecond` meter keyed to a single `errors` counter
(`BoltOnMetricGroup.java:371`). Phase M adds two more counters +
meters, and `recordProduce` / `recordFetch` pick the right one based
on the API they came from.

### Per-consumer-group sub-group — `kafka.group.<id>`

One sub-group per Kafka consumer-group id seen on `JoinGroup`,
`Heartbeat`, or `OffsetCommit`. Consumer-group names are free-text and
user-supplied; cap applies.

| Metric | Type | Wire point |
|---|---|---|
| `memberCount` | Gauge<Integer> | Register once per group; read from `KafkaGroupRegistry.memberCount(groupId)` |
| `rebalancePerSecond` | Meter | Every `JoinGroup` with `groupInstanceId` → null-or-new triggers a rebalance |
| `heartbeatPerSecond` | Meter | `handleHeartbeatRequest` L1209 already calls `recordHeartbeat` — meter it |
| `offsetCommitPerSecond` | Meter | `handleOffsetCommitRequest` L1037 already calls `recordOffsetCommit` |
| `joinLatencyMs` | Histogram (window 1024) | `JoinGroup` entry → response complete; already measured at `KafkaRequestHandler.java:1146` as `elapsedMs`, currently discarded |

Cardinality cap: `kafka.metrics.per-group.max-cardinality` (default
`500`). Same overflow + one-time WARN behaviour as the topic dimension.

The `memberCount` gauge is the one metric that needs a read-through
into live state — `KafkaGroupRegistry` keeps the membership list per
group id, and `SessionEntityMetricGroup.registerGauge` already has the
idempotent-register helper at `BoltOnMetricGroup.java:410`. The gauge
closes over the group-registry reference and the group id, so it
updates without a per-heartbeat write.

## Cardinality cap implementation

The base-class cap at `BoltOnMetricGroup.cappedLookup` L223 is a
ConcurrentMap-plus-size-check. It is *not* LRU today — once 1000
topics have been seen, every new topic falls into `__overflow__` and
stays there, and the 1000 that *got* in keep their slot even if they
are stale. For Phase M the cap stays as-is: long-lived topics are the
common case, and adding an LRU would need a per-access write to a
`lastTouchedNanos` field in every entity group (a hot-path write we
should avoid without evidence that stale-topic retention is a real
problem).

If a future phase needs LRU eviction: replace the `ConcurrentMap` with
`org.apache.fluss.utils.clock.SystemClock`-stamped
`SessionEntityMetricGroup` entries, keep the eviction contract
(LRU-on-`lastTouchedNanos`, fold-into-`__overflow__`), and unregister
the evicted entity group from the `MetricRegistry` so the reporter
stops emitting it. That is ~40 LOC and one extra config key
(`kafka.metrics.eviction.enabled`). Out of scope here — tracked as a
note, not a commitment.

Eviction contract when it ships:

1. On every `entityGroup`/`clientGroup` lookup, stamp
   `lastTouchedNanos = System.nanoTime()`.
2. When the map size would exceed the cap, evict the minimum-stamped
   entry first. Fold its counters into `__overflow__` before closing
   its `AbstractMetricGroup` (so no data is lost, only
   de-dimensioned).
3. Log the WARN once per cap event, as today.

## Structured DEBUG logging

The existing DEBUG line at `KafkaRequestHandler.java:484` is wired off
`future.whenComplete` — correct for latency, but it runs on whatever
thread completes the future (usually the tablet-server IO pool), and
it is missing three fields an operator needs (`clientId`,
`requestBytes`, `responseBytes`, `errorCode`). Phase M rewrites the
dispatch in `processRequest` as a single try/finally so both the
metric update and the log line see the same `startNanos`,
`requestBytes`, `responseBytes`, and `errorCode`.

Signature:

```
LOG.debug(
    "kafka-request api={} apiVersion={} clientId={} principal={} correlationId={}"
        + " bytes.in={} bytes.out={} elapsed.ms={} error={}",
    request.apiKey().name(),
    request.apiVersion(),
    request.header().clientId(),
    request.principal().getName(),
    request.header().correlationId(),
    requestBytes,
    responseBytes,
    elapsedMs,
    errorCode);
```

Fields:

| Field | Source |
|---|---|
| `api` | `request.apiKey().name()` (uppercase enum — matches `ApiKeys` wire) |
| `apiVersion` | `request.apiVersion()` |
| `clientId` | `request.header().clientId()` — populated by the Kafka client in every request |
| `principal` | `request.principal().getName()` — `FlussPrincipal` from SASL or `ANONYMOUS` on PLAINTEXT |
| `correlationId` | `request.header().correlationId()` |
| `requestBytes` | Measured at decode time; stashed on `KafkaRequest` |
| `responseBytes` | Measured at `sendResponse` flush |
| `elapsedMs` | `(System.nanoTime() - startNanos) / 1_000_000L` |
| `errorCode` | `Errors.forException(err).code()` when future completed exceptionally; else the first non-zero per-topic error code from the response; else `"none"`. Short form. |

Guarded under `LOG.isDebugEnabled()` — one line per request at INFO
would swamp the log at even modest throughput (10 kRPS → 864M
lines/day). No MDC: Fluss-wide convention is inline fields in a
single logger-formatted message, matching the existing Fluss-native
request logs in `fluss-server/src/main/java/org/apache/fluss/server/rpc/*`.

## INFO / WARN / ERROR policy

| Level | Event | Site |
|---|---|---|
| INFO | Listener bound | `KafkaServer.start` (on the first `ChannelFuture` sync) |
| INFO | First channelActive | `KafkaCommandDecoder.java:222` — already present |
| INFO | SASL mechanism enabled | `KafkaServerContext` ctor when `sasl.enabled.mechanisms` non-empty |
| INFO | Config rejected at startup | `KafkaServerConfig.validate` — any misconfiguration logs and throws |
| DEBUG | Per-request | `KafkaRequestHandler.processRequest` — one line per request |
| WARN | Authz denied | `AuthzHelper.authorizeOrThrow` (one line per deny, includes principal + op + resource) |
| WARN | Unsupported API version negotiated down | `handleApiVersionsRequest` when the client's `max` > our `max` |
| WARN | Cardinality cap hit | `BoltOnMetricGroup.cappedLookup` L238/L247 — already present, one-shot per cap |
| ERROR | Failed frame decode | `KafkaCommandDecoder.decode` catch block — unrecoverable parse error |
| ERROR | Handler threw unhandled | Every handler's outer `catch (Throwable t)` (already present, 30+ call sites) |
| ERROR | Connection state corrupt | `KafkaCommandDecoder` when `inflightResponses` sees a request out of order |

The WARN for unsupported API version is new — today the negotiation
silently picks the overlap. The WARN level matches what Kafka's
`ApiVersion` path does when a client asks for something the broker
does not support, and makes the common "old client / new server"
problem obvious in logs.

## Six wire points

The canonical bolt-on observability contract is six wire points.
Every Fluss bolt-on (Kafka today, Iceberg REST next, future HTTP
projections after) hits the same six, in the same base class:

| # | Wire point | File | Method | Call |
|---|---|---|---|---|
| 1 | Connection open | `KafkaCommandDecoder.java` | `channelActive` L217 | `kafkaMetrics.onConnectionOpened()` L228 |
| 2 | Connection close | `KafkaCommandDecoder.java` | `channelInactive` L234 | `kafkaMetrics.onConnectionClosed()` L247 |
| 3 | Per-request wrap | `KafkaRequestHandler.java` | `processRequest` L463 | try/finally around dispatch; `metrics.onRequest(api, elapsed, err)` + per-API sub-group histograms |
| 4 | Produce bytes-in | `KafkaRequestHandler.java` | `recordProduceMetrics` L946 | `metrics.recordProduce(topic, bytes, records, err)` L977 |
| 5 | Fetch bytes-out | `KafkaRequestHandler.java` | `recordFetchMetrics` L2131 | `metrics.recordFetch(topic, bytes, err)` L2152 |
| 6a | SASL outcome | `KafkaCommandDecoder.java` | SASL-handshake completion L160 | `kafkaMetrics.onSaslOutcome(ok, mechanism)` L162 |
| 6b | Authz outcome | `AuthzHelper.java` | `authorizeTopicBatch` + `authorizeOrThrow` | `metrics.onAuthzOutcome(allowed, op, rt)` L123, L141, L174, L181, L185 |
| 6c | Group lifecycle | `KafkaRequestHandler.java` | `handleJoinGroupRequest` L1147, `handleHeartbeatRequest` L1209, `handleOffsetCommitRequest` L1037 | `m.recordJoin` / `recordHeartbeat` / `recordOffsetCommit` |

Wire points 1, 2, 3, and 6a/6b/6c are already landed (per the status
table above); 3 is the one that needs the rewrite in `processRequest`
to pick up the four new histograms plus the improved log line, and
4/5 need the error-split widening. Everything else is a read-only
audit for completeness.

## Config keys

Four new `ConfigOption`s live in `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`.
Two of them (`KAFKA_METRICS_PER_TOPIC_ENABLED`,
`KAFKA_METRICS_PER_TOPIC_MAX_CARDINALITY`,
`KAFKA_METRICS_PER_GROUP_ENABLED`,
`KAFKA_METRICS_PER_GROUP_MAX_CARDINALITY`) are already defined at
L2100–L2130 as part of the existing `fluss kafka` config block. Phase
M wires them to the `KafkaMetricGroup` constructor — today the
constructor takes `topicMaxCardinality` and `groupMaxCardinality` as
`int`s at L41 but the server bootstrap passes hard-coded values.

| Key | Type | Default | Read at |
|---|---|---|---|
| `kafka.metrics.per-topic.enabled` | boolean | `true` | `KafkaMetricGroup` ctor — if false, `topicMetrics` returns null and the per-topic branch is skipped |
| `kafka.metrics.per-topic.max-cardinality` | int | `1000` | `KafkaMetricGroup` ctor → `BoltOnMetricGroup` super |
| `kafka.metrics.per-group.enabled` | boolean | `true` | `KafkaMetricGroup` ctor — same null-skip pattern |
| `kafka.metrics.per-group.max-cardinality` | int | `500` | `KafkaMetricGroup` ctor → `BoltOnMetricGroup` super |

All four are read once at `KafkaMetricGroup` construction (restart to
change). That matches the Fluss-wide convention documented in §8 of
the project CLAUDE guide — hierarchical dot-separated keys with
hyphens, no hot-reload, server restart required for metric-surface
changes. Description strings follow the existing voice of the
neighbouring options (L2101–L2130).

Per-API metrics have no enable flag — the cardinality is bounded by
the protocol (32 today) and operators on a Kafka-flavoured deployment
always want the per-API view. Disabling it would also break the one
generic per-request log field (`api`) that every reporter depends on.

## Files

### Add

| File | Purpose | LOC |
|---|---|---|
| `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaObservabilityITCase.java` | End-to-end IT: asserts the seven metrics in §10 fire | ~300 |

### Modify

| File | Change | Expected LOC |
|---|---|---|
| `fluss-rpc/src/main/java/org/apache/fluss/rpc/metrics/BoltOnMetricGroup.java` | Widen `ApiMetricGroup` with `requestBytes` / `responseBytes` / `totalTimeMs` histograms; widen `SessionEntityMetricGroup` with `operation`-tagged error counters | ~80 |
| `fluss-rpc/src/main/java/org/apache/fluss/rpc/metrics/BoltOnMetricNames.java` | Add `REQUEST_BYTES`, `RESPONSE_BYTES`, `REQUEST_TOTAL_TIME_MS` constants (reuse existing `MetricNames.*` where they already exist) | ~10 |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/metrics/KafkaMetricGroup.java` | Read the four config keys; widen `recordProduce`/`recordFetch` signatures to carry the op kind; extend `recordJoin` to record the histogram value; register `memberCount` gauge on first `recordJoin` | ~70 |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java` | Rewrite `processRequest` L463 as a single try/finally; measure `requestBytes` / `responseBytes`; replace the existing DEBUG log with the nine-field one | ~60 |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaServerContext.java` | Read `kafka.metrics.per-topic.*` / `kafka.metrics.per-group.*` config keys; pass them to `KafkaMetricGroup` ctor instead of hard-coded values | ~20 |
| `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaCommandDecoder.java` | Log the negotiated-down API version WARN when we observe a client downgrade | ~10 |

### Module-placement question

`BoltOnMetricGroup` already lives in `fluss-rpc`
(`fluss-rpc/src/main/java/org/apache/fluss/rpc/metrics/BoltOnMetricGroup.java`),
so the question "should it move from `fluss-kafka` into `fluss-rpc`"
is moot — it is already in the lower module. That is the correct
place for it: `fluss-kafka` and the forthcoming `fluss-iceberg-rest`
are sibling modules that cannot cross-depend (per the module-boundary
rule in §10 of the project CLAUDE guide), and they share this
infrastructure. No move needed.

`BoltOnMetricNames` (same package) is also in `fluss-rpc`. The only
Kafka-specific flavour — `KafkaMetricGroup` — correctly lives in
`fluss-kafka` because it adds Kafka-protocol-aware helpers on top
(`recordProduce`, `recordFetch`, `onSaslOutcome`, `onAuthzOutcome`,
`recordJoin`/`Heartbeat`/`OffsetCommit`) and depends on Kafka types
(`OperationType`, `ResourceType`).

## Verification

New IT at
`fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaObservabilityITCase.java`.
Single-node `FlussClusterExtension` with `kafka.enabled=true`,
`authorizer.enabled=true` for the denied-produce scenario, PLAIN-SASL
JAAS config for `admin` + `alice`, both per-topic/per-group metric
flags left at their defaults. Listener `KAFKA`, Kafka database
`kafka`. Reads the metric surface off the cluster's `MetricRegistry`
via a small test-only accessor — same pattern as
`FlinkTestBase.getMetrics`.

Scenarios:

- **Topic bytes-in/out symmetry.** Open a `KafkaProducer`, send 100
  records split 50/50 across `kafka.topic-a` and `kafka.topic-b`.
  Assert `kafka.topic.topic-a.bytesInPerSecond` and
  `kafka.topic.topic-b.bytesInPerSecond` both have count > 0 and
  reflect the ~25 records × payload-size each. Open a `KafkaConsumer`,
  poll all 100. Assert `bytesOutPerSecond` > 0 for both topic
  sub-groups. Pins wire points 4 and 5.
- **SASL failure counter increment.** Spin a second `KafkaProducer`
  with a deliberately wrong password. Assert the connection is
  rejected, `kafka.authFailurePerSecond` count increased by at least
  1, and `kafka.authSuccessPerSecond` is unchanged. Pins wire point
  6a.
- **Authz denial counter increment.** With alice authenticated and no
  ACL grants, attempt `producer.send(new ProducerRecord("kafka.denied", ...))`.
  Assert the future completes with `TopicAuthorizationException`,
  `kafka.authzDenyPerSecond` count +1. Pins wire point 6b.
- **Three-consumer `JoinGroup` member count.** Spin up three
  `KafkaConsumer`s in group `ops-readers`, subscribed to a
  pre-created topic, poll once each to force the rebalance. Assert
  `kafka.group.ops-readers.memberCount` gauge reads `3` within 5 s
  (await-and-assert with a 100 ms poll). Assert
  `kafka.group.ops-readers.rebalancePerSecond` count ≥ 1,
  `joinLatencyMs` histogram has at least 3 samples. Pins wire point
  6c plus the `memberCount` gauge registration.
- **Cardinality overflow.** Configure `kafka.metrics.per-topic.max-cardinality=5`
  (override in the `FlussClusterExtension` builder). Produce one
  record each to 6 distinct topics. Assert
  `kafka.topic.__overflow__` sub-group exists, has `bytesInCount > 0`,
  and the topic-5 sub-group (4th seen, 0-indexed) is present while
  topic-6 is not in the primary map. Assert exactly one WARN was
  logged matching `cardinality cap (5) reached` (use the
  `FlussClusterExtension` log-tap helper). Pins the cap + overflow
  contract.
- **Per-request DEBUG log content.** Set the `KafkaRequestHandler`
  logger to DEBUG for the test, produce one record, consume it, and
  grep the captured log for two lines: `api=PRODUCE …` and
  `api=FETCH …`, each with `clientId`, `principal`, `bytes.in`,
  `bytes.out`, `elapsed.ms`, `error=none`. Pins wire point 3.

All assertions use AssertJ per §1 of the project CLAUDE guide; no
hardcoded values — topic names, group ids, record payloads are all
generated via `RandomStringUtils.randomAlphanumeric` so repeated runs
don't re-use state.

## Reusability for future bolt-ons

The Iceberg REST projection (Phase E preview — see design 0006 §0) is
the immediate second consumer of `BoltOnMetricGroup`. It will extend
the base class with `subsystemName() = "iceberg_rest"`, wire its own
channel-open / channel-close hooks off the HTTP listener's
`channelActive` / `channelInactive`, and wrap each REST dispatch in
the same try/finally shape as `KafkaRequestHandler.processRequest`.
Per-table and per-namespace become the two entity dimensions, replacing
Kafka's topic and consumer-group. The six wire points — channel open,
channel close, per-request wrap, auth outcome, authz outcome, session
lifecycle — carry over unchanged, which is precisely why the base
class lives in `fluss-rpc`.

A future HTTP bolt-on (e.g. the Flink catalog bridge) would do the
same, with its own pair of cardinality-capped dimensions. The cap
configuration pattern — two keys per dimension (`enabled` +
`max-cardinality`) — becomes the template; each bolt-on defines its
own keys under its own prefix (`iceberg-rest.metrics.per-table.*`, for
instance) and passes the values into its `BoltOnMetricGroup` subclass
constructor.

## Scope

~250 LOC production across `fluss-rpc` (widenings to
`ApiMetricGroup` / `SessionEntityMetricGroup`) and `fluss-kafka`
(`KafkaMetricGroup` ctor wiring + `KafkaRequestHandler.processRequest`
rewrite + `KafkaServerContext` config read), plus ~300 LOC for the
single new IT. One PR, one component tag `[kafka][metrics]`, no
cross-module refactor beyond the two files already in `fluss-rpc`.
No breaking changes to the existing aggregate metric surface —
`kafka.bytesInPerSecond` / `requestsPerSecond` / `authSuccessPerSecond`
all keep their current names and semantics, the Phase M deltas are
strictly additive (new sub-groups + new fields in the DEBUG line).
