# Design 0010 — Kafka ACL enforcement: audit, SR-HTTP principal, and the missing E2E gate

**Status:** Phase G gap-fill, 2026-04-24.
**Supersedes:** none — narrows §7 of design 0006 and §4 of design 0002 to a
single enforcement audit.
**Related:** `0001-kafka-api-protocol-and-metadata.md`,
`0002-kafka-schema-registry-and-typed-tables.md`,
`0005-catalog-and-projections.md`, `0006-kafka-api-mapping-into-fluss.md`.

## Context

Phase F landed SASL/PLAIN + SCRAM on the Kafka listener. After that, every
inbound `KafkaRequest` carries a `FlussPrincipal` that reflects a real
authenticated identity instead of the placeholder `FlussPrincipal.ANONYMOUS`
used by PLAINTEXT listeners. But SASL on its own is decorative: if a handler
accepts the request without consulting `Authorizer.authorize(session, op,
resource)`, the principal has no effect on the reply the client gets.
Everything still works the way it did before Phase F, just with more TLS on
the wire.

Phase G turns that principal into a gate. The majority of Kafka handlers in
`fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java`
already call `AuthzHelper.authorizeOrThrow(...)` or
`AuthzHelper.authorizeTopicBatch(...)` before reaching the transcoders; what
this phase does is audit that coverage, close any holes, and land the one
path the audit flags as uncovered — the Schema Registry HTTP listener, which
runs outside the Kafka wire pipeline and therefore never sees the SASL
principal. When the SR handler asks `SchemaRegistryService` "who is the
caller?" today, the answer is hard-wired to `ANONYMOUS`, so
`kafka.schema-registry.rbac.enforced=true` plus any non-wildcard catalog
grant will forbid everyone. This document closes that and adds the IT that
proves the authorizer actually fires end-to-end on both wire and HTTP
surfaces.

## Status at time of writing

The enforcement plumbing is in place on both sides; the only functional gap
is SR-HTTP principal wiring into `SchemaRegistryService.callerPrincipal()`.

| Piece | File | Line / symbol | Status |
|---|---|---|---|
| Authz adapter | `fluss-kafka/src/main/java/org/apache/fluss/kafka/auth/AuthzHelper.java` | `sessionOf` L68, `authorizeTopicBatch` L101/111, `authorizeOrThrow` L155/165 | Shipped. Returns "allow" when `authorizer == null` so PLAINTEXT and the testing-gateway setup stay green. |
| Authorizer handle | `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaServerContext.java` | `authorizer()` L238 | Shipped. Nullable; handlers must no-op authz when null. |
| Authorizer SPI | `fluss-server/src/main/java/org/apache/fluss/server/authorizer/Authorizer.java` | `isAuthorized` L60, `authorize` L73, `addAcls` L91, `dropAcls` L102, `listAcls` L110 | Shipped. `authorize` throws `AuthorizationException`; `isAuthorized` returns boolean and is the one `AuthzHelper.authorizeTopicBatch` calls. |
| Resource types | `fluss-common/src/main/java/org/apache/fluss/security/acl/ResourceType.java` | `GROUP` L66 | Shipped in Phase G.2. GROUP is a direct child of CLUSTER — cluster-wide grants roll up; database grants don't. |
| Operation types | `fluss-common/src/main/java/org/apache/fluss/security/acl/OperationType.java` | enum L34–L44 | Unchanged. `ALL`, `READ`, `WRITE`, `CREATE`, `DROP`, `ALTER`, `DESCRIBE`; READ/WRITE/CREATE/DROP/ALTER imply DESCRIBE. |
| `IMPLEMENTED_APIS` | `fluss-kafka/src/main/java/org/apache/fluss/kafka/KafkaRequestHandler.java` | L191–L232 | 33 API keys including `DESCRIBE_ACLS`, `CREATE_ACLS`, `DELETE_ACLS`, the two SCRAM-credentials APIs, and the two SASL APIs (intercepted in the decoder — never reach the handler). |
| `ADVERTISED_APIS` | same file | L299–L342 | Superset of `IMPLEMENTED_APIS` for txn APIs we acknowledge but do not serve. |
| `dispatch` switch | same file | L496–L605 | One `case` per key in `IMPLEMENTED_APIS`; default → `handleUnsupportedRequest` (L603). |
| SR principal carrier | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryCallContext.java` | `set`/`clear`/`current` | Shipped. ThreadLocal, set by the HTTP handler before dispatch, cleared in `finally`. |
| SR principal extractor | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/auth/HttpPrincipalExtractor.java` | `extract(FullHttpRequest, InetSocketAddress)` L101 | Shipped. Header first, Basic second. |
| SR Basic-auth store | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/auth/JaasHttpPrincipalStore.java` | `fromJaasText` L61 | Shipped. Parses the PLAIN-SASL JAAS grammar. |
| SR config options | `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` | `KAFKA_SCHEMA_REGISTRY_TRUSTED_PROXY_CIDRS` L2208, `KAFKA_SCHEMA_REGISTRY_BASIC_AUTH_JAAS_CONFIG` L2225 | Shipped. Empty defaults keep the SR open only to `ANONYMOUS`, i.e. RBAC off. |
| SR handler wire-in | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryHttpHandler.java` | `channelRead0` L80, `resolvePrincipal` L112 | Shipped. Reads the principal, pushes it onto `SchemaRegistryCallContext`, clears in `finally`. |
| SR service read-back | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryService.java` | `callerPrincipal` L278 | Reads `SchemaRegistryCallContext.current()`; falls back to `PrincipalEntity.ANONYMOUS` at L281 when no principal was set. |
| SR wire-in factory | `fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/SchemaRegistryBootstrap.java` | L83–L85 | `JaasHttpPrincipalStore.fromJaasText(basicAuthJaas)` → `HttpPrincipalExtractor` → `SchemaRegistryHttpHandler`. |
| Existing Kafka-wire IT | `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaTopicAuthzITCase.java` | class Javadoc | Proves CREATE-TOPICS deny/allow through SASL+Session+Authorizer on one API. |

What is *not* in place: an IT that exercises Produce, ACL admin round-trip
(CreateAcls/DescribeAcls/DeleteAcls), and SR-HTTP authorization together.
The existing `KafkaTopicAuthzITCase` only covers CreateTopics. Until that IT
lands, nothing pins the HTTP principal path and no test fails if someone
wires a fresh handler past `AuthzHelper`.

## Audit — per-handler authz coverage

Every API key in `IMPLEMENTED_APIS` (L191–L232) is listed below with the
call site of its authz gate inside `KafkaRequestHandler.java`. Line numbers
reference the gate call itself (the `AuthzHelper.authorizeOrThrow(` or
`AuthzHelper.authorizeTopicBatch(` line), not the handler entry.

| API key | Handler | Gate line | Op | Resource | Notes |
|---|---|---|---|---|---|
| `API_VERSIONS` | `handleApiVersionsRequest` L657 | — | — | — | No authz (correct). Advertises versions before any auth step. |
| `METADATA` | `handleMetadataRequest` L689 | — | — | — | Per-topic filtering happens inside `KafkaMetadataBuilder`; coverage tracked separately under phase 0001 — does not expose topic data. |
| `DESCRIBE_CLUSTER` | `handleDescribeClusterRequest` L708 | L716 | DESCRIBE | CLUSTER | `authorizeOrThrow` → `CLUSTER_AUTHORIZATION_FAILED`. |
| `CREATE_TOPICS` | `handleCreateTopicsRequest` L739 | L749 | CREATE | DATABASE(kafkaDatabase) | Deny returns per-topic `CLUSTER_AUTHORIZATION_FAILED`. |
| `DELETE_TOPICS` | `handleDeleteTopicsRequest` L778 | L790 | DROP | TABLE(kafkaDatabase, topic) | Topic batch; denied rows get `TOPIC_AUTHORIZATION_FAILED`. |
| `PRODUCE` | `handleProduceRequest` L858 | L873 | WRITE | TABLE(kafkaDatabase, topic) | Metered batch; denied topics emit `TOPIC_AUTHORIZATION_FAILED` per partition. |
| `INIT_PRODUCER_ID` | `handleInitProducerIdRequest` L2186 | — | — | — | Stub id generator today; no authz gate. Tracked as a known minor gap (see Gap list). |
| `FETCH` | `handleFetchRequest` L2074 | L2089 | READ | TABLE(kafkaDatabase, topic) | Metered batch; filterFetchByAllowed drops denied topics. |
| `LIST_OFFSETS` | `handleListOffsetsRequest` L1335 | L1350 | DESCRIBE | TABLE(kafkaDatabase, topic) | Topic batch. |
| `FIND_COORDINATOR` | `handleFindCoordinatorRequest` L981 | `denyGroupIfUnauthorized` L990 → `authorizeOrThrow` L1743 | DESCRIBE | GROUP(key) | Per-group, via helper. Key defaults to cluster for non-group coordinators. |
| `OFFSET_COMMIT` | `handleOffsetCommitRequest` L1005 | helper → L1743 | READ | GROUP(groupId) | Per-group gate. |
| `OFFSET_FETCH` | `handleOffsetFetchRequest` L1046 | helper → L1743 | READ | GROUP(groupId) | Same helper. Multi-group v8+ falls back to CLUSTER. |
| `LIST_GROUPS` | `handleListGroupsRequest` L1242 | helper → L1743 | DESCRIBE | CLUSTER | Multi-group, cluster-scoped. |
| `DESCRIBE_GROUPS` | `handleDescribeGroupsRequest` L1312 | helper → L1743 | DESCRIBE | CLUSTER | Same. |
| `JOIN_GROUP` | `handleJoinGroupRequest` L1118 | helper → L1743 | READ | GROUP(groupId) | |
| `SYNC_GROUP` | `handleSyncGroupRequest` L1157 | helper → L1743 | READ | GROUP(groupId) | |
| `HEARTBEAT` | `handleHeartbeatRequest` L1190 | helper → L1743 | READ | GROUP(groupId) | |
| `LEAVE_GROUP` | `handleLeaveGroupRequest` L1218 | helper → L1743 | READ | GROUP(groupId) | |
| `DELETE_GROUPS` | `handleDeleteGroupsRequest` L1265 | helper → L1743 | DROP | CLUSTER | Batch; today coarse-checked against CLUSTER. See Gap list. |
| `OFFSET_DELETE` | `handleOffsetDeleteRequest` L1288 | helper → L1743 | DROP | GROUP(groupId) | Per-group. |
| `DELETE_RECORDS` | `handleDeleteRecordsRequest` L1495 | L1510 | DROP | TABLE(kafkaDatabase, topic) | Topic batch. |
| `OFFSET_FOR_LEADER_EPOCH` | `handleOffsetForLeaderEpochRequest` L1389 | L1405 | DESCRIBE | TABLE(kafkaDatabase, topic) | Topic batch. |
| `CREATE_PARTITIONS` | `handleCreatePartitionsRequest` L1445 | L1461 | ALTER | TABLE(kafkaDatabase, topic) | Topic batch. |
| `DESCRIBE_CONFIGS` | `handleDescribeConfigsRequest` L1549 | via `authorizeConfigsByTopic` L1714 | DESCRIBE | TABLE(kafkaDatabase, topic) | Topic batch, filtered to resourceType=TOPIC only. Broker/cluster resources pass through. |
| `ALTER_CONFIGS` | `handleAlterConfigsRequest` L1591 | L1609 | ALTER | TABLE(kafkaDatabase, topic) | Topic batch. |
| `INCREMENTAL_ALTER_CONFIGS` | `handleIncrementalAlterConfigsRequest` L1644 | L1662 | ALTER | TABLE(kafkaDatabase, topic) | Topic batch. |
| `ELECT_LEADERS` | `handleElectLeadersRequest` L1734 | L1743 | ALTER | CLUSTER | Top-level `CLUSTER_AUTHORIZATION_FAILED` on deny. |
| `DESCRIBE_PRODUCERS` | `handleDescribeProducersRequest` L2018 | L2034 | READ | TABLE(kafkaDatabase, topic) | Metered topic batch. |
| `DESCRIBE_CLIENT_QUOTAS` | `handleDescribeClientQuotasRequest` L1766 | L1775 | DESCRIBE | CLUSTER | |
| `ALTER_CLIENT_QUOTAS` | `handleAlterClientQuotasRequest` L1799 | L1808 | ALTER | CLUSTER | |
| `DESCRIBE_ACLS` | `handleDescribeAclsRequest` L1846 | L1857 | DESCRIBE | CLUSTER | First checks `context.authorizer() == null` → returns `SECURITY_DISABLED`. |
| `CREATE_ACLS` | `handleCreateAclsRequest` L1885 | L1902 | ALTER | CLUSTER | Same SECURITY_DISABLED preamble. |
| `DELETE_ACLS` | `handleDeleteAclsRequest` L1935 | L1952 | ALTER | CLUSTER | Same. |
| `DESCRIBE_USER_SCRAM_CREDENTIALS` | `handleDescribeUserScramCredentialsRequest` L607 | — | — | — | Known gap — no authz gate today. SCRAM store is a broker-private secret set; should be gated ALTER on CLUSTER. |
| `ALTER_USER_SCRAM_CREDENTIALS` | `handleAlterUserScramCredentialsRequest` L618 | — | — | — | Same gap, same fix. |
| `SASL_HANDSHAKE` | intercepted in `KafkaCommandDecoder` | — | — | — | No authz (correct). Never reaches the handler. |
| `SASL_AUTHENTICATE` | intercepted in `KafkaCommandDecoder` | — | — | — | No authz (correct). Never reaches the handler. |

The "helper → L1743" rows all funnel through
`denyGroupIfUnauthorized(KafkaRequest, OperationType, String)` which is
itself a thin wrapper around `AuthzHelper.authorizeOrThrow` at the cited
line. Errors are translated to Kafka's `GroupAuthorizationException` /
`GROUP_AUTHORIZATION_FAILED` inside that helper.

## Gap list

Three handlers land without an authz call today. None of them leak topic
data; two leak cluster-admin surface and one leaks a producer-id, so they
still need gating.

- `handleInitProducerIdRequest` (L2186) — issues a monotonically increasing
  stub producer id to any caller. Should be gated `WRITE` on `CLUSTER` (Kafka's
  `IdempotentWrite`) or, if we want finer control later, `WRITE` on
  `TABLE(kafkaDatabase, transactional.id)`. Fix is three lines of
  `authorizeOrThrow(... OperationType.WRITE, Resource.cluster())` inside the
  handler's `try` block.
- `handleDescribeUserScramCredentialsRequest` (L607) — reads the broker
  SCRAM credential store. Should be gated `DESCRIBE` on `CLUSTER`.
- `handleAlterUserScramCredentialsRequest` (L618) — mutates that same
  store. Should be gated `ALTER` on `CLUSTER`.
- SR HTTP — the known functional gap this phase closes end-to-end. The
  `HttpPrincipalExtractor` + `JaasHttpPrincipalStore` + `SchemaRegistryCallContext`
  wiring is already in the tree, but there is no IT that covers it, so no
  test fails when the call chain breaks. The IT in §8 pins the chain.

Two additional coarse gates are documented here because the audit surfaced
them but they are *intentional* today:

- `DELETE_GROUPS` and `LIST_GROUPS` / `DESCRIBE_GROUPS` fall back to a
  cluster-scoped check when the request does not carry a single groupId.
  Per-member iteration would be more accurate but requires splitting the
  transcoder's batch logic; deferred until a concrete complaint surfaces.
- `DESCRIBE_CONFIGS` / `ALTER_CONFIGS` / `INCREMENTAL_ALTER_CONFIGS` pass
  through broker and cluster-scoped resource rows unchecked. They can only
  be used to describe server configs, which are already world-readable,
  so the loose path is not a privilege escalation. Revisit if broker-scope
  ALTER is ever wired.

## SR HTTP principal extraction

The SR HTTP listener runs on a Netty event-loop group independent of the
Kafka wire port, so it has no SASL session to read a principal off of. Two
trust paths cover the realistic deployment shapes — a reverse proxy that
terminates its own auth, and a dev-mode Basic-auth shortcut — and an empty
default denies both, so an operator flipping `rbac.enforced=true` without
also setting a trust path gets "everyone is ANONYMOUS, everything is
forbidden" rather than "everyone is admin". That is the intended failure
mode.

**Trust path 1: `X-Forwarded-User`.** A reverse proxy (NGINX, Envoy, the
Confluent REST sidecar) terminates Basic/OIDC auth and forwards the
resolved username in the header. The SR honours it only when the remote
socket address matches one of the CIDR blocks in
`kafka.schema-registry.trusted-proxy-cidrs`. Empty list → header is never
trusted. The parse loop builds a list of `CidrRange` objects at startup;
runtime check is a single linear scan (trusted proxy fleets are small).

**Trust path 2: HTTP Basic.** `Authorization: Basic <base64>` is decoded,
split on the first `:`, and the pair is looked up in a
`JaasHttpPrincipalStore` built from
`kafka.schema-registry.basic-auth-jaas-config`. JAAS text is the same
shape as `security.sasl.listener.name.<listener>.plain.jaas.config` — one
`PlainLoginModule required user_<name>="<password>";` block — which lets
operators reuse credentials they already have for SASL.

**Precedence.** Header wins when both are present. Rationale: if a trusted
proxy is forwarding an identity, it already asserted that identity on its
terminating listener; a stale `Authorization` header the client forgot to
strip must not override it.

**Signatures (already in the tree).**

```java
// fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/auth/HttpPrincipalExtractor.java
public final class HttpPrincipalExtractor {
    public HttpPrincipalExtractor(
            List<String> trustedProxyCidrs, JaasHttpPrincipalStore basicAuthStore);
    public Optional<FlussPrincipal> extract(
            FullHttpRequest request, InetSocketAddress remote);
    public static final String HEADER_FORWARDED_USER = "X-Forwarded-User";
    public static final String HEADER_AUTHORIZATION = "Authorization";
}

// fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/auth/JaasHttpPrincipalStore.java
public final class JaasHttpPrincipalStore {
    public JaasHttpPrincipalStore(Map<String, String> credentials);
    public static JaasHttpPrincipalStore fromJaasText(String jaasConfigText);
    public Optional<String> authenticate(String user, String password);
}
```

**Per-request lifecycle.** `SchemaRegistryHttpHandler.channelRead0` (L80)
calls `resolvePrincipal(ctx, request)` (L112) exactly once per request,
pushes the result onto `SchemaRegistryCallContext.set(principal)` (L84),
dispatches, and clears in the surrounding `finally` block (L108). The
clear-in-finally is load-bearing: the SR handler is synchronous on the
event-loop thread and we do not want a principal leaking into the next
request when the loop reuses the thread. `SchemaRegistryCallContext` is
intentionally a `ThreadLocal` rather than a Netty `AttributeKey` — the
service layer has no `ChannelHandlerContext` to read from and we don't
want it to grow one just for this.

**Fallback.** When neither path yields a principal,
`HttpPrincipalExtractor.extract` returns `Optional.empty()`, the handler
skips the `set()` call, and `SchemaRegistryService.callerPrincipal()`
(L278) reads `null` from the context and returns
`PrincipalEntity.ANONYMOUS` (L281). That keeps tests and unauthenticated
dev deployments working exactly as before.

## Kafka → Fluss resource/op mapping

Kafka's ACL model keys on `(ResourceType, ResourceName, Operation)`; Fluss
keys on `(Resource, OperationType)`. The adapter in
`fluss-kafka/src/main/java/org/apache/fluss/kafka/auth/KafkaAclsTranscoder.java`
translates between the two for DescribeAcls/CreateAcls/DeleteAcls; the
per-handler gates above use the same mapping implicitly.

| Kafka op | Kafka resource | Fluss `OperationType` | Fluss `Resource` | Handler entry |
|---|---|---|---|---|
| Write | Topic | `WRITE` | `TABLE(kafkaDatabase, topic)` | Produce |
| Read | Topic | `READ` | `TABLE(kafkaDatabase, topic)` | Fetch, DescribeProducers |
| Describe | Topic | `DESCRIBE` | `TABLE(kafkaDatabase, topic)` | ListOffsets, OffsetForLeaderEpoch, DescribeConfigs |
| Create | Topic | `CREATE` | `DATABASE(kafkaDatabase)` | CreateTopics (gate is on the backing database, not per-topic — the topic doesn't exist yet) |
| Delete | Topic | `DROP` | `TABLE(kafkaDatabase, topic)` | DeleteTopics, DeleteRecords |
| Alter | Topic | `ALTER` | `TABLE(kafkaDatabase, topic)` | CreatePartitions, AlterConfigs, IncrementalAlterConfigs |
| Read | Group | `READ` | `GROUP(groupId)` | JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch |
| Describe | Group | `DESCRIBE` | `GROUP(groupId)` | FindCoordinator (group case) |
| Delete | Group | `DROP` | `GROUP(groupId)` | OffsetDelete |
| Describe | Group (multi-group) | `DESCRIBE` | `CLUSTER` | ListGroups, DescribeGroups, multi-group OffsetFetch, DeleteGroups (see Gap list — deliberate coarsening) |
| Describe | Cluster | `DESCRIBE` | `CLUSTER` | DescribeCluster, DescribeClientQuotas |
| Alter | Cluster | `ALTER` | `CLUSTER` | ElectLeaders, AlterClientQuotas |
| Describe | Acl (cluster) | `DESCRIBE` | `CLUSTER` | DescribeAcls |
| Alter | Acl (cluster) | `ALTER` | `CLUSTER` | CreateAcls, DeleteAcls |
| Write | TransactionalId | `WRITE` | `CLUSTER` (today) | InitProducerId (to be wired; see Gap list) |
| Describe | User SCRAM credentials | `DESCRIBE` | `CLUSTER` (to be wired) | DescribeUserScramCredentials |
| Alter | User SCRAM credentials | `ALTER` | `CLUSTER` (to be wired) | AlterUserScramCredentials |

The implicit Fluss hierarchy —
`ALL` ⊇ `{READ, WRITE, CREATE, DROP, ALTER}` each ⊇ `DESCRIBE`; `CLUSTER`
roll-up to `DATABASE` and `GROUP`; `DATABASE` roll-up to its own `TABLE`
children — means granting `WRITE on CLUSTER` is equivalent to granting
`WRITE on every TABLE` plus `WRITE on every GROUP`. That matches Kafka's
cluster-wildcard semantics and keeps the number of grants an operator
needs to write bounded.

## ACL wire-API principal mapping

Kafka's ACL wire format carries principals as `"<type>:<name>"` strings —
`"User:alice"`, `"Group:ops-team"`. Fluss's `FlussPrincipal` has
`(name, type)` as separate fields with the same types (`User` is the
default when SASL hands us a bare name; `Group` is intentionally unused on
the authenticator side but accepted by the authorizer for grants).

Translation lives in `KafkaAclsTranscoder` (symbols for both directions
under `AuthzHelper`'s module) and is a direct lexical split on the first
`:`. The inverse — rendering a `FlussPrincipal` as Kafka's principal
string for DescribeAcls responses — concatenates `type + ":" + name`.
Unknown or empty types round-trip as `"User:"` so that buggy callers
don't get a 500; they get a syntactically valid but semantically empty
principal that cannot match any grant.

The PLAIN-SASL path in
`fluss-common/.../security/auth/sasl/plain/PlainLoginModule.java` sets
`FlussPrincipal.type = "User"` on successful auth; SCRAM does the same.
That is the only principal type the Kafka bolt-on mints at authentication
time — `Group` principals exist in ACL storage but are not produced by
any Kafka-side authenticator, because Kafka's SASL mechanisms do not
surface group membership.

## Verification

The IT has two responsibilities:

1. Pin the existing wire-path gate for at least one *data-plane* API
   (Produce), not just CreateTopics.
2. Pin the full ACL admin round-trip (CreateAcls → DescribeAcls →
   DeleteAcls) and the SR-HTTP principal path — the two things that
   today have no regression test.

**File:** `fluss-kafka/src/test/java/org/apache/fluss/kafka/KafkaAclEnforcementITCase.java`.
Follows the shape of the sibling `KafkaTopicAuthzITCase`: a single-node
`FlussClusterExtension` started with `authorizer.enabled=true` and a
PLAIN-SASL JAAS config for `admin` (super-user) and `alice`
(non-super-user). Listener `KAFKA`, Kafka database `kafka`.

Scenarios:

- **alice-no-grants Produce.** alice authenticates, opens a
  kafka-clients `KafkaProducer` against the KAFKA listener, sends one
  record to a pre-created topic `kafka.alice-topic`. Asserts the send
  future completes with a `TopicAuthorizationException` and that the
  response carries `TOPIC_AUTHORIZATION_FAILED` for that topic only.
- **alice-with-grant Produce.** Admin-direct call:
  `authorizer.addAcls(adminSession, [AclBinding(User:alice, ALLOW, WRITE,
  TABLE("kafka","alice-topic"))])`. Repeat the same send; asserts it
  succeeds and the produced record is consumable by a subsequent admin
  Fetch.
- **AdminClient.createAcls round-trip.** alice (with ALTER on CLUSTER)
  creates an ACL binding via `AdminClient.createAcls(Collections.singletonList(...))`;
  asserts the returned future is non-failing and that
  `AdminClient.describeAcls(AclBindingFilter.ANY)` includes the new
  binding byte-for-byte.
- **AdminClient.deleteAcls.** Same client calls
  `AdminClient.deleteAcls(Collections.singletonList(filter))` with a
  filter matching exactly that binding; asserts the
  `FilterResult.values().size() == 1` and a second `describeAcls` no
  longer includes it.
- **SR GET /subjects with Basic + catalog grant — 200.** Starts the SR
  with `kafka.schema-registry.enabled=true`,
  `rbac.enforced=true`,
  `basic-auth-jaas-config=<jaas text with alice>`,
  `trusted-proxy-cidrs=[]`.
  Grants alice `READ` on the catalog wildcard via `CatalogService`. Sends
  `GET /subjects` with `Authorization: Basic <base64(alice:alice-secret)>`;
  asserts HTTP 200 and a JSON array body.
- **SR GET /subjects without the grant — 403.** Same config, no grant.
  Asserts HTTP 403 and a body mentioning "not granted READ".
- **callerPrincipal end-to-end.** A test-mode probe extends
  `SchemaRegistryService` with a package-private accessor that returns
  the last observed `callerPrincipal()` (or the test hooks into a debug
  log via an `ILoggingEvent` capture). After issuing the Basic-auth
  request, asserts the observed value equals `"alice"`, not the literal
  `PrincipalEntity.ANONYMOUS`.

**Grep gates.** The CI already enforces Checkstyle; in addition this
phase adds two manual greps the author should run and record in the PR
description:

```
grep -rn "callerPrincipal\|ANONYMOUS" \
     fluss-kafka/src/main/java/org/apache/fluss/kafka/sr/
```

Expected: one match at `SchemaRegistryService.java:278` (the method) and
one at `SchemaRegistryService.java:281` (the fallback). No new hardcoded
`ANONYMOUS` on any gated path.

```
grep -rn "AuthzHelper\." fluss-kafka/src/main/java \
  | awk -F: '{print $1}' | sort -u
```

Expected file set: `KafkaRequestHandler.java` and the test harness only.
No handler may reach the authorizer directly; every call goes through
`AuthzHelper` so that the `authorizer == null` no-op behaves uniformly.

## Scope and sequencing

Roughly 500 LOC total across one PR:

- SR principal classes + Bootstrap wire-up: zero LOC (already in tree).
- `handleInitProducerIdRequest` gate: ~6 LOC.
- Two SCRAM handlers' gates: ~12 LOC.
- New `KafkaAclEnforcementITCase`: ~350 LOC (seven scenarios, two
  shared setup methods, a small SR HTTP client helper).
- Minor doc updates to `0002` (SR auth) and `0006` (audit summary
  table): ~20 LOC.
- No changes to `fluss-common`, `fluss-rpc`, or `fluss-server`. Config
  options and `ResourceType.GROUP` already landed.

One PR because the IT is the thing that *fails* without the handler-side
gates; splitting the handler gates out from the IT would let CI stay
green while the real proof is absent.

## Out of scope

- **Delegation tokens.** Kafka's `CreateDelegationToken`/`Renew`/`Expire`
  APIs are not advertised, not implemented, and not gated. If Fluss ever
  grows delegation-token SASL, this doc does not cover it.
- **SASL\_SSL.** TLS wrapping for SASL is a listener-config concern, not a
  handler-authz concern; the work lives in `fluss-rpc` and is independent.
- **Kerberos / SASL\_GSSAPI.** No `GssapiLoginModule` is in the tree and
  the Kafka bolt-on does not plan to intercept Kerberos tickets.
- **TLS listener for SR HTTP.** The SR HTTP server is plain HTTP today; an
  HTTPS listener is a separate bootstrap change that would add a
  `SslContext` to `SchemaRegistryHttpServer` without touching any of the
  authz logic in this doc.
