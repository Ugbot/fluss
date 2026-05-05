# Design 0005 — Fluss Catalog + Projections

**Status:** Implemented through Phase C.2, 2026-04-23.
**Supersedes:** (none — concretizes §1 and §5 of design 0002 after the
architectural reshape.)
**Related:** `0002-kafka-schema-registry-and-typed-tables.md`,
`0004-kafka-group-coordinator.md`, and the structural plan at
`~/.claude/plans/check-which-branch-should-encapsulated-karp.md`.

## Why this exists

Phase-A1 of the Kafka Schema Registry stored its state as custom properties
on the Kafka data table. That worked for one projection but was Kafka SR-
shaped — the moment we needed a second projection (Iceberg REST Catalog,
Flink multi-format catalog) we'd have either duplicated the storage model or
grafted Iceberg concepts onto Kafka-shaped rows. Design 0002 §1 already
called this out; this doc records the eventual resolution: **one catalog,
many projections**.

The catalog is a small Polaris/Unity-style metadata service persisted as
Fluss PK tables. Every HTTP-shaped consumer of catalog state (SR today,
Iceberg REST scaffold in Phase E preview, Flink catalog bridge later) is a
projection over the same storage — never a parallel copy.

## Layering

```
┌──────────────────────────────────────────────────────────────────┐
│ Projections (HTTP / SPI)                                         │
│   • Kafka Schema Registry            (fluss-kafka/sr)            │
│   • Iceberg REST Catalog             (fluss-iceberg-rest, Ph E)  │
│   • Flink multi-format catalog       (fluss-flink, future)       │
│                                                                  │
│   Depend only on CatalogService SPI + HTTP scaffolding.          │
└──────────────────────────────────────────────────────────────────┘
                              ▲
┌──────────────────────────────────────────────────────────────────┐
│ CatalogService SPI    (fluss-catalog)                            │
│   namespaces / tables / schema history /                         │
│   Kafka subject bindings / principals / grants /                 │
│   checkPrivilege                                                 │
│                                                                  │
│   FlussCatalogService implementation reads + writes 7 PK tables  │
│   in _catalog via the public Fluss client. Lazy bootstrap;       │
│   lazy-per-handle table creation; read-after-write consistent    │
│   via PK lookup chains (scan avoided on hot paths).              │
└──────────────────────────────────────────────────────────────────┘
                              ▲
┌──────────────────────────────────────────────────────────────────┐
│ Fluss core                                                       │
│   Public API consumed: Connection, Admin, Table, Lookuper,       │
│   UpsertWriter, CoordinatorLeaderBootstrap SPI.                  │
│   No knowledge of catalog / projections.                         │
└──────────────────────────────────────────────────────────────────┘
```

Direction of dependencies: downward only. Fluss core is the API surface
everything above it consumes. The catalog module has no references back into
Kafka, and the core has no references into the catalog.

## Entity shape (seven tables, `_catalog` database)

| Table | PK | Purpose | Written by |
|---|---|---|---|
| `_namespaces` | `namespace_id UUID` | Namespace hierarchy (parent_id chain). | `createNamespace` |
| `_tables` | `table_id UUID` | Multi-format table records: `(format, backing_ref, current_schema_id)`. | `createTable` + `registerSchema` (pointer update) |
| `_schemas` | `schema_id UUID` | Append-only schema history per table; each row carries the deterministic SR schema id. | `registerSchema` |
| `_principals` | `principal_id UUID` | Principal identity; resolved by name on every call. | `ensurePrincipal` |
| `_grants` | `grant_id UUID` | Principal × kind × entity-id × privilege. CATALOG-wildcard grants apply to any specific entity. | `grant` |
| `_kafka_bindings` | `subject STRING` | Kafka-shaped subject → table_id mapping (keeps Kafka vocabulary out of the core entities). | SR projection's `bindKafkaSubject` |
| `_id_reservations` | `sr_id INT` | Deterministic-hash + probe id allocator for Kafka SR compat. | `registerSchema` |

All tables use 16 buckets. PK names are deliberately `_single_underscore` —
the `__` prefix is reserved by Fluss core.

## Consistency model

The catalog talks to Fluss through the same `Connection` / `Table` /
`UpsertWriter` / `Lookuper` APIs an application would. Writes are committed
individually; reads use PK lookups on the hot path because batch scans over
PK tables trail writes by a noticeable window.

### Idempotent re-registration

Writers can re-submit the same subject+schema text and expect the same id.
The SR does this by walking a PK chain:

```
subject        → _kafka_bindings.tableId
tableId        → _tables.currentSchemaId
currentSchemaId → _schemas.schemaText
```

If the latest schema matches the submission, the SR returns the existing
SR schema id without appending. Every hop is a PK lookup — read-after-write
consistent.

### Deterministic SR schema ids

`id = hash(tableId, schemaVersion, format) & 0x7fffffff`. Collisions (rare
with 31-bit space) linear-probe forward, up to eight slots, then fail. The
`_id_reservations` row is the authoritative mapping from id to schema_id.

## Bootstrap / lifecycle

A new SPI `CoordinatorLeaderBootstrap` lives in fluss-server. Implementations
live in whatever module they own. `CoordinatorServer.initCoordinatorLeader`
loads all implementations via `ServiceLoader`, sorts by `priority()`, and
starts each with `(Configuration, ZooKeeperClient, MetadataManager,
ServerMetadataCache, List<Endpoint>)`.

Priorities today:
- `FlussCatalogBootstrap` — 10 (first, so later projections find the service).
- `SchemaRegistryBootstrap` — 100.
- `IcebergRestBootstrap` — 100 (sibling projection; either order works).

The catalog bootstrap is lazy: it registers a `FlussCatalogService` whose
Fluss `Connection` opens on first use. Eager connection from inside
`initCoordinatorLeader` hits `NotCoordinatorLeaderException` because the
coordinator hasn't yet announced leadership to its own RPC endpoint.

System tables are created lazily per handle (`table(Handle)` on first
touch), not eagerly — creating all seven in sequence at bootstrap tripped a
replica-schema propagation race. Each fresh creation is followed by a
`waitUntilReady` poll.

## RBAC (Phase C.2)

- Principals are materialised on first `grant` or `ensurePrincipal` call.
  `PrincipalEntity.ANONYMOUS` is the default for unauthenticated requests.
- Grants are idempotent on the `(principal, kind, entity_id, privilege)`
  tuple. Revoke is a row delete.
- `checkPrivilege(principal, kind, id, priv)` matches direct grants or
  catalog-wide wildcards (`kind=CATALOG`, `entity_id=*`).
- Projections enforce RBAC at their own entry points. The Kafka SR has a
  single `authorize(WRITE|READ)` call per endpoint gated by
  `kafka.schema-registry.rbac.enforced` (default false). Flipping the flag on
  without a principal-extraction path locks the SR down — by design; we want
  SASL/header-principal wired before real enforcement.

## Why a second projection matters for this design

The Iceberg REST Catalog scaffold (Phase E preview) exists not because
anyone is asking for it yet, but because a second consumer of
`CatalogService` with a completely different wire shape is the best way to
validate that the catalog entities aren't accidentally Kafka-shaped. If
Iceberg REST's GET `/v1/namespaces` needs a field the Kafka SR didn't, the
catalog schema absorbs it; if it can be served from what's already there,
we've proved the shape holds.

## Out of scope / follow-ups

- Credential vending (Polaris "trusting the catalog to mint short-lived
  cloud credentials"). No storage slot reserved — add a `_credentials` table
  when we need it.
- Multi-level namespace hierarchy beyond two levels. Currently supported by
  `parent_id` chaining but untested past a single level.
- `_schemas.version` rollback / deprecation. Append-only today; no delete
  path.
- Cross-catalog federation / read-through. One catalog per Fluss cluster.
- Audit log / lineage entity tables.
