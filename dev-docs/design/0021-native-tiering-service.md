# Standalone Native Tiering Service — Implementation Spec

Phase 5 of `dev-docs/design/0020-execution-order-and-roadmap.md`: a standalone, no-Flink tiering daemon that drives the existing `LakeTieringFactory` / `LakeWriter` / `LakeCommitter` SPI over the existing `CoordinatorGateway` RPC (`lakeTieringHeartbeat` / `prepareLakeTableSnapshot` / `commitLakeTableSnapshot`), with **all lakes behind the SPI — no format branching in the core**, Iceberg writing through `iceberg-aws` `S3FileIO` (AWS SDK v2, no Hadoop), and a read-through/write-through catalog built on the already-present `fluss-catalog` + `fluss-iceberg-rest` modules.

## 0. Key finding that makes this cheap

The entire tiering **data plane and commit protocol are already engine-agnostic**. Flink only supplies four pieces of scaffolding:

| Flink concept | What it actually does in tiering | Fluss-native replacement |
|---|---|---|
| `SplitEnumerator` (`TieringSourceEnumerator`) | Runs the heartbeat loop, requests a table, generates splits, tracks epochs, force-finish timer | A plain daemon loop on a `ScheduledExecutorService` |
| `SourceReader`/`SplitReader` (`TieringSplitReader`) | Reads Fluss log/snapshot via the **Fluss client** `LogScanner`/`BatchScanner` and writes via `LakeWriter` | Reuse the class almost verbatim (see §3) |
| `OneInputStreamOperator` (`TieringCommitOperator`) | Aggregates per-bucket `WriteResult`s, calls `LakeCommitter.commit`, then commits to Fluss | Extract its body to a plain `TableTieringCommitter` |
| `OperatorEventGateway` / `SourceEvent` | Reader→enumerator messaging (finished/failed/maxDuration) | In-process method calls / a queue |

Concretely, `TieringSplitReader` (`fluss-flink/fluss-flink-common/.../tiering/source/TieringSplitReader.java`) already takes a `org.apache.fluss.client.Connection`, calls `connection.getTable(...)`, `table.newScan().createLogScanner()`, `logScanner.subscribe(...)`, `logScanner.poll(...)` / `((LogScannerImpl) logScanner).pollRecordBatch(...)`, and `lakeTieringFactory.createLakeWriter(...)`. Its **only** Flink dependencies are the `SplitReader` / `RecordsWithSplitIds` / `SplitsChange` interfaces. `TieringCommitOperator` (`.../tiering/committer/TieringCommitOperator.java`) is the same: its real work is in `commitWriteResults(...)` and uses only `Connection`, `Admin`, `LakeCommitter`, and `FlussTableLakeSnapshotCommitter` (which is itself pure RPC, no Flink — see `.../tiering/committer/FlussTableLakeSnapshotCommitter.java`).

So Phase 5 is mostly an **extraction + a daemon main loop**, not a rewrite.

---

## 1. New module layout

Two new Maven modules, registered in the root `pom.xml` `<modules>` after `fluss-lake` and before `fluss-dist`.

### 1.1 `fluss-lake-tiering-core` (engine-agnostic library)

Path: `fluss-lake-tiering-core/`. Depends on: `fluss-common` (provided/compile), `fluss-client`, `fluss-rpc`, `fluss-metrics`. **Must NOT depend on** `fluss-server`, `org.apache.flink:*`, or any `fluss-lake-*` impl (lakes are loaded via SPI / `PluginManager` at runtime). Add a `maven-enforcer` `bannedDependencies` rule banning `org.apache.flink` and `org.apache.hadoop`.

```
fluss-lake-tiering-core/
  pom.xml
  src/main/java/org/apache/fluss/lake/tiering/
    coordinator/
      TieringCoordinatorClient.java        # wraps CoordinatorGateway heartbeat RPC (extracted from TieringSourceEnumerator.HeartBeatHelper)
      HeartbeatRequests.java               # static builders (basic/withRequestTable/finished/failed) — extracted verbatim
      TieringTableAssignment.java          # value: tableId, tieringEpoch, TablePath (replaces Tuple3)
      CoordinatorEpoch.java                # holds flussCoordinatorEpoch
    split/
      TieringSplit.java                    # MOVED from flink-common (see §2)
      TieringLogSplit.java                 # MOVED
      TieringSnapshotSplit.java            # MOVED
      TieringSplitGenerator.java           # MOVED (already pure: Admin + client only)
    reader/
      TieringTableReader.java              # extracted core of TieringSplitReader (no Flink SplitReader iface)
      BucketWriteResult.java               # renamed/MOVED TableBucketWriteResult (drop Serializable-for-Flink note)
      WriterInitContextImpl.java           # MOVED TieringWriterInitContext
    committer/
      TableTieringCommitter.java           # extracted core of TieringCommitOperator.commitWriteResults
      CommitterInitContextImpl.java        # MOVED TieringCommitterInitContext
      FlussTableLakeSnapshotCommitter.java # MOVED verbatim (already pure RPC)
    metrics/
      TieringMetricNames.java              # constants
    LakeTieringFactoryLoader.java          # wraps LakeStoragePluginSetUp.fromDataLakeFormat
```

### 1.2 `fluss-tiering-service` (the daemon)

Path: `fluss-tiering-service/`. Depends on: `fluss-lake-tiering-core`, `fluss-client`, `fluss-metrics`, and (runtime/optional, via `provided` + plugin dir) `fluss-lake-iceberg`. **No** `fluss-server`, **no** Flink.

```
fluss-tiering-service/
  pom.xml
  src/main/java/org/apache/fluss/tiering/service/
    TieringService.java                    # main loop daemon (see §4)
    TieringServiceOptions.java             # ConfigOptions for the daemon
    TieringWorkerPool.java                 # bounded executor; one table tiered at a time per worker
    TieringServiceMain.java                # public static void main(String[]) entrypoint
  src/main/resources/
    META-INF/NOTICE
  src/test/java/org/apache/fluss/tiering/service/
    TieringServiceITCase.java
```

### 1.3 `fluss-dist` additions

- `fluss-dist/src/main/resources/bin/tiering-service.sh` — start/stop wrapper mirroring `coordinator-server.sh`, delegating to `fluss-daemon.sh` with main class `org.apache.fluss.tiering.service.TieringServiceMain`.
- `fluss-dist/src/main/resources/conf/server.yaml` — document new `tiering.*` keys.
- Add `fluss-tiering-service` and `fluss-lake-tiering-core` jars to the dist assembly; lake impl jars go under `plugins/` so they are loaded by `PluginManager` (mirrors how `LakeStoragePluginSetUp.getAllLakeStoragePlugins` already concats `pluginManager.load(...)`).

---

## 2. Classes to MOVE (engine-agnostic already — relocate verbatim, repackage)

These have **zero Flink imports** today and only need their package changed from `org.apache.fluss.flink.tiering.*` to `org.apache.fluss.lake.tiering.*` (and callers updated). Move from `fluss-flink/fluss-flink-common/src/main/java/...`:

| Source file | New home |
|---|---|
| `tiering/source/split/TieringSplit.java` | `lake/tiering/split/TieringSplit.java` |
| `tiering/source/split/TieringLogSplit.java` | `lake/tiering/split/TieringLogSplit.java` |
| `tiering/source/split/TieringSnapshotSplit.java` | `lake/tiering/split/TieringSnapshotSplit.java` |
| `tiering/source/split/TieringSplitGenerator.java` | `lake/tiering/split/TieringSplitGenerator.java` |
| `tiering/source/TableBucketWriteResult.java` | `lake/tiering/reader/BucketWriteResult.java` |
| `tiering/source/TieringWriterInitContext.java` | `lake/tiering/reader/WriterInitContextImpl.java` |
| `tiering/committer/TieringCommitterInitContext.java` | `lake/tiering/committer/CommitterInitContextImpl.java` |
| `tiering/committer/FlussTableLakeSnapshotCommitter.java` | `lake/tiering/committer/FlussTableLakeSnapshotCommitter.java` |

`TieringSplitGenerator` is the strongest proof of portability: it already takes only `org.apache.fluss.client.admin.Admin` and uses `BucketOffsetsRetrieverImpl`, `getLatestLakeSnapshot`, `getLatestKvSnapshots`, `listPartitionInfos` — all client APIs. Drop its one `org.apache.flink.util.FlinkRuntimeException` for `org.apache.fluss.exception.FlussRuntimeException`.

**Keeping the Flink connector working:** to avoid deleting Flink tiering yet (roadmap: "made optional, not deleted"), have `fluss-flink-common` depend on `fluss-lake-tiering-core` and replace its moved classes with re-imports, OR keep thin Flink subclasses. Simplest: `fluss-flink-common` adds a dependency on `fluss-lake-tiering-core`; the Flink `TieringSplitReader`/`TieringCommitOperator` become **thin adapters** that delegate to the extracted core classes (§3, §5).

---

## 3. Extract the reader core: `TieringTableReader`

Source of truth: `TieringSplitReader.java` (full logic already read). Create `lake/tiering/reader/TieringTableReader.java` containing **all** the existing private logic, but with the Flink `SplitReader<...>` interface and `RecordsWithSplitIds` removed.

Signature changes:
- Constructor: `TieringTableReader(Connection connection, LakeTieringFactory<WriteResult,?> factory, ClassLoader userClassLoader, Duration pollTimeout, TieringMetrics metrics)` — identical to the existing `protected` ctor.
- Replace `fetch(): RecordsWithSplitIds<...>` with `List<BucketWriteResult<WriteResult>> tierTable(TablePath, long tableId, List<TieringSplit> splits)` that drives a table to completion: it reuses the existing `handleSplitsChanges` body (renamed `addSplits`), then loops calling the existing `fetch()` body (renamed `pollOnce()`) until `currentTableSplitsByBucket` is empty, accumulating finished `BucketWriteResult`s.
- `RecordsWithSplitIds`/`TableBucketWriteResultWithSplitIds` inner class is deleted; the methods that returned it now return `List<BucketWriteResult<WriteResult>>` (the finished entries) directly. The `nextSplit()`/`nextRecordFromSplit()` iteration protocol is a Flink artifact and is dropped.
- `wakeUp()` / `close()` kept; `handleTableReachTieringMaxDuration(tableId)` kept (now called directly by the daemon's force-finish timer, not via `SourceEvent`).

**Everything else is reused unchanged**, including:
- The Arrow batch fast-path: `useRecordBatchPath()`, `handleArrowBatchRecords(...)`, `((LogScannerImpl) currentLogScanner).pollRecordBatch(pollTimeout)`, `ArrowScanRecords`, `SupportsRecordBatchWrite`, `ArrowRecordBatch`, `ArrowBatchData.truncateAndTransferOwnership(...)`. This is exactly the Kafka→Arrow-log→lake path the roadmap ITCases exercise.
- The row path: `handleLogRecords(...)`, `LogScanner.poll` → `ScanRecords`.
- Snapshot (PK) path: `BoundedSplitReader` over `table.newScan().createBatchScanner(bucket, snapshotId)`.
- Offset/timestamp bookkeeping (`processLogRecords`, `LogOffsetAndTimestamp`, `consumedUpToOffset` capping at `stoppingOffset`).

> Note: `BoundedSplitReader` and `RecordAndPos` currently live in `fluss-flink` (`flink/source/reader/`). They wrap a `BatchScanner` and are not Flink-coupled in substance, but they import nothing from the operator runtime. Move `BoundedSplitReader` + `RecordAndPos` into `fluss-lake-tiering-core` (`lake/tiering/reader/`) as well, or inline the trivial `BatchScanner` drain loop. Verify with `grep -n 'import org.apache.flink'` on both before moving.

How the daemon reads Arrow log via the client `LogScanner` (the load-bearing call sequence, unchanged from `TieringSplitReader`):
```
Table table = connection.getTable(tablePath);
LogScanner scanner = table.newScan().createLogScanner();
scanner.subscribe(partitionId, bucket, startingOffset);   // or subscribe(bucket, startingOffset)
// arrow append-only fast path:
ArrowScanRecords recs = ((LogScannerImpl) scanner).pollRecordBatch(pollTimeout);
for (ArrowBatchData batch : recs.records(bucket)) { batchWriter.write(new ArrowRecordBatch(batch)); }
// generic path:
ScanRecords recs = scanner.poll(pollTimeout);
for (ScanRecord r : recs.records(bucket)) { lakeWriter.write(r); }
scanner.unsubscribe(partitionId, bucket);   // on split completion
```

---

## 4. The daemon main loop: `TieringService`

Replaces `TieringSourceEnumerator` (the heartbeat/epoch/scheduling brain) + `TieringSourceReader` (the connection owner). Single process, owns one `Connection`, one `RpcClient`/`CoordinatorGateway`, one `Admin`, a `TieringSplitGenerator`, and a `TieringWorkerPool`.

Reuse directly from `TieringSourceEnumerator.java`:
- `start()`: create `Connection` (`ConnectionFactory.createConnection(flussConf)`), `Admin`, `RpcClient.create(...)`, `GatewayClientProxy.createGatewayProxy(metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class)`, then the **registration handshake**: `coordinatorGateway.lakeTieringHeartbeat(basicHeartBeat()).get()` to obtain `flussCoordinatorEpoch`.
- The heartbeat request builders in `HeartBeatHelper` → moved to `coordinator/HeartbeatRequests.java` verbatim (basic, `heartBeatWithRequestNewTieringTable`, `tieringTableHeartBeat`, `failedTableHeartBeat`, force-finished list, `PbLakeTieringStats` population).

Main loop (replaces `context.callAsync(this::requestTieringTableSplitsViaHeartBeat, ..., pollTieringTableIntervalMs)`):

```
ScheduledExecutorService poller = single-thread, period = tiering.poll-interval (default from POLL_TIERING_TABLE_INTERVAL)
each tick:
  1. drain pending finished/failed tables -> build heartbeat (HeartbeatRequests.tieringTableHeartBeat)
  2. if a worker is free: set request_table=true
  3. resp = coordinatorGateway.lakeTieringHeartbeat(req).get(3 min)
     - update flussCoordinatorEpoch from resp
  4. if resp.hasTieringTable():
        TieringTableAssignment a = from resp.getTieringTable() (tableId, tieringEpoch, TablePath)
        submit to TieringWorkerPool:
           TableInfo info = admin.getTableInfo(path).get()
           List<TieringSplit> splits = splitGenerator.generateTableSplits(info)  // may be empty
           if empty -> mark finished(tableId, epoch) (matches enumerator's empty-splits short-circuit)
           else:
             start a force-finish timer: schedule handleReachMaxDuration after info.getTableConfig().getDataLakeFreshness()
             List<BucketWriteResult> results = tableReader.tierTable(path, tableId, splits)
             TableTieringCommitter.commit(tableId, path, results)   // §5
             on success -> finishedTables.put(tableId, FinishInfo(epoch, isForceFinished, stats))
             on failure -> failedTableEpochs.put(tableId, epoch)
  5. periodically (or every tick during a long tier) send heartbeat for in-flight tieringTableEpochs
     so the coordinator's renewTieringHeartbeat keeps the table out of TIERING_SERVICE_TIMEOUT_MS (2 min).
```

Critical coordination invariants reused from the server side (`LakeTableTieringManager.java`):
- **Epoch fencing**: every per-table request carries `(tableId, coordinatorEpoch, tieringEpoch)`. `validateTieringServiceRequest` throws `FencedTieringEpochException` on mismatch — the daemon must propagate the assigned `tieringEpoch` through to `finished`/`failed`/heartbeat for that table (exactly as `tieringTableEpochs` map does in the enumerator).
- **Heartbeat liveness**: `renewTieringHeartbeat` requires the table be in `Tiering` state and refreshes `liveTieringTableIds`. The daemon MUST keep heart-beating in-flight tables; otherwise `checkTieringServiceTimeout` (every 15s, 2-min timeout) flips it back to `Pending`. So the poll interval must be well under 2 minutes (keep the existing default) and long tiers must heartbeat mid-flight.
- **Force-finish**: when the freshness timer fires, call `tableReader.handleTableReachTieringMaxDuration(tableId)` (which sets the reader to force-complete whatever is tiered so far) and mark the eventual finish as `isForceFinished=true` → coordinator transitions `Tiered → Pending` (immediate re-tier) instead of `Tiered → Scheduled`. This mirrors `LakeTableTieringManager.finishTableTiering(..., isForceFinished, ...)`.
- **One table at a time** by default (the enumerator comment explicitly serializes table requests). `TieringWorkerPool` defaults to 1 concurrent table but is configurable; each worker requests its own table via its own `request_table=true` heartbeat, preserving the single-table-per-request property.

Shutdown (`close()`): reuse the enumerator's logic — move all `tieringTableEpochs` to `failedTableEpochs` and send one final `failedTableHeartBeat` so the coordinator re-queues them promptly, then close `Admin`, `Connection`, `RpcClient`.

---

## 5. Extract the committer core: `TableTieringCommitter`

Source of truth: `TieringCommitOperator.commitWriteResults(...)` (already read). Create `lake/tiering/committer/TableTieringCommitter.java`. It is constructed with `(Configuration flussConf, Configuration lakeTieringConfig, LakeTieringFactory factory, Admin admin, FlussTableLakeSnapshotCommitter flussCommitter)` and exposes:

```
CommitResult commit(long tableId, TablePath path, List<BucketWriteResult<WriteResult>> results)
```

Body is **lifted verbatim** from `commitWriteResults`:
1. Filter `results` to those with non-null `writeResult()` (empty → no-op `CommitResult(null,null)`).
2. Guard against drop/recreate: `admin.getTableInfo(path).get()`; fail if `tableId` changed.
3. `try (LakeCommitter c = factory.createLakeCommitter(new CommitterInitContextImpl(path, info, lakeTieringConfig, flussConf)))`:
   - build `logEndOffsets` / `logMaxTieredTimestamps` maps from `BucketWriteResult`,
   - `Committable committable = c.toCommittable(writeResults)`,
   - **missing-snapshot safety check**: `getLatestLakeSnapshot(path)` + `checkFlussNotMissingLakeSnapshot(...)` (lift verbatim — handles the case where lake has a Fluss-committed snapshot Fluss doesn't know about; aborts the committable and throws),
   - two-phase Fluss commit: `flussCommitter.prepareLakeSnapshot(tableId, path, logEndOffsets)` → file path → `snapshotProperties = singletonMap(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, path)` → `LakeCommitResult r = c.commit(committable, snapshotProperties)` → `flussCommitter.commit(tableId, path, r, offsetsFile, logEndOffsets, logMaxTieredTimestamps)`,
   - return `CommitResult(committable, r.getTieringStats())`.

`FlussTableLakeSnapshotCommitter` (moved verbatim) already performs the prepare/commit RPCs against `CoordinatorGateway.prepareLakeTableSnapshot` / `commitLakeTableSnapshot` and handles the readable-vs-tiered snapshot fan-out for Paimon DV (`LakeCommitResult.committedIsReadable()` / `getReadableSnapshot()`), and the `KEEP_LATEST` / `KEEP_ALL_PREVIOUS` retention semantics. No changes.

The `stats` returned flow into the next heartbeat's `finished_tables` `PbLakeTieringStats` (file size / record count), which `LakeTableTieringManager.updateTableTieringResult` records for the per-table metrics.

---

## 6. Lakes stay behind the SPI — no format branching

The daemon never names a lake format in code. Loading path (reuse existing machinery):

- `LakeTieringFactoryLoader.load(dataLakeFormat, pluginManager)` → `LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat, pluginManager)` (already implemented in `fluss-common/.../lake/lakestorage/LakeStoragePluginSetUp.java`; SPI via `ServiceLoader<LakeStoragePlugin>` concatenated with `PluginManager.load`). Then `plugin.createLakeStorage(dataLakeConfig).createLakeTieringFactory()` → a `LakeTieringFactory<WriteResult,Committable>`.
- The daemon, `TieringTableReader`, and `TableTieringCommitter` are all generic over `<WriteResult, Committable>` and touch only the SPI interfaces in `fluss-common/.../lake/{writer,committer,batch,serializer}`: `LakeWriter.write/complete`, `SupportsRecordBatchWrite.write(ArrowRecordBatch)`, `LakeCommitter.toCommittable/commit/abort/getMissingLakeSnapshot`, `WriterInitContext`, `CommitterInitContext`.
- The **one** existing format check in `TieringSplitReader.useRecordBatchPath()` currently hard-codes `DataLakeFormat.PAIMON` and `LogFormat.ARROW`. This is a leak. **Replace it with a capability probe**: use the Arrow batch path whenever `unshadedArrowAvailable && !tableInfo.hasPrimaryKey() && logFormat == ARROW && lakeWriter instanceof SupportsRecordBatchWrite`. Decide by `instanceof SupportsRecordBatchWrite` on the writer the factory returns, not by `DataLakeFormat`. This makes the Arrow fast-path available to any lake whose `LakeWriter` implements `SupportsRecordBatchWrite` (Iceberg can opt in) and removes the only format branch. (The factory/writer are created lazily, so probe on first `getOrCreateLakeWriter`.)

Adding a lake (e.g. Delta in a later step) = drop a `fluss-lake-delta` jar implementing `LakeStoragePlugin`/`LakeStorage`/`LakeTieringFactory` into `plugins/`. Zero daemon changes.

---

## 7. Iceberg via `iceberg-aws` `S3FileIO` (AWS SDK v2, no Hadoop)

Today `fluss-lake-iceberg` builds its catalog through `IcebergCatalogUtils.createIcebergCatalog` → `org.apache.iceberg.CatalogUtil.buildIcebergCatalog(name, props, IcebergConfiguration.from(conf).get())`, where `IcebergConfiguration` reflectively loads a Hadoop `Configuration` (`fluss-lake-iceberg/.../conf/IcebergConfiguration.java`, `HadoopUtils.java`, `HadoopConfSerde.java`) and the pom pulls `hadoop-common`/`hadoop-hdfs-client` (`provided`). For the standalone service we want **no Hadoop on the classpath**.

Plan (within `fluss-lake-iceberg`, gated so the Flink path is unaffected):
1. Add an `iceberg-aws` dependency (+ AWS SDK v2 BOM: `software.amazon.awssdk:s3`, `:sts`, `:auth`, `:apache-client` or `url-connection-client`) to `fluss-lake-iceberg/pom.xml`. Keep Hadoop deps `provided` and `optional` so they are absent from the tiering-service runtime classpath.
2. Make catalog construction Hadoop-optional: `CatalogUtil.buildIcebergCatalog` accepts a `null` Hadoop conf for catalogs that don't need it. `IcebergConfiguration.from(...)` already returns `null` when Hadoop classes are absent (its Javadoc says so). So with Hadoop off the classpath, `IcebergConfiguration.get()` returns `null` and `buildIcebergCatalog` proceeds Hadoop-free. Verify `IcebergConfiguration.loadHadoopConfig` swallows `ClassNotFoundException` (it currently reflectively loads `org.apache.hadoop.conf.Configuration`) — if it throws, wrap in a try/catch returning null.
3. Configure `FileIO` explicitly via catalog properties instead of relying on Hadoop FS: set
   - `io-impl = org.apache.iceberg.aws.s3.S3FileIO`
   - `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key` / or default AWS credential chain, `s3.path-style-access`, `client.region`.
   These come from the daemon's `dataLakeConfig` (passed straight into `createIcebergCatalog`'s `props` map — `IcebergCatalogUtils` already forwards `configuration.toMap()`). For a `RESTCatalog` (recommended, see §8) the warehouse/`FileIO` config is negotiated with the REST server, so `S3FileIO` is selected by the same `io-impl` property.
4. Add a `maven-enforcer` `bannedDependencies` rule for `org.apache.hadoop:*` to `fluss-tiering-service/pom.xml` and to `fluss-lake-tiering-core/pom.xml` so a transitive Hadoop pull fails the build (roadmap exit criterion: `dependency:tree` shows no Hadoop/Flink in the tiering path).

The Iceberg writer/committer themselves (`IcebergLakeWriter`, `IcebergLakeCommitter`, `RecordWriter`, `TaskWriterFactory`, `AppendOnlyTaskWriter`, `DeltaTaskWriter`) need **no change** — they operate on `org.apache.iceberg.Table` obtained from `catalog.loadTable(...)`; the `FileIO` is whatever the catalog injected. Confirmed: `IcebergLakeWriter` only references `Catalog`/`Table`/`TaskWriter`, never a filesystem directly.

---

## 8. Read-through / write-through catalog via `fluss-catalog` + `fluss-iceberg-rest`

These two modules already exist on this branch and slot in exactly as the roadmap intends:

- `fluss-catalog` (`CatalogService` interface + `FlussCatalogService` impl, in `fluss-catalog/.../catalog/`) is a catalog **backed by Fluss PK tables** in a reserved `_catalog` database (namespaces, tables, schemas, grants, quotas, Kafka txn state). It is the metadata system-of-record for the **hot tier** (Fluss). `FlussCatalogService` reads via `Lookuper.lookup` / `BatchScanner` and writes via `UpsertWriter.upsert` — i.e. it is already a read/write catalog over Fluss.

- `fluss-iceberg-rest` (`IcebergRestHttpServer` + `IcebergRestHttpHandler`, in `fluss-iceberg-rest/.../iceberg/rest/`) is an **Iceberg REST Catalog HTTP server** that delegates to a `CatalogService`. Today it serves `GET /v1/config`, `GET/POST /v1/namespaces`. This is the surface that exposes Fluss tables to any Iceberg reader (Spark/Trino/DuckDB/pyiceberg) and is the natural front door for the **write-through** path.

How they wire the read-through/write-through model for tiering:

1. **Write-through (cold tier):** The tiering service writes Iceberg data/metadata through the Iceberg SPI (`IcebergLakeCommitter.commit` calls `Transaction.commit`/`appendFiles`), producing real Iceberg snapshots. The committed snapshot id + bucket offsets are recorded back into Fluss via `commitLakeTableSnapshot` (already happens). So Iceberg becomes the durable record of cold data; Fluss holds the hot tail + the offset watermark linking the two.
2. **Read-through (serving):** `IcebergRestHttpHandler` (backed by `FlussCatalogService`, or by the same Iceberg catalog the committer uses) lets external Iceberg engines load the table and read committed snapshots directly from object storage via `S3FileIO`. For the freshest data, the existing `LakeSource`/`IcebergLakeSource` union-read path (Fluss log tail + lake snapshot) is what Flink/Spark connectors already use; the REST catalog advertises the Iceberg side.
3. **Single source of truth handshake:** `fluss-iceberg-rest`'s REST `loadTable` should resolve the **same** Iceberg table the tiering committer wrote (same warehouse + `S3FileIO` config), so a reader hitting the REST catalog and a tier job writing through the SPI agree on snapshots. Extend `IcebergRestHttpHandler` with `GET /v1/namespaces/{ns}/tables` and `GET .../tables/{tbl}` (load-table) returning the Iceberg `TableMetadata` location — these are the endpoints external engines require beyond the current config/namespaces stubs.

Both modules currently expose a `CoordinatorLeaderBootstrap` SPI (their `META-INF/services` files target `org.apache.fluss.server.coordinator.spi.CoordinatorLeaderBootstrap`) so they boot **inside the CoordinatorServer**, not inside the tiering daemon. That is the correct split: the catalog/REST surface is a server-side concern (serving + metadata), and the tiering daemon is a separate process that only writes lake data and commits offsets via RPC. The daemon does **not** depend on `fluss-catalog`/`fluss-iceberg-rest`; it shares the lake (object storage + Iceberg catalog) with them.

---

## 9. Config (`TieringServiceOptions`)

New `ConfigOption`s (follow `ConfigBuilder` patterns in `fluss-common/.../config/ConfigBuilder.java`):
- `tiering.poll-interval` (Duration, default = existing `POLL_TIERING_TABLE_INTERVAL`) — heartbeat/table-request cadence; must stay < `LakeTableTieringManager.TIERING_SERVICE_TIMEOUT_MS` (2 min).
- `tiering.max-concurrent-tables` (int, default 1) — `TieringWorkerPool` size.
- `tiering.poll-timeout` (Duration, default `TieringSplitReader.DEFAULT_POLL_TIMEOUT` = 10s) — `LogScanner.poll` timeout.
- `datalake.format` (reuse existing cluster option) — selects the `LakeStoragePlugin`.
- Plus the existing Fluss client bootstrap config (`bootstrap.servers`, etc.) and the lake/`dataLakeConfig` block (S3 endpoint/region/credentials, Iceberg catalog props) passed verbatim to `createLakeStorage`/`createIcebergCatalog`.

---

## 10. Tests / exit criteria

1. **Reuse the existing tiering ITCases** that the roadmap names as the gate: `fluss-lake/fluss-lake-iceberg/src/test/java/.../tiering/IcebergTieringITCase.java` and the Paimon equivalent, plus the new Kafka→Arrow-log→{Iceberg,Paimon} ITCases (commits `70a255cb`, `b9c6b552`). Add a variant that drives them through `TieringService` instead of the Flink job (inject a `FlussClusterExtension`, start a `TieringService` against it, assert lake snapshots + Fluss `getLatestLakeSnapshot` offsets match). The data-plane assertions are unchanged because the reader/committer cores are the same code.
2. `fluss-tiering-service/.../TieringServiceITCase.java`: end-to-end with the in-process cluster — create a datalake-enabled table, produce Arrow log, run the daemon one round, assert committed Iceberg snapshot via `S3FileIO` against a MinIO/S3Mock container (podman), and assert epoch fencing + force-finish behavior against `LakeTableTieringManager`.
3. `dependency:tree` enforced clean of `org.apache.hadoop` and `org.apache.flink` in `fluss-lake-tiering-core` and `fluss-tiering-service` (maven-enforcer `bannedDependencies`).
4. AssertJ only; JUnit5; `*ITCase` naming; `spotless:apply`; Java 8 source level (no `var`, no `List.of`, etc. — the moved Flink code already complies).

---

## 11. Summary of reuse vs new

**Reuse verbatim (move + repackage):** `TieringSplit*`, `TieringSplitGenerator`, `TableBucketWriteResult`, `TieringWriterInitContext`, `TieringCommitterInitContext`, `FlussTableLakeSnapshotCommitter`, `HeartBeatHelper` builders, all of `fluss-common/.../lake/*` SPI, all of `fluss-lake-iceberg` writer/committer internals.

**Extract (lift method bodies, drop Flink interface):** `TieringSplitReader` → `TieringTableReader`; `TieringCommitOperator.commitWriteResults` → `TableTieringCommitter`.

**New:** `TieringService` (daemon loop replacing `TieringSourceEnumerator` + `TieringSourceReader`), `TieringWorkerPool`, `TieringServiceMain`, `TieringServiceOptions`, `tiering-service.sh`, the two new modules' poms, the `useRecordBatchPath` de-branching, the `S3FileIO`/no-Hadoop Iceberg wiring, and the extra REST load-table endpoints in `fluss-iceberg-rest`.

**Unchanged server side:** `LakeTableTieringManager`, `CoordinatorService.lakeTieringHeartbeat`, the `prepareLakeTableSnapshot`/`commitLakeTableSnapshot` RPCs, and the `FlussApi.proto` messages — the daemon speaks the existing protocol exactly as the Flink enumerator does.
