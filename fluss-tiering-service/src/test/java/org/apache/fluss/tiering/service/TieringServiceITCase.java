/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.tiering.service;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ITCase for the standalone {@link TieringService} daemon.
 *
 * <p>This mirrors the Flink {@code PaimonTieringITCase} / {@code IcebergTieringITCase} setup
 * (cluster + lake) but drives tiering through the engine-free daemon instead of a Flink job: it
 * starts a {@link FlussClusterExtension} cluster and a real Paimon lake (filesystem catalog),
 * creates a lake-enabled log table, writes records, then boots the {@link TieringService} which
 * runs the full {@code TieringSplitGenerator -> TieringTableReader -> TableTieringCommitter} loop
 * on its own poller + worker pool. It then asserts the data landed in Paimon and that the Fluss
 * lake snapshot (bucket end-offsets) was committed back into the cluster.
 *
 * <p>The daemon resolves the Paimon {@code LakeStoragePlugin} via the SPI {@code ServiceLoader}
 * from the test classpath (so {@code pluginManager} is {@code null}), exactly as it would from the
 * {@code plugins/} directory in production.
 */
class TieringServiceITCase {

    private static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private static Configuration clientConf;
    private static String warehousePath;
    private static Connection conn;
    private static Admin admin;

    private TieringService tieringService;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        // do not clean snapshots for test purpose
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-tiering-service")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create Paimon warehouse path", e);
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);
        return conf;
    }

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @AfterEach
    void afterEach() {
        if (tieringService != null) {
            tieringService.close();
            tieringService = null;
        }
    }

    @Test
    void testTierLogTableEndToEnd() throws Exception {
        // create a lake-enabled log table
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_table_e2e");
        long tableId = createLogTable(tablePath, 1);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // write records to the table
        List<InternalRow> writtenRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows = new ArrayList<>();
            rows.add(row(i * 3, "v" + (i * 3)));
            rows.add(row(i * 3 + 1, "v" + (i * 3 + 1)));
            rows.add(row(i * 3 + 2, "v" + (i * 3 + 2)));
            writtenRows.addAll(rows);
            writeAppendRows(tablePath, rows);
        }
        long expectedEndOffset = writtenRows.size();

        // boot the standalone tiering daemon; it runs one (or more) tiering rounds autonomously.
        tieringService = newTieringService();
        tieringService.start();
        assertThat(tieringService.isRunning()).isTrue();

        // the daemon's loop generates splits, tiers them and commits the Fluss lake snapshot; once
        // committed, the leader replica advertises a non-negative lake snapshot id and the lake log
        // end offset reaches the number of written rows.
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
                    assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                            .isGreaterThanOrEqualTo(0);
                    assertThat(replica.getLakeLogEndOffset()).isEqualTo(expectedEndOffset);
                });

        // the data must have landed in the Paimon lake
        checkDataInPaimon(tablePath, writtenRows);

        // and the Fluss lake snapshot (bucket end-offsets) must have been committed
        checkFlussOffsetsInSnapshot(
                tablePath, Collections.singletonMap(tableBucket, expectedEndOffset));
    }

    private TieringService newTieringService() {
        Configuration flussConfig = new Configuration(clientConf);
        // poll fast so the test does not wait for the 30s default cadence
        flussConfig.set(TieringServiceOptions.POLL_INTERVAL, Duration.ofMillis(500L));
        Configuration dataLakeConfig = Configuration.fromMap(getPaimonCatalogConf());
        // null pluginManager -> the daemon loads the Paimon LakeStoragePlugin from the SPI on the
        // test classpath, mirroring the plugins/ directory layout used in production.
        return new TieringService(
                flussConfig,
                dataLakeConfig,
                new Configuration(),
                DataLakeFormat.PAIMON.toString(),
                null);
    }

    private long createLogTable(TablePath tablePath, int bucketNum) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private void writeAppendRows(TablePath tablePath, List<InternalRow> rows) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (InternalRow row : rows) {
                writer.append(row);
            }
            writer.flush();
        }
    }

    private static Map<String, String> getPaimonCatalogConf() {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", warehousePath);
        return paimonConf;
    }

    private static Catalog getPaimonCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(getPaimonCatalogConf())));
    }

    private void checkDataInPaimon(TablePath tablePath, List<InternalRow> expectedRows)
            throws Exception {
        try (Catalog catalog = getPaimonCatalog()) {
            FileStoreTable table =
                    (FileStoreTable)
                            catalog.getTable(
                                    Identifier.create(
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
            try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                            table.newRead().createReader(table.newReadBuilder().newScan().plan());
                    CloseableIterator<org.apache.paimon.data.InternalRow> rowIterator =
                            reader.toCloseableIterator()) {
                int count = 0;
                while (rowIterator.hasNext()) {
                    org.apache.paimon.data.InternalRow actual = rowIterator.next();
                    InternalRow expected = expectedRows.get(count);
                    assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
                    assertThat(actual.getString(1).toString())
                            .isEqualTo(expected.getString(1).toString());
                    count++;
                }
                assertThat(count).isEqualTo(expectedRows.size());
            }
        }
    }

    private void checkFlussOffsetsInSnapshot(
            TablePath tablePath, Map<TableBucket, Long> expectedOffsets) throws Exception {
        try (Catalog catalog = getPaimonCatalog()) {
            FileStoreTable table =
                    (FileStoreTable)
                            catalog.getTable(
                                    Identifier.create(
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            assertThat(snapshot).isNotNull();

            String offsetFile = snapshot.properties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
            assertThat(offsetFile).isNotNull();
            Map<TableBucket, Long> recordedOffsets =
                    new LakeTable(
                                    new LakeTable.LakeSnapshotMetadata(
                                            // snapshot id is not asserted here
                                            -1, new FsPath(offsetFile), null))
                            .getOrReadLatestTableSnapshot()
                            .getBucketLogEndOffset();
            assertThat(recordedOffsets).isEqualTo(expectedOffsets);
        }
    }
}
