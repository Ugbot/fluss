/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;

/** Integration test for Iceberg compaction. */
class IcebergRewriteITCase extends FlinkIcebergTieringTestBase {
    protected static final String DEFAULT_DB = "fluss";

    private static StreamExecutionEnvironment execEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.enableCheckpointing(1000);
    }

    @Test
    void testLogTableCompaction() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            TablePath t1 = TablePath.of(DEFAULT_DB, "log_table");
            long t1Id = createLogTable(t1, true);
            TableBucket t1Bucket = new TableBucket(t1Id, 0);

            int i = 0;
            List<InternalRow> flussRows = new ArrayList<>();
            flussRows.addAll(writeLogTableRecords(t1, t1Bucket, ++i));

            flussRows.addAll(writeLogTableRecords(t1, t1Bucket, ++i));

            flussRows.addAll(writeLogTableRecords(t1, t1Bucket, ++i));
            checkFileCountInIcebergTable(t1, 3);

            // Write should trigger compaction now since the current data file count is greater or
            // equal MIN_FILES_TO_COMPACT
            flussRows.addAll(writeLogTableRecords(t1, t1Bucket, ++i));
            // Should only have two files now, one file it for newly written, one file is for target
            // compacted file
            checkFileCountInIcebergTable(t1, 2);

            // check data in iceberg to make sure compaction won't lose data or duplicate data
            checkDataInIcebergAppendOnlyTable(t1, flussRows, 0);
        } finally {
            jobClient.cancel().get();
        }
    }

    private List<InternalRow> writeLogTableRecords(
            TablePath tablePath, TableBucket tableBucket, long expectedLogEndOffset)
            throws Exception {
        List<InternalRow> rows = Arrays.asList(row(1, "v1"));
        writeRows(tablePath, rows, true);
        assertReplicaStatus(tableBucket, expectedLogEndOffset);
        return rows;
    }
}
