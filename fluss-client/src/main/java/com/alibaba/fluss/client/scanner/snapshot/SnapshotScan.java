/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.scanner.snapshot;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A class to describe snapshot scan for a single bucket.
 *
 * <p>It contains the files of the snapshot to scan and the row type used to deserialize the
 * records.
 *
 * @since 0.1
 */
@PublicEvolving
public class SnapshotScan {

    private final TableBucket tableBucket;

    private final List<FsPathAndFileName> fsPathAndFileNames;

    private final Schema tableSchema;

    @Nullable private final int[] projectedFields;

    public SnapshotScan(
            TableBucket tableBucket,
            List<FsPathAndFileName> fsPathAndFileNames,
            Schema tableSchema,
            @Nullable int[] projectedFields) {
        this.tableBucket = tableBucket;
        this.fsPathAndFileNames = fsPathAndFileNames;
        this.tableSchema = tableSchema;
        this.projectedFields = projectedFields;

        if (projectedFields != null) {
            for (int projectedField : projectedFields) {
                if (projectedField < 0 || projectedField >= tableSchema.getColumns().size()) {
                    throw new IllegalArgumentException(
                            "Projected field index "
                                    + projectedField
                                    + " is out of bound for schema "
                                    + tableSchema.toRowType());
                }
            }
        }
    }

    public List<FsPathAndFileName> getFsPathAndFileNames() {
        return fsPathAndFileNames;
    }

    /**
     * Gets the row type used to deserialize the records from kv snapshot. The row type is the table
     * schema before column projection if there is projection pushdown.
     */
    public Schema getTableSchema() {
        return tableSchema;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public int[] getProjectedFields() {
        return projectedFields;
    }
}
