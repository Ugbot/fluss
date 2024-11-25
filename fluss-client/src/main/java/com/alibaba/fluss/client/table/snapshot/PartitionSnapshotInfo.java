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

package com.alibaba.fluss.client.table.snapshot;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * A class representing the snapshot information of a partition in a table. It contains:
 * <li>The id of the table
 * <li>The id of the partition
 * <li>The snapshot information of the buckets of the partition
 *
 * @since 0.2
 */
@PublicEvolving
public class PartitionSnapshotInfo {

    private final long tableId;
    private final long partitionId;
    private final BucketsSnapshotInfo bucketsSnapshotInfo;

    public PartitionSnapshotInfo(
            long tableId, long partitionId, BucketsSnapshotInfo bucketsSnapshotInfo) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.bucketsSnapshotInfo = bucketsSnapshotInfo;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public BucketsSnapshotInfo getBucketsSnapshotInfo() {
        return bucketsSnapshotInfo;
    }

    @Override
    public String toString() {
        return "PartitionSnapshotInfo{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", snapshots="
                + bucketsSnapshotInfo
                + '}';
    }
}
