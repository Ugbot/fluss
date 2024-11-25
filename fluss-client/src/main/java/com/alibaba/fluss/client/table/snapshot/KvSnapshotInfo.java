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

import java.util.Map;

/**
 * A class representing the kv snapshot information of a table. It contains:
 * <li>The id of the table
 * <li>The snapshot information of the buckets of the table.
 *
 * @since 0.1
 */
@PublicEvolving
public class KvSnapshotInfo {

    private final long tableId;

    private final BucketsSnapshotInfo snapshots;

    public KvSnapshotInfo(long tableId, Map<Integer, BucketSnapshotInfo> snapshots) {
        this.tableId = tableId;
        this.snapshots = new BucketsSnapshotInfo(snapshots);
    }

    public long getTableId() {
        return tableId;
    }

    public BucketsSnapshotInfo getBucketsSnapshots() {
        return snapshots;
    }

    @Override
    public String toString() {
        return "KvSnapshotInfo{" + "tableId=" + tableId + ", snapshots=" + snapshots + '}';
    }
}
