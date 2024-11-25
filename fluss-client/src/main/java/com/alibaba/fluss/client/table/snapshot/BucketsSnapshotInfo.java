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
import java.util.Optional;
import java.util.Set;

/**
 * A class representing the snapshot information of a couples of buckets in a table or a partition.
 *
 * <p>It contains a map from the bucket id to the latest snapshot of the bucket. If the bucket does
 * not have any snapshot, the value for the bucket in the map is {@link Optional#empty()}.
 *
 * @since 0.2
 */
@PublicEvolving
public class BucketsSnapshotInfo {

    // a map from  the bucket id to the latest snapshot of the bucket
    // if no snapshot for the bucket id, the value will be null
    private final Map<Integer, BucketSnapshotInfo> snapshots;

    public BucketsSnapshotInfo(Map<Integer, BucketSnapshotInfo> bucketsSnapshotInfo) {
        this.snapshots = bucketsSnapshotInfo;
    }

    public Set<Integer> getBucketIds() {
        return snapshots.keySet();
    }

    public Optional<BucketSnapshotInfo> getBucketSnapshotInfo(int bucketId) {
        return Optional.ofNullable(snapshots.get(bucketId));
    }

    @Override
    public String toString() {
        return "BucketsSnapshotInfo{" + "snapshots=" + snapshots + '}';
    }
}
