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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

/** Bucket assigner interface. */
@Internal
public interface BucketAssigner {

    /**
     * Assign the bucket for the given key. The key may be null.
     *
     * @param key the key
     * @param cluster the cluster
     * @return the bucket id
     * @deprecated use {@link #assignBucket(byte[], InternalRow, Cluster)} instead.
     */
    @Deprecated
    int assignBucket(@Nullable byte[] key, Cluster cluster);

    /**
     * Assign the bucket for the given row with given key.
     *
     * <p>TODO: use {@link com.alibaba.fluss.row.BinaryRow} for the bucket key to replace byte[] and
     * row parameters.
     *
     * @param key the key
     * @param row the row
     * @param cluster the cluster
     * @return the bucket id
     */
    default int assignBucket(@Nullable byte[] key, InternalRow row, Cluster cluster) {
        return assignBucket(key, cluster);
    }

    /**
     * When append record to record accumulator, whether the record accumulator need to abort this
     * record if batch full, and this record need resend by the sender after change bucket id.
     */
    boolean abortIfBatchFull();

    /**
     * Notifies the bucket assigner a new batch is about to be created. When using the sticky bucket
     * assigner, this method can change the chosen sticky bucket for the new batch.
     *
     * @param cluster The current cluster metadata
     * @param prevBucketId The bucket previously selected for the record that triggered a new batch
     */
    default void onNewBatch(Cluster cluster, int prevBucketId) {}

    /** This is called when bucket assigner is closed. */
    void close();
}
