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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

/**
 * The base table split for the Flink tiering source.
 *
 * <p>This is a thin adapter over the engine-agnostic {@link
 * org.apache.fluss.lake.tiering.split.TieringSplit} that lives in {@code fluss-lake-tiering-core}.
 * All of the split data and behaviour (table path, bucket, offsets, equality, etc.) is inherited
 * from the core class; this Flink subclass only adds the Flink {@link SourceSplit} contract so the
 * splits can flow through the Flink {@code Source}/{@code SplitEnumerator}/{@code SourceReader}
 * runtime. The core already declares {@code splitId()} (the single method required by {@link
 * SourceSplit}), so no extra wiring is needed beyond declaring the interface.
 */
public abstract class TieringSplit extends org.apache.fluss.lake.tiering.split.TieringSplit
        implements SourceSplit {

    public TieringSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int numberOfSplits,
            boolean skipCurrentRound) {
        super(tablePath, tableBucket, partitionName, numberOfSplits, skipCurrentRound);
    }

    public TieringSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int numberOfSplits,
            boolean skipCurrentRound,
            int splitIndex,
            long tieringRoundTimestamp) {
        super(
                tablePath,
                tableBucket,
                partitionName,
                numberOfSplits,
                skipCurrentRound,
                splitIndex,
                tieringRoundTimestamp);
    }

    /**
     * Discriminates on the Flink concrete split types. The core base discriminates on the core
     * concrete types; the Flink adapter splits are distinct types, so the type check is overridden
     * here to recognise the Flink ones. (The core base intentionally leaves these non-final to
     * allow engine adapters to do exactly this.)
     */
    @Override
    public boolean isTieringSnapshotSplit() {
        return this instanceof TieringSnapshotSplit;
    }

    @Override
    public boolean isTieringLogSplit() {
        return this instanceof TieringLogSplit;
    }

    /**
     * Re-exposes the core {@code splitKind()} to the {@code fluss-flink} split package so the
     * {@link TieringSplitSerializer} (which is not a subclass and lives in a different package than
     * the core class) can read it. Widens the inherited {@code protected} method; the inherited
     * body already routes through the overridden {@link #isTieringSnapshotSplit()} / {@link
     * #isTieringLogSplit()} above.
     */
    @Override
    public byte splitKind() {
        return super.splitKind();
    }

    /**
     * Narrows the return type of the core {@code copy(...)} to the Flink {@link TieringSplit} so
     * the Flink enumerator can keep working with {@code List<TieringSplit>} (Flink type).
     */
    @Override
    public abstract TieringSplit copy(
            int numberOfSplits, int splitIndex, long tieringRoundTimestamp);

    @Override
    public TieringSplit copy(int numberOfSplits) {
        return copy(numberOfSplits, getSplitIndex(), getTieringRoundTimestamp());
    }
}
