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
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.utils.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A batch of kv records that is or will be sent to server by {@link PutKvRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public class KvWriteBatch extends WriteBatch {
    private final DefaultKvRecordBatch.Builder recordsBuilder;
    private final @Nullable int[] targetColumns;

    public KvWriteBatch(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            DefaultKvRecordBatch.Builder recordsBuilder,
            int[] targetColumns) {
        super(tableBucket, physicalTablePath);
        this.recordsBuilder = recordsBuilder;
        this.targetColumns = targetColumns;
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        // currently, we throw exception directly when the target columns of the write record is
        // not the same as the current target columns in the batch
        if (!Arrays.equals(targetColumns, writeRecord.getTargetColumns())) {
            throw new IllegalStateException(
                    String.format(
                            "target columns %s of the write record to append are not the same as the current target columns %s in the batch.",
                            Arrays.toString(writeRecord.getTargetColumns()),
                            Arrays.toString(targetColumns)));
        }

        byte[] key = writeRecord.getKey();
        InternalRow row = writeRecord.getRow();
        Preconditions.checkNotNull(key != null, "key must be not null for kv record");
        Preconditions.checkNotNull(callback, "write callback must be not null");
        if (!recordsBuilder.hasRoomFor(key, row) || isClosed()) {
            return false;
        } else {
            recordsBuilder.append(key, row);
            callbacks.add(callback);
            recordCount++;
            return true;
        }
    }

    @Override
    public void serialize() {
        // do nothing, records are serialized into memory buffer when appending
    }

    @Override
    public boolean trySerialize() {
        // records have been serialized.
        return true;
    }

    @VisibleForTesting
    public DefaultKvRecordBatch records() {
        try {
            return recordsBuilder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to build record batch.", e);
        }
    }

    @Nullable
    public int[] getTargetColumns() {
        return targetColumns;
    }

    @Override
    public BytesView build() {
        DefaultKvRecordBatch recordBatch = records();
        return new MemorySegmentBytesView(
                recordBatch.getMemorySegment(),
                recordBatch.getPosition(),
                recordBatch.sizeInBytes());
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        reopened = false;
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public int sizeInBytes() {
        return recordsBuilder.getSizeInBytes();
    }

    @Override
    public List<MemorySegment> memorySegments() {
        return Collections.singletonList(recordsBuilder.getMemorySegment());
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return recordsBuilder.writerId();
    }

    @Override
    public int batchSequence() {
        return recordsBuilder.batchSequence();
    }

    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        recordsBuilder.resetWriterState(writerId, batchSequence);
    }
}
