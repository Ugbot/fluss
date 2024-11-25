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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.DefaultKvRecord;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordReadContext;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.utils.BytesUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvWriteBatch}. */
public class KvWriteBatchTest {
    private InternalRow row;
    private byte[] key;
    private int estimatedSizeInBytes;

    @BeforeEach
    void setup() {
        row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        int[] pkIndex = DATA1_SCHEMA_PK.getPrimaryKeyIndexes();
        key = new KeyEncoder(DATA1_ROW_TYPE, pkIndex).encode(row);
        estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, row);
    }

    @Test
    void testTryAppendWithWriteLimit() throws Exception {
        int writeLimit = 100;
        KvWriteBatch kvProducerBatch =
                createKvWriteBatch(
                        new TableBucket(DATA1_TABLE_ID_PK, 0),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        for (int i = 0;
                i
                        < (writeLimit - DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE)
                                / estimatedSizeInBytes;
                i++) {
            boolean appendResult =
                    kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());

            assertThat(appendResult).isTrue();
        }

        // batch full.
        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testToBytes() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();
        assertDefaultKvRecordBatchEquals(kvProducerBatch.records());
    }

    @Test
    void testCompleteTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(kvWriteBatch.complete()).isTrue();
        assertThatThrownBy(kvWriteBatch::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "A SUCCEEDED batch must not attempt another state change to SUCCEEDED");
    }

    @Test
    void testFailedTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isTrue();
        // FAILED --> FAILED transitions are ignored.
        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isFalse();
    }

    @Test
    void testClose() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));
        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        kvProducerBatch.close();
        assertThat(kvProducerBatch.isClosed()).isTrue();

        appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    protected WriteRecord createWriteRecord() {
        return new WriteRecord(
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK), WriteKind.PUT, key, key, row, null);
    }

    private KvWriteBatch createKvWriteBatch(TableBucket tb) throws Exception {
        return createKvWriteBatch(tb, Integer.MAX_VALUE, MemorySegment.allocateHeapMemory(1000));
    }

    private KvWriteBatch createKvWriteBatch(
            TableBucket tb, int writeLimit, MemorySegment memorySegment) throws Exception {
        return new KvWriteBatch(
                tb,
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                DefaultKvRecordBatch.Builder.builder(
                        DATA1_TABLE_INFO_PK.getSchemaId(),
                        writeLimit,
                        new MemorySegmentOutputView(memorySegment),
                        KvFormat.COMPACTED),
                null);
    }

    private WriteCallback newWriteCallback() {
        return exception -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }

    private void assertDefaultKvRecordBatchEquals(DefaultKvRecordBatch recordBatch) {
        assertThat(recordBatch.getRecordCount()).isEqualTo(1);

        DataType[] dataTypes = DATA1_ROW_TYPE.getChildren().toArray(new DataType[0]);
        Iterator<KvRecord> iterator =
                recordBatch
                        .records(
                                KvRecordReadContext.createReadContext(
                                        KvFormat.COMPACTED, dataTypes))
                        .iterator();
        assertThat(iterator.hasNext()).isTrue();
        KvRecord kvRecord = iterator.next();
        assertThat(toArray(kvRecord.getKey())).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
    }
}
