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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SplitSkipReader}. */
class SplitSkipReaderTest {

    private static final RowType TEST_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()), new DataField("b", DataTypes.STRING()));

    @Test
    void testReadWithOutSkip() throws IOException {
        List<ScanRecord> scanRecords = mockScanRecords(10);
        TestingSnapshotScanner snapshotScanner = new TestingSnapshotScanner(scanRecords);
        SplitSkipReader reader = new SplitSkipReader(snapshotScanner, 0);

        List<RecordAndPos> records = collectRecords(reader);
        assertRecords(records, scanRecords, 1);
    }

    @Test
    void testReadWithSkip() throws IOException {
        List<ScanRecord> scanRecords = mockScanRecords(20);
        TestingSnapshotScanner snapshotScanner = new TestingSnapshotScanner(scanRecords);
        SplitSkipReader reader = new SplitSkipReader(snapshotScanner, 10);

        List<RecordAndPos> records = collectRecords(reader);
        assertRecords(records, scanRecords.subList(10, scanRecords.size()), 11);

        // skip all
        snapshotScanner = new TestingSnapshotScanner(scanRecords);
        reader = new SplitSkipReader(snapshotScanner, 20);
        records = collectRecords(reader);
        assertThat(records).isEmpty();
    }

    @Test
    void testReadWithSkipOverTotalRecordsNum() {
        List<ScanRecord> scanRecords = mockScanRecords(10);
        TestingSnapshotScanner snapshotScanner = new TestingSnapshotScanner(scanRecords);
        SplitSkipReader snapshotReader = new SplitSkipReader(snapshotScanner, 11);

        assertThatThrownBy(() -> collectRecords(snapshotReader))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Skip more than the number of total records, has skipped 10 record(s), but remain 1 record(s) to skip.");
    }

    /**
     * An test class extends {@link SnapshotScanner} with override poll() method to control the
     * records returned for test purpose.
     */
    private static class TestingSnapshotScanner extends SnapshotScanner implements SplitScanner {

        private final CloseableIterator<ScanRecord> recordIterator;

        public TestingSnapshotScanner(List<ScanRecord> scanRecords) {
            super(new Configuration(), KvFormat.COMPACTED, null, null);
            this.recordIterator = CloseableIterator.wrap(scanRecords.iterator());
        }

        @Override
        @Nullable
        public CloseableIterator<ScanRecord> poll(Duration timeout) {
            return recordIterator.hasNext() ? recordIterator : null;
        }
    }

    private List<ScanRecord> mockScanRecords(int records) {
        List<ScanRecord> scanRecords = new ArrayList<>(records);
        for (int i = 0; i < records; i++) {
            IndexedRow indexedRow = row(TEST_ROW_TYPE, new Object[] {i, "test" + i});
            scanRecords.add(new ScanRecord(indexedRow));
        }
        return scanRecords;
    }

    private void assertRecords(
            List<RecordAndPos> readRecords,
            List<ScanRecord> expectedRecords,
            int startRecordCount) {
        // check records num
        assertThat(readRecords.size()).isEqualTo(expectedRecords.size());

        for (int i = 0; i < readRecords.size(); i++) {
            RecordAndPos recordAndPos = readRecords.get(i);

            // check record
            assertThat(recordAndPos.record()).isEqualTo(expectedRecords.get(i));

            // check pos
            assertThat(recordAndPos.readRecordsCount()).isEqualTo(startRecordCount + i);
        }
    }

    private List<RecordAndPos> collectRecords(SplitSkipReader reader) throws IOException {
        List<RecordAndPos> records = new ArrayList<>();
        while (true) {
            CloseableIterator<RecordAndPos> recordIter = reader.readBatch();
            if (recordIter == null) {
                break;
            }
            while (recordIter.hasNext()) {
                RecordAndPos recordAndPos = recordIter.next();
                records.add(
                        new RecordAndPos(recordAndPos.scanRecord, recordAndPos.readRecordsCount));
            }
            recordIter.close();
        }
        return records;
    }
}
