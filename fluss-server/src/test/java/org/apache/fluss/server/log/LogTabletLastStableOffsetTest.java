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

package org.apache.fluss.server.log;

import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase J.3 — verifies {@link LogTablet#lastStableOffset()} and the open-transaction tracking on
 * append. Uses randomized payloads (per project policy) and walks the LSO through the lifecycle of
 * one and two interleaved transactional batches.
 *
 * <p>Scope: covers the in-memory bookkeeping LogTablet now performs on append and on {@link
 * LogTablet#appendTxnMarker(long, short, boolean)}. The Kafka-side IT (read_committed end-to-end)
 * remains gated on the J.4 follow-up; this unit test pins the load-bearing core behaviour.
 */
final class LogTabletLastStableOffsetTest extends LogTestBase {

    private @TempDir File tempDir;
    private LogTablet logTablet;
    private FlussScheduler scheduler;

    @BeforeEach
    public void setup() throws Exception {
        super.before();
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        logTablet =
                LogTablet.create(
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);
    }

    @AfterEach
    public void teardown() throws Exception {
        scheduler.shutdown();
    }

    @Test
    void lsoEqualsHwmWhenNoOpenTxn() throws Exception {
        Random rand = new Random();
        List<Object[]> rows =
                Arrays.asList(
                        new Object[] {rand.nextInt(), randString(rand)},
                        new Object[] {rand.nextInt(), randString(rand)});
        MemoryLogRecords mr = genMemoryLogRecordsWithWriterId(rows, /*writerId*/ -1L, 0, 0L);
        logTablet.appendAsLeader(mr);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        // No transactional writers — heap empty, LSO == HWM.
        assertThat(logTablet.lastStableOffset()).isEqualTo(logTablet.getHighWatermark());
        assertThat(logTablet.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void lsoHoldsAtFirstOffsetWhileTxnOpenAdvancesOnCommit() throws Exception {
        Random rand = new Random();
        long writerId = 1000L + rand.nextInt(50_000);

        // Append a transactional batch (sequence=0 ⇒ tracked as open).
        List<Object[]> txnRows =
                Arrays.asList(
                        new Object[] {rand.nextInt(), randString(rand)},
                        new Object[] {rand.nextInt(), randString(rand)},
                        new Object[] {rand.nextInt(), randString(rand)});
        MemoryLogRecords txnBatch = genMemoryLogRecordsWithWriterId(txnRows, writerId, 0, 0L);
        long firstOffsetOfOpenTxn = logTablet.localLogEndOffset();
        logTablet.appendAsLeader(txnBatch);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        assertThat(logTablet.openTransactionCount()).isEqualTo(1);
        assertThat(logTablet.lastStableOffset()).isEqualTo(firstOffsetOfOpenTxn);
        assertThat(logTablet.lastStableOffset()).isLessThanOrEqualTo(logTablet.getHighWatermark());

        // Append a commit marker; LSO must catch back up to HWM.
        logTablet.appendTxnMarker(writerId, /*epoch*/ (short) 0, /*commit*/ true);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        assertThat(logTablet.openTransactionCount()).isEqualTo(0);
        assertThat(logTablet.lastStableOffset()).isEqualTo(logTablet.getHighWatermark());
    }

    @Test
    void abortMarkerRecordsRangeAndAdvancesLso() throws Exception {
        Random rand = new Random();
        long writerId = 5000L + rand.nextInt(50_000);

        List<Object[]> txnRows =
                Arrays.asList(
                        new Object[] {rand.nextInt(), randString(rand)},
                        new Object[] {rand.nextInt(), randString(rand)});
        MemoryLogRecords txnBatch = genMemoryLogRecordsWithWriterId(txnRows, writerId, 0, 0L);
        long firstOffsetOfOpenTxn = logTablet.localLogEndOffset();
        logTablet.appendAsLeader(txnBatch);
        long markerExpectedOffset = logTablet.localLogEndOffset();
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // Sanity: pre-marker LSO holds.
        assertThat(logTablet.lastStableOffset()).isEqualTo(firstOffsetOfOpenTxn);

        logTablet.appendTxnMarker(writerId, /*epoch*/ (short) 0, /*commit*/ false);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        assertThat(logTablet.openTransactionCount()).isEqualTo(0);
        assertThat(logTablet.lastStableOffset()).isEqualTo(logTablet.getHighWatermark());

        // The aborted-range entry covers [firstOffsetOfOpenTxn, markerOffset).
        List<LogTablet.AbortedTxn> overlapping =
                logTablet.abortedTxnsInRange(firstOffsetOfOpenTxn, logTablet.getHighWatermark());
        assertThat(overlapping).hasSize(1);
        assertThat(overlapping.get(0).writerId()).isEqualTo(writerId);
        assertThat(overlapping.get(0).firstOffset()).isEqualTo(firstOffsetOfOpenTxn);
        assertThat(overlapping.get(0).lastOffsetExclusive()).isEqualTo(markerExpectedOffset);
    }

    @Test
    void interleavedOpenTxnsLsoTracksOldest() throws Exception {
        Random rand = new Random();
        long writerA = 1L + rand.nextInt(10_000);
        long writerB = 100_001L + rand.nextInt(10_000);

        // Open txn A first.
        long firstA = logTablet.localLogEndOffset();
        logTablet.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        java.util.Collections.singletonList(
                                new Object[] {rand.nextInt(), randString(rand)}),
                        writerA,
                        0,
                        0L));

        // Then open txn B.
        logTablet.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        java.util.Collections.singletonList(
                                new Object[] {rand.nextInt(), randString(rand)}),
                        writerB,
                        0,
                        0L));
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // LSO is held at A's first offset (oldest).
        assertThat(logTablet.openTransactionCount()).isEqualTo(2);
        assertThat(logTablet.lastStableOffset()).isEqualTo(firstA);

        // Commit A — LSO advances, but is still pinned by B.
        logTablet.appendTxnMarker(writerA, (short) 0, true);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        assertThat(logTablet.openTransactionCount()).isEqualTo(1);
        assertThat(logTablet.lastStableOffset()).isLessThan(logTablet.getHighWatermark());

        // Commit B — LSO catches up.
        logTablet.appendTxnMarker(writerB, (short) 0, true);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        assertThat(logTablet.openTransactionCount()).isEqualTo(0);
        assertThat(logTablet.lastStableOffset()).isEqualTo(logTablet.getHighWatermark());
    }

    private static String randString(Random r) {
        int len = 4 + r.nextInt(12);
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++) {
            b.append((char) ('a' + r.nextInt(26)));
        }
        return b.toString();
    }
}
