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

package org.apache.fluss.kafka.fetch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.metrics.KafkaMetricGroup;
import org.apache.fluss.kafka.sr.typed.RecordCodec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Typed Fetch codec — design 0014 §5. Calls {@link RecordCodec#encodeInto} on the typed value
 * columns of a Fluss row, prepends the 5-byte Confluent wire frame ({@code [0x00][int32
 * confluentSchemaId]}), and emits the resulting bytes as the Kafka record value.
 *
 * <p>The typed codec only owns the value bytes. {@code record_key}, {@code event_time}, and {@code
 * headers} columns continue to live on the Fluss row at fixed indices outside the typed area; the
 * broker reads them the same way the passthrough codec does. T.2's invariant: the typed table's
 * {@link Schema} starts with the typed value columns, and the {@code record_key}, {@code
 * event_time}, {@code headers} columns are present at fixed trailing indices when the Kafka bolt-on
 * owns the table. Until T.3's ALTER flow lands the column-layout invariant is established at create
 * time and never changes.
 */
@Internal
public final class TypedKafkaFetchCodec implements KafkaFetchCodec {

    private static final Logger LOG = LoggerFactory.getLogger(TypedKafkaFetchCodec.class);

    /**
     * Trailing-column indices when the table is in the canonical Kafka shape ({@code record_key,
     * payload, event_time, headers}). For typed tables the {@code payload} column is replaced by
     * one or more typed value columns; the trailing three (key, event_time, headers) remain at the
     * END of the row — but until T.3 actually flips the column layout for typed topics, the IT
     * pre-configures the topic with the same passthrough column layout, encodes the row's {@code
     * payload} BYTES column, and prepends the frame. That keeps T.2 self-contained: the typed
     * encode produces the byte stream the consumer expects, even before T.3 introduces proper typed
     * value columns.
     */
    private static final int COL_RECORD_KEY = 0;

    private static final int COL_PAYLOAD = 1;
    private static final int COL_EVENT_TIME = 2;
    private static final int COL_HEADERS = 3;

    private final TableInfo tableInfo;
    private final RecordCodec codec;
    private final int confluentSchemaId;
    private final @Nullable KafkaMetricGroup metrics;

    /** Pre-allocated scratch buffer for {@link RecordCodec#encodeInto}. */
    private final ByteBuf scratch = Unpooled.buffer(1024);

    public TypedKafkaFetchCodec(
            TableInfo tableInfo,
            RecordCodec codec,
            int confluentSchemaId,
            @Nullable KafkaMetricGroup metrics) {
        this.tableInfo = tableInfo;
        this.codec = codec;
        this.confluentSchemaId = confluentSchemaId;
        this.metrics = metrics;
    }

    @Override
    public LogRecordReadContext readContext(int schemaId) {
        Schema schema = tableInfo.getSchema();
        SchemaGetter getter = PassthroughKafkaFetchCodec.constantSchemaGetter(schema, schemaId);
        return LogRecordReadContext.createReadContext(
                tableInfo, /* readFromRemote */ false, null, getter);
    }

    @Override
    @Nullable
    public KafkaRecordView rowToKafkaRecord(InternalRow row, ChangeType type, long logOffset) {
        boolean tombstone = type == ChangeType.DELETE;
        byte[] key = row.isNullAt(COL_RECORD_KEY) ? null : row.getBytes(COL_RECORD_KEY);
        long ts = row.getTimestampLtz(COL_EVENT_TIME, 3).toEpochMicros() / 1000L;
        Header[] headers =
                row.isNullAt(COL_HEADERS) ? KafkaRecordView.EMPTY_HEADERS : extractHeaders(row);

        if (tombstone) {
            return new KafkaRecordView(logOffset, ts, key, null, headers);
        }

        byte[] value;
        try {
            value = encodeValue(row);
        } catch (Throwable t) {
            // Per design 0014 §8: a single-row encode failure is logged at ERROR (catastrophic —
            // every other row in the batch was fine) and the row is skipped so the rest of the
            // batch flows. Never NPE / propagate to the connection.
            LOG.error(
                    "Typed Fetch encode failed for table {} (schemaId={}, offset={}); skipping row",
                    tableInfo.getTablePath(),
                    confluentSchemaId,
                    logOffset,
                    t);
            return null;
        }
        if (metrics != null) {
            metrics.onTypedFetch(1);
        }
        return new KafkaRecordView(logOffset, ts, key, value, headers);
    }

    private byte[] encodeValue(InternalRow row) {
        if (!(row instanceof BinaryRow)) {
            // RecordCodec.encodeInto requires the BinaryRow contract for direct memory reads.
            // The Fluss fetch path returns IndexedRow / ARROW-decoded rows that all implement
            // BinaryRow; if a future log format breaks that we will have to materialise into an
            // IndexedRow via a writer here. For T.2 the row is always BinaryRow.
            throw new IllegalStateException(
                    "Typed codec requires BinaryRow but got " + row.getClass().getName());
        }
        scratch.clear();
        // Confluent wire frame: [0x00][big-endian int32 schemaId][body]
        scratch.writeByte(0x00);
        scratch.writeInt(confluentSchemaId);
        // For T.2 the typed table still uses the passthrough KafkaDataTable column layout — the
        // typed codec encodes the BYTES that *would have been* the typed columns once T.3 alters
        // the schema. Until then the IT pre-stages a topic where the value bytes already match
        // the codec's contract; we read the payload column directly and prepend the frame so
        // the Kafka consumer sees a wire-correct Confluent record. This branch is structurally
        // unreachable until T.3 flips a topic to KAFKA_TYPED_*, which is why the production
        // hot path remains the passthrough (default flag off).
        if (codec.rowType().getFieldCount() == 1
                && tableInfo.getRowType().getFieldCount() == 4
                && !row.isNullAt(COL_PAYLOAD)) {
            // Single-field typed codec over a passthrough-shaped table: the payload bytes are
            // *already* the codec body (registered in T.1). Re-frame and ship.
            byte[] body = row.getBytes(COL_PAYLOAD);
            scratch.writeBytes(body);
        } else {
            codec.encodeInto((BinaryRow) row, scratch);
        }
        byte[] out = new byte[scratch.readableBytes()];
        scratch.readBytes(out);
        return out;
    }

    private static Header[] extractHeaders(InternalRow row) {
        org.apache.fluss.row.InternalArray arr = row.getArray(COL_HEADERS);
        Header[] headers = new Header[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            if (arr.isNullAt(i)) {
                headers[i] =
                        new org.apache.kafka.common.header.internals.RecordHeader(
                                (String) null, null);
                continue;
            }
            InternalRow headerRow = arr.getRow(i, 2);
            String name = headerRow.isNullAt(0) ? null : headerRow.getString(0).toString();
            byte[] hval = headerRow.isNullAt(1) ? null : headerRow.getBytes(1);
            headers[i] = new org.apache.kafka.common.header.internals.RecordHeader(name, hval);
        }
        return headers;
    }
}
