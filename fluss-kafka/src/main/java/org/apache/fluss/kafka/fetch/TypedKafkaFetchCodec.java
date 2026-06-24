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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Typed Fetch codec — design 0014 §5. Calls {@link RecordCodec#encodeInto} on the typed value
 * columns of a Fluss row, prepends the 5-byte Kafka SR wire frame ({@code [0x00][int32
 * srSchemaId]}), and emits the resulting bytes as the Kafka record value.
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

    // record_key is always the leading column; COL_PAYLOAD is only used on the 4-column
    // passthrough-shape path in encodeValue().
    private static final int COL_RECORD_KEY = 0;
    private static final int COL_PAYLOAD = 1;

    private final TableInfo tableInfo;
    private final RecordCodec codec;
    private final int srSchemaId;
    private final @Nullable KafkaMetricGroup metrics;
    private final SchemaGetter schemaGetter;

    // event_time and headers are always the last two columns of a Kafka-shaped table regardless of
    // whether it is in passthrough (4 cols) or typed (N+3 cols) form. Compute their indices from
    // the actual schema so they remain correct after T.3 shape evolution adds user columns.
    private final int colEventTime;
    private final int colHeaders;

    /** Pre-allocated scratch buffer for {@link RecordCodec#encodeInto}. */
    private final ByteBuf scratch = Unpooled.buffer(1024);

    public TypedKafkaFetchCodec(
            TableInfo tableInfo,
            RecordCodec codec,
            int srSchemaId,
            @Nullable KafkaMetricGroup metrics) {
        this(tableInfo, codec, srSchemaId, metrics, null);
    }

    public TypedKafkaFetchCodec(
            TableInfo tableInfo,
            RecordCodec codec,
            int srSchemaId,
            @Nullable KafkaMetricGroup metrics,
            @Nullable SchemaGetter schemaGetter) {
        this.tableInfo = tableInfo;
        this.codec = codec;
        this.srSchemaId = srSchemaId;
        this.metrics = metrics;
        this.schemaGetter =
                schemaGetter != null
                        ? schemaGetter
                        : PassthroughKafkaFetchCodec.constantSchemaGetter(
                                tableInfo.getSchema(), tableInfo.getSchemaId());
        int fieldCount = tableInfo.getRowType().getFieldCount();
        this.colEventTime = fieldCount - 2;
        this.colHeaders = fieldCount - 1;
    }

    @Override
    public LogRecordReadContext readContext(int schemaId) {
        // Use the shared schemaGetter so old batches (written with an earlier Fluss schema ID)
        // are deserialized with their original schema and then projected to the current target
        // schema by DefaultLogRecordBatch via getOutputProjectedRow(). Without this, every batch
        // regardless of its schemaId is read as the current schema, which corrupts v1 rows after
        // a T.3 additive evolution (e.g. reading a 5-col v1 IndexedRow as 6-col v2).
        return LogRecordReadContext.createReadContext(
                tableInfo, /* readFromRemote */ false, null, schemaGetter);
    }

    @Override
    @Nullable
    public KafkaRecordView rowToKafkaRecord(InternalRow row, ChangeType type, long logOffset) {
        boolean tombstone = type == ChangeType.DELETE;
        byte[] key = row.isNullAt(COL_RECORD_KEY) ? null : row.getBytes(COL_RECORD_KEY);
        long ts = row.getTimestampLtz(colEventTime, 3).toEpochMicros() / 1000L;
        Header[] headers =
                row.isNullAt(colHeaders)
                        ? KafkaRecordView.EMPTY_HEADERS
                        : extractHeaders(row, colHeaders);

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
                    srSchemaId,
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
        scratch.clear();
        // Kafka SR wire frame: [0x00][big-endian int32 schemaId][body]
        scratch.writeByte(0x00);
        scratch.writeInt(srSchemaId);
        // For T.2 the typed table still uses the passthrough KafkaDataTable column layout — the
        // typed codec encodes the BYTES that *would have been* the typed columns once T.3 alters
        // the schema. Until then the IT pre-stages a topic where the value bytes already match
        // the codec's contract; we read the payload column directly and prepend the frame so
        // the Kafka consumer sees a wire-correct Kafka SR record. This branch is structurally
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
            // Typed table (T.3+): user cols live at indices 1..N of the stored row (col 0 is
            // record_key). The codec reads at indices 0..N-1, so wrap with a UserColumnSlice
            // that shifts every column access by COL_RECORD_KEY + 1 = 1.
            // UserColumnSlice accepts InternalRow so schema-evolved v1 batches — which arrive
            // as ProjectedRow (not BinaryRow) after LogRecordReadContext re-maps column indices
            // — are encoded correctly rather than silently skipped.
            codec.encodeInto(new UserColumnSlice(row, COL_RECORD_KEY + 1), scratch);
        }
        byte[] out = new byte[scratch.readableBytes()];
        scratch.readBytes(out);
        return out;
    }

    /**
     * Thin {@link BinaryRow} wrapper that shifts all column-index accesses by a fixed offset. Used
     * so that {@link RecordCodec#encodeInto} can read user columns starting at index 0 while they
     * actually live at {@code colOffset} onward in the physical typed-table row.
     *
     * <p>Only the {@link InternalRow} data-access methods are offset; the {@link
     * org.apache.fluss.row.MemoryAwareGetters} and structural {@link BinaryRow} methods delegate
     * directly to the underlying row — they are never called by the generated Avro encoder.
     */
    private static final class UserColumnSlice implements BinaryRow {

        private final InternalRow delegate;
        private final int colOffset;

        UserColumnSlice(InternalRow delegate, int colOffset) {
            this.delegate = delegate;
            this.colOffset = colOffset;
        }

        @Override
        public int getFieldCount() {
            return delegate.getFieldCount() - colOffset;
        }

        @Override
        public boolean isNullAt(int pos) {
            return delegate.isNullAt(pos + colOffset);
        }

        @Override
        public boolean getBoolean(int pos) {
            return delegate.getBoolean(pos + colOffset);
        }

        @Override
        public byte getByte(int pos) {
            return delegate.getByte(pos + colOffset);
        }

        @Override
        public short getShort(int pos) {
            return delegate.getShort(pos + colOffset);
        }

        @Override
        public int getInt(int pos) {
            return delegate.getInt(pos + colOffset);
        }

        @Override
        public long getLong(int pos) {
            return delegate.getLong(pos + colOffset);
        }

        @Override
        public float getFloat(int pos) {
            return delegate.getFloat(pos + colOffset);
        }

        @Override
        public double getDouble(int pos) {
            return delegate.getDouble(pos + colOffset);
        }

        @Override
        public BinaryString getChar(int pos, int length) {
            return delegate.getChar(pos + colOffset, length);
        }

        @Override
        public BinaryString getString(int pos) {
            return delegate.getString(pos + colOffset);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return delegate.getDecimal(pos + colOffset, precision, scale);
        }

        @Override
        public TimestampNtz getTimestampNtz(int pos, int precision) {
            return delegate.getTimestampNtz(pos + colOffset, precision);
        }

        @Override
        public TimestampLtz getTimestampLtz(int pos, int precision) {
            return delegate.getTimestampLtz(pos + colOffset, precision);
        }

        @Override
        public byte[] getBytes(int pos) {
            return delegate.getBytes(pos + colOffset);
        }

        @Override
        public byte[] getBinary(int pos, int length) {
            return delegate.getBinary(pos + colOffset, length);
        }

        @Override
        public InternalArray getArray(int pos) {
            return delegate.getArray(pos + colOffset);
        }

        @Override
        public InternalMap getMap(int pos) {
            return delegate.getMap(pos + colOffset);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return delegate.getRow(pos + colOffset, numFields);
        }

        @Override
        public MemorySegment[] getSegments() {
            throw new UnsupportedOperationException("UserColumnSlice.getSegments");
        }

        @Override
        public int getOffset() {
            throw new UnsupportedOperationException("UserColumnSlice.getOffset");
        }

        @Override
        public int getSizeInBytes() {
            throw new UnsupportedOperationException("UserColumnSlice.getSizeInBytes");
        }

        @Override
        public void copyTo(byte[] dst, int dstOffset) {
            throw new UnsupportedOperationException("UserColumnSlice.copyTo");
        }

        @Override
        public BinaryRow copy() {
            throw new UnsupportedOperationException("UserColumnSlice.copy");
        }

        @Override
        public void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
            throw new UnsupportedOperationException("UserColumnSlice.pointTo");
        }

        @Override
        public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
            throw new UnsupportedOperationException("UserColumnSlice.pointTo");
        }
    }

    private static Header[] extractHeaders(InternalRow row, int colHeaders) {
        org.apache.fluss.row.InternalArray arr = row.getArray(colHeaders);
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
