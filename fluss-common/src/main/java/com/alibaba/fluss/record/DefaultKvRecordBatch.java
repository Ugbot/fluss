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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.exception.CorruptMessageException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.Preconditions;
import com.alibaba.fluss.utils.crc.Crc32C;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * KvRecordBatch implementation for magic 0 and above. The schema of {@link KvRecordBatch} is given
 * below:
 *
 * <ul>
 *   RecordBatch =>
 *   <li>Length => Int32
 *   <li>Magic => Int8
 *   <li>CRC => Uint32
 *   <li>SchemaId => Int16
 *   <li>Attributes => Int8
 *   <li>WriterID => Int64
 *   <li>SequenceID => Int32
 *   <li>RecordCount => Int32
 *   <li>Records => [Record]
 * </ul>
 *
 * <p>The CRC covers the data from the schemaId to the end of the batch (i.e. all the bytes that
 * follow the CRC). It is located after the magic byte, which means that servers must parse the
 * magic byte before deciding how to interpret the bytes between the batch length and the magic
 * byte. The CRC-32C (Castagnoli) polynomial is used for the computation.
 *
 * <p>The current attributes are given below:
 *
 * <pre>
 * -----------------------------------------------------------------------------------------------
 * | Unused (0-8)
 * -----------------------------------------------------------------------------------------------
 * </pre>
 *
 * @since 0.1
 */
@PublicEvolving
public class DefaultKvRecordBatch implements KvRecordBatch {

    static final int LENGTH_LENGTH = 4;
    static final int MAGIC_LENGTH = 1;
    static final int CRC_LENGTH = 4;
    static final int SCHEMA_ID_LENGTH = 2;
    static final int ATTRIBUTE_LENGTH = 1;
    static final int WRITE_CLIENT_ID_LENGTH = 8;
    static final int BATCH_SEQUENCE_LENGTH = 4;
    static final int RECORDS_COUNT_LENGTH = 4;

    static final int LENGTH_OFFSET = 0;
    static final int MAGIC_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
    static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    static final int SCHEMA_ID_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTES_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
    static final int WRITE_CLIENT_ID_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int BATCH_SEQUENCE_OFFSET = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
    public static final int RECORDS_COUNT_OFFSET = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    public static final int RECORD_BATCH_HEADER_SIZE = RECORDS_OFFSET;

    public static final int KV_OVERHEAD = LENGTH_OFFSET + LENGTH_LENGTH;

    private MemorySegment segment;
    private int position;

    public void pointTo(MemorySegment segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    @Override
    public boolean isValid() {
        return sizeInBytes() >= RECORD_BATCH_HEADER_SIZE && checksum() == computeChecksum();
    }

    @Override
    public void ensureValid() {
        int sizeInBytes = sizeInBytes();
        if (sizeInBytes < RECORD_BATCH_HEADER_SIZE) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + RECORD_BATCH_HEADER_SIZE
                            + ")");
        }
        if (!isValid()) {
            throw new CorruptMessageException(
                    "Record batch is corrupt (stored crc = "
                            + checksum()
                            + ", computed crc = "
                            + computeChecksum()
                            + ")");
        }
    }

    @Override
    public long checksum() {
        return segment.getUnsignedInt(position + CRC_OFFSET);
    }

    @Override
    public short schemaId() {
        return segment.getShort(position + SCHEMA_ID_OFFSET);
    }

    @Override
    public byte magic() {
        return segment.get(position + MAGIC_OFFSET);
    }

    @Override
    public long writerId() {
        return segment.getLong(position + WRITE_CLIENT_ID_OFFSET);
    }

    @Override
    public int batchSequence() {
        return segment.getInt(position + BATCH_SEQUENCE_OFFSET);
    }

    @Override
    public int sizeInBytes() {
        return KV_OVERHEAD + segment.getInt(position + LENGTH_OFFSET);
    }

    @Override
    public int getRecordCount() {
        return segment.getInt(position + RECORDS_COUNT_OFFSET);
    }

    public MemorySegment getMemorySegment() {
        return segment;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public Iterable<KvRecord> records(ReadContext readContext) {
        return () -> iterator(readContext);
    }

    private Iterator<KvRecord> iterator(ReadContext readContext) {
        if (getRecordCount() == 0) {
            return Collections.emptyIterator();
        }

        return new KvRecordIterator() {
            final short schemaId = schemaId();
            int position = DefaultKvRecordBatch.this.position + RECORD_BATCH_HEADER_SIZE;
            int iteratorNumber = 0;

            @Override
            protected KvRecord readNext() {
                KvRecord kvRecord =
                        DefaultKvRecord.readFrom(segment, position, schemaId, readContext);
                iteratorNumber++;
                position += kvRecord.getSizeInBytes();
                return kvRecord;
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return true;
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    private long computeChecksum() {
        ByteBuffer buffer = segment.wrap(position, sizeInBytes());
        return Crc32C.compute(buffer, SCHEMA_ID_OFFSET, sizeInBytes() - SCHEMA_ID_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKvRecordBatch that = (DefaultKvRecordBatch) o;
        int sizeInBytes = sizeInBytes();
        return sizeInBytes == that.sizeInBytes()
                && segment.equalTo(that.segment, position, that.position, sizeInBytes);
    }

    abstract class KvRecordIterator implements CloseableIterator<KvRecord> {
        private final int numRecords;
        private int readRecords = 0;

        KvRecordIterator() {
            int numRecords = getRecordCount();
            if (numRecords < 0) {
                throw new IllegalArgumentException(
                        "Found invalid record count "
                                + numRecords
                                + " in magic v"
                                + magic()
                                + " batch");
            }
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecords < numRecords;
        }

        @Override
        public KvRecord next() {
            if (readRecords >= numRecords) {
                throw new NoSuchElementException();
            }
            readRecords++;
            KvRecord rec = readNext();
            if (readRecords == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining()) {
                    throw new IllegalArgumentException("Incorrect declared batch size");
                }
            }
            return rec;
        }

        protected abstract KvRecord readNext();

        protected abstract boolean ensureNoneRemaining();

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /** Make a {@link DefaultKvRecordBatch} instance from the given bytes. */
    public static DefaultKvRecordBatch pointToBytes(byte[] bytes) {
        return pointToBytes(bytes, 0);
    }

    /** Make a {@link DefaultKvRecordBatch} instance from the given bytes. */
    public static DefaultKvRecordBatch pointToBytes(byte[] bytes, int offset) {
        DefaultKvRecordBatch records = new DefaultKvRecordBatch();
        records.pointTo(MemorySegment.wrap(bytes), offset);
        return records;
    }

    /** Make a {@link DefaultKvRecordBatch} instance from the given memory segment. */
    public static DefaultKvRecordBatch pointToMemory(MemorySegment segment, int position) {
        DefaultKvRecordBatch records = new DefaultKvRecordBatch();
        records.pointTo(segment, position);
        return records;
    }

    /**
     * Make a {@link DefaultKvRecordBatch} instance from the given byte buffer. The byte buffer can
     * be either direct or non-direct.
     */
    public static DefaultKvRecordBatch pointToByteBuffer(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
            return pointToMemory(segment, buffer.position());
        } else if (buffer.hasArray()) {
            byte[] bytes = buffer.array();
            int offset = buffer.arrayOffset() + buffer.position();
            return pointToBytes(bytes, offset);
        } else {
            // fallback to copy bytes
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return pointToBytes(bytes);
        }
    }

    /** Builder for {@link DefaultKvRecordBatch}. */
    public static class Builder implements AutoCloseable {

        private final int schemaId;
        private final byte magic;
        // The max bytes can be appended.
        private final int writeLimit;
        private final MemorySegmentOutputView outputView;
        private long writerId;
        private int batchSequence;
        private int currentRecordNumber;
        private int sizeInBytes;
        private boolean isClosed;
        private DefaultKvRecordBatch builtRecords;
        private final KvFormat kvFormat;

        private Builder(
                int schemaId,
                byte magic,
                int writeLimit,
                MemorySegmentOutputView outputView,
                KvFormat kvFormat)
                throws IOException {
            Preconditions.checkArgument(
                    schemaId <= Short.MAX_VALUE,
                    "schemaId shouldn't be greater than the max value of short: "
                            + Short.MAX_VALUE);
            this.schemaId = schemaId;
            this.magic = magic;
            this.writeLimit = writeLimit;
            this.outputView = outputView;
            this.writerId = LogRecordBatch.NO_WRITER_ID;
            this.batchSequence = LogRecordBatch.NO_BATCH_SEQUENCE;
            this.currentRecordNumber = 0;
            this.isClosed = false;
            // We don't need to write header information while the builder creating, we'll skip it
            // first.
            outputView.setPosition(RECORD_BATCH_HEADER_SIZE);
            this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
            this.kvFormat = kvFormat;
        }

        public static Builder builder(
                int schemaId, int writeLimit, MemorySegmentOutputView outputView, KvFormat kvFormat)
                throws IOException {
            return new Builder(schemaId, CURRENT_KV_MAGIC_VALUE, writeLimit, outputView, kvFormat);
        }

        public static Builder builder(
                int schemaId, MemorySegmentOutputView outputView, KvFormat kvFormat)
                throws IOException {
            return new Builder(
                    schemaId, CURRENT_KV_MAGIC_VALUE, Integer.MAX_VALUE, outputView, kvFormat);
        }

        /**
         * Check if we have room for a new record containing the given row. If no records have been
         * appended, then this returns true.
         */
        public boolean hasRoomFor(byte[] key, InternalRow row) {
            return sizeInBytes + DefaultKvRecord.sizeOf(key, row) <= writeLimit;
        }

        /**
         * Wrap a KvRecord with the given key, value and append the KvRecord to
         * DefaultKvRecordBatch.
         *
         * @param key the key in the KvRecord to be appended
         * @param row the value in the KvRecord to be appended. If the value is null, it means the
         *     KvRecord is for delete the corresponding key.
         */
        public void append(byte[] key, @Nullable InternalRow row) throws IOException {
            if (isClosed) {
                throw new IllegalStateException(
                        "Tried to put a record, but KvRecordBatchBuilder is closed for record puts.");
            }
            int recordByteSizes =
                    DefaultKvRecord.writeTo(outputView, key, toTargetKvFormatRow(row));
            currentRecordNumber++;
            if (currentRecordNumber == Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Maximum number of records per batch exceeded, max records: "
                                + Integer.MAX_VALUE);
            }
            sizeInBytes += recordByteSizes;
        }

        public void setWriterState(long writerId, int batchBaseSequence) {
            this.writerId = writerId;
            this.batchSequence = batchBaseSequence;
        }

        public void resetWriterState(long writerId, int batchSequence) {
            // trigger to rewrite batch header
            this.builtRecords = null;
            this.writerId = writerId;
            this.batchSequence = batchSequence;
        }

        public DefaultKvRecordBatch build() throws IOException {
            if (builtRecords != null) {
                return builtRecords;
            }

            writeBatchHeader();
            MemorySegment segment = outputView.getMemorySegment();
            builtRecords = DefaultKvRecordBatch.pointToMemory(segment, 0);
            return builtRecords;
        }

        public MemorySegment getMemorySegment() {
            return outputView.getMemorySegment();
        }

        public long writerId() {
            return writerId;
        }

        public int batchSequence() {
            return batchSequence;
        }

        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public void close() throws IOException {
            isClosed = true;
        }

        public int getSizeInBytes() {
            return sizeInBytes;
        }

        // ----------------------- internal methods -------------------------------
        private void writeBatchHeader() throws IOException {
            outputView.setPosition(0);
            // update header.
            outputView.writeInt(sizeInBytes - LENGTH_LENGTH);
            outputView.writeByte(magic);
            // write empty crc first.
            outputView.writeUnsignedInt(0);
            outputView.writeShort((short) schemaId);
            outputView.writeByte(computeAttributes());
            outputView.writeLong(writerId);
            outputView.writeInt(batchSequence);
            outputView.writeInt(currentRecordNumber);
            // Update crc.
            outputView.setPosition(CRC_OFFSET);
            long crc =
                    Crc32C.compute(
                            outputView.getSharedBuffer(),
                            SCHEMA_ID_OFFSET,
                            sizeInBytes - SCHEMA_ID_OFFSET);
            outputView.writeUnsignedInt(crc);
            // reset the position to origin position.
            outputView.setPosition(sizeInBytes);
        }

        private byte computeAttributes() {
            return 0;
        }

        // TODO: support arbitrary InternalRow, especially for the GenericRow.
        private InternalRow toTargetKvFormatRow(InternalRow internalRow) {
            if (internalRow == null) {
                return null;
            }
            if (kvFormat == KvFormat.COMPACTED) {
                if (internalRow instanceof CompactedRow) {
                    return internalRow;
                } else {
                    // currently, we don't support to do row conversion for simplicity,
                    // just throw exception
                    throw new IllegalArgumentException(
                            "The row to be appended to kv record batch with compacted format "
                                    + "should be a compacted row, but got a "
                                    + internalRow.getClass().getSimpleName());
                }
            } else if (kvFormat == KvFormat.INDEXED) {
                if (internalRow instanceof IndexedRow) {
                    return internalRow;
                } else {
                    throw new IllegalArgumentException(
                            "The row to be appended to kv record batch "
                                    + "with indexed format should be a indexed row, but got "
                                    + internalRow.getClass().getSimpleName());
                }
            } else {
                throw new UnsupportedOperationException("Unsupported kv format: " + kvFormat);
            }
        }
    }
}
