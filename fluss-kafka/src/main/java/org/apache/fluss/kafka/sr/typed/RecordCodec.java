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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.RowType;

/**
 * A per-{@code (tableId, schemaId, format)} codec that translates wire bytes into a Fluss {@link
 * BinaryRow} and back.
 *
 * <p>Plan §22.4 hot-path contract:
 *
 * <pre>
 *   interface RecordCodec {
 *       int decodeInto(ByteBuf src, int offset, int length, BinaryRowWriter dst);
 *       int encodeInto(BinaryRow src, ByteBuf dst);
 *       short schemaVersion();
 *       RowType rowType();
 *   }
 * </pre>
 *
 * <p>Implementations are generated at runtime by {@link AvroCodecCompiler} using Janino. The {@code
 * decodeInto} path MUST be zero-allocation on the happy path for primitive scalars — every
 * transient byte[] or boxing in this interface defeats the purpose of compiling.
 *
 * <p>Implementations MUST be thread-safe. Callers may share a single instance across all produce /
 * fetch threads.
 *
 * @since 0.9
 */
@Internal
public interface RecordCodec {

    /**
     * Decode wire bytes from {@code src[offset..offset+length)} into {@code dst}. Advances {@code
     * src}'s reader index by exactly {@code length} bytes (the caller has already sliced the
     * logical frame).
     *
     * @param src the source buffer
     * @param offset absolute offset within {@code src} where the payload begins
     * @param length number of bytes of payload
     * @param dst the destination row writer; will be {@code reset()}-ed by the codec
     * @return the number of bytes actually consumed (MUST equal {@code length} on success)
     */
    int decodeInto(ByteBuf src, int offset, int length, IndexedRowWriter dst);

    /**
     * Encode a {@link BinaryRow} into {@code dst}.
     *
     * @param src the source row
     * @param dst the destination buffer; writer index is advanced by the number of bytes written
     * @return the number of bytes written
     */
    int encodeInto(BinaryRow src, ByteBuf dst);

    /**
     * The schema version this codec was compiled for. Used by the bolt-on wiring to key the codec
     * cache.
     *
     * @return the schema version (from Schema Registry); 0 if not yet bound to an SR version
     */
    short schemaVersion();

    /** The row type this codec reads / writes. */
    RowType rowType();
}
