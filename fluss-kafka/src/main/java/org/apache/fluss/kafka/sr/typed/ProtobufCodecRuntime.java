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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * Protobuf wire-format helpers used by the Janino-generated {@link RecordCodec} implementations.
 * Direct Netty {@link ByteBuf} ops to avoid {@code CodedInputStream}'s per-call allocations.
 *
 * <p>Scalar-only for Phase T-MF.4 (mirrors {@link JsonCodecRuntime} + {@link AvroCodecRuntime} T.1
 * scope).
 *
 * <p>Wire types (Protobuf spec, "Encoding"):
 *
 * <ul>
 *   <li>{@code 0} = varint (bool, int32, int64, uint32, uint64, sint32, sint64, enum)
 *   <li>{@code 1} = fixed64 (double, fixed64, sfixed64)
 *   <li>{@code 2} = length-delimited (string, bytes, embedded messages, packed repeated)
 *   <li>{@code 5} = fixed32 (float, fixed32, sfixed32)
 * </ul>
 *
 * @since 0.9
 */
@Internal
public final class ProtobufCodecRuntime {

    /** Wire type bit mask (low 3 bits). */
    public static final int WIRE_TYPE_MASK = 0x7;

    public static final int WIRE_VARINT = 0;
    public static final int WIRE_FIXED64 = 1;
    public static final int WIRE_LENGTH = 2;
    public static final int WIRE_FIXED32 = 5;

    private ProtobufCodecRuntime() {}

    // ------------------------------------------------------------------------------------------
    // DECODE
    // ------------------------------------------------------------------------------------------

    /** Read one varint (up to 10 bytes). */
    public static long readVarint(ByteBuf src) {
        long result = 0;
        int shift = 0;
        while (shift < 70) {
            byte b = src.readByte();
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new IllegalStateException("Varint exceeds 10 bytes");
    }

    /** Zigzag-decode a signed 32-bit int from its varint form. */
    public static int zigzagDecode32(long v) {
        return (int) ((v >>> 1) ^ -(v & 1));
    }

    /** Zigzag-decode a signed 64-bit long from its varint form. */
    public static long zigzagDecode64(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    /** Read a fixed 32-bit little-endian value. */
    public static int readFixed32(ByteBuf src) {
        return src.readIntLE();
    }

    /** Read a fixed 64-bit little-endian value. */
    public static long readFixed64(ByteBuf src) {
        return src.readLongLE();
    }

    /** Read a length-prefixed byte[] (length is a varint). */
    public static byte[] readLengthPrefixed(ByteBuf src) {
        int len = (int) readVarint(src);
        byte[] out = new byte[len];
        src.readBytes(out);
        return out;
    }

    /** Read a length-prefixed UTF-8 string. */
    public static String readLengthPrefixedString(ByteBuf src) {
        return new String(readLengthPrefixed(src), StandardCharsets.UTF_8);
    }

    /** Skip bytes whose wire type has already been read off the tag. */
    public static void skipValue(ByteBuf src, int wireType) {
        switch (wireType) {
            case WIRE_VARINT:
                readVarint(src);
                return;
            case WIRE_FIXED64:
                src.skipBytes(8);
                return;
            case WIRE_LENGTH:
                int len = (int) readVarint(src);
                src.skipBytes(len);
                return;
            case WIRE_FIXED32:
                src.skipBytes(4);
                return;
            default:
                throw new IllegalStateException("Unknown wire type: " + wireType);
        }
    }

    // ------------------------------------------------------------------------------------------
    // ENCODE
    // ------------------------------------------------------------------------------------------

    /** Write a varint. */
    public static void writeVarint(ByteBuf dst, long v) {
        while ((v & ~0x7FL) != 0) {
            dst.writeByte((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        dst.writeByte((int) v);
    }

    /** Zigzag-encode a signed 32-bit int. */
    public static long zigzagEncode32(int v) {
        return ((long) v << 1) ^ ((long) v >> 31);
    }

    /** Zigzag-encode a signed 64-bit long. */
    public static long zigzagEncode64(long v) {
        return (v << 1) ^ (v >> 63);
    }

    /** Write the tag byte(s) for a (field number, wire type). */
    public static void writeTag(ByteBuf dst, int fieldNumber, int wireType) {
        writeVarint(dst, ((long) fieldNumber << 3) | wireType);
    }

    public static void writeFixed32(ByteBuf dst, int v) {
        dst.writeIntLE(v);
    }

    public static void writeFixed64(ByteBuf dst, long v) {
        dst.writeLongLE(v);
    }

    public static void writeLengthPrefixed(ByteBuf dst, byte[] value) {
        writeVarint(dst, value.length);
        dst.writeBytes(value);
    }

    public static void writeLengthPrefixedString(ByteBuf dst, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeLengthPrefixed(dst, bytes);
    }

    public static void writeLengthPrefixedBinaryString(ByteBuf dst, BinaryString value) {
        writeLengthPrefixed(dst, value.toString().getBytes(StandardCharsets.UTF_8));
    }
}
