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

/**
 * Low-level Avro binary-format helpers shared by every Janino-compiled {@link RecordCodec}. All
 * methods operate directly on Netty {@link ByteBuf} to avoid the one-byte-at-a-time {@code
 * InputStream} hop that {@code BinaryDecoder} takes over {@code DirectBinaryDecoder}.
 *
 * <p>Avro binary format reference: see the Avro 1.11 spec, section "Encodings".
 *
 * <ul>
 *   <li>{@code int}, {@code long}: <a href="https://protobuf.dev">ZigZag-encoded varint</a>, max 10
 *       bytes.
 *   <li>{@code float}: little-endian 32-bit IEEE-754.
 *   <li>{@code double}: little-endian 64-bit IEEE-754.
 *   <li>{@code boolean}: single byte, 0 / 1.
 *   <li>{@code bytes} / {@code string}: zigzag length prefix, then the raw payload.
 *   <li>union branch index: zigzag varint.
 * </ul>
 *
 * <p>Every method leaves {@code src}'s reader index advanced by exactly the number of bytes it
 * consumed.
 *
 * @since 0.9
 */
@Internal
public final class AvroCodecRuntime {

    private AvroCodecRuntime() {}

    // ------------------------------------------------------------------------------------------
    // DECODE
    // ------------------------------------------------------------------------------------------

    /** Reads a ZigZag-encoded varint int. Avro ints are up to 5 bytes. */
    public static int readInt(ByteBuf src) {
        long zz = readZigZagVarint(src, 5);
        return (int) zz;
    }

    /** Reads a ZigZag-encoded varint long. Avro longs are up to 10 bytes. */
    public static long readLong(ByteBuf src) {
        return readZigZagVarint(src, 10);
    }

    /** Reads a little-endian 32-bit float. */
    public static float readFloat(ByteBuf src) {
        return Float.intBitsToFloat(src.readIntLE());
    }

    /** Reads a little-endian 64-bit double. */
    public static double readDouble(ByteBuf src) {
        return Double.longBitsToDouble(src.readLongLE());
    }

    /** Reads a single byte; 0 = false, 1 = true, anything else is an error. */
    public static boolean readBoolean(ByteBuf src) {
        byte b = src.readByte();
        if (b == 0) {
            return false;
        }
        if (b == 1) {
            return true;
        }
        throw new IllegalStateException("Avro boolean byte must be 0 or 1, got " + (b & 0xFF));
    }

    /**
     * Reads a length-prefixed byte[] (Avro {@code bytes} or {@code string} payload). Returns a
     * fresh byte[]; unavoidable allocation.
     */
    public static byte[] readBytes(ByteBuf src) {
        long len = readLong(src);
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw new IllegalStateException("Avro bytes length out of range: " + len);
        }
        byte[] out = new byte[(int) len];
        if (len > 0) {
            src.readBytes(out);
        }
        return out;
    }

    /**
     * Reads a length-prefixed UTF-8 string. Returns a fresh String; unavoidable allocation because
     * the Fluss {@code BinaryWriter} ultimately wants a UTF-8 {@code BinaryString}, but most
     * downstream writers hit a bytes path anyway.
     */
    public static String readString(ByteBuf src) {
        byte[] bytes = readBytes(src);
        // UTF-8 decode. BinaryString.fromBytes is available but String's ctor is equivalent for
        // Avro strings, which the spec defines as UTF-8.
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Reads a fixed-size byte[]. Returns a fresh byte[]. */
    public static byte[] readFixed(ByteBuf src, int size) {
        byte[] out = new byte[size];
        src.readBytes(out);
        return out;
    }

    /**
     * Reads the branch index of an Avro union. Returns a non-negative int (zigzag-decoded), which
     * for the supported {@code [null, T]} idiom is 0 if {@code null} or 1 if {@code T}.
     */
    public static int readUnionIndex(ByteBuf src) {
        return readInt(src);
    }

    /** Skip an Avro int/long without materialising the value. */
    public static void skipVarint(ByteBuf src, int maxBytes) {
        for (int i = 0; i < maxBytes; i++) {
            byte b = src.readByte();
            if ((b & 0x80) == 0) {
                return;
            }
        }
        throw new IllegalStateException("Varint exceeded " + maxBytes + " bytes.");
    }

    // ------------------------------------------------------------------------------------------
    // ENCODE
    // ------------------------------------------------------------------------------------------

    /** Writes an int as a ZigZag varint. */
    public static void writeInt(ByteBuf dst, int v) {
        writeZigZagVarint(dst, (long) v);
    }

    /** Writes a long as a ZigZag varint. */
    public static void writeLong(ByteBuf dst, long v) {
        writeZigZagVarint(dst, v);
    }

    /** Writes a float as a little-endian 32-bit IEEE-754. */
    public static void writeFloat(ByteBuf dst, float v) {
        dst.writeIntLE(Float.floatToRawIntBits(v));
    }

    /** Writes a double as a little-endian 64-bit IEEE-754. */
    public static void writeDouble(ByteBuf dst, double v) {
        dst.writeLongLE(Double.doubleToRawLongBits(v));
    }

    /** Writes a boolean as a single byte. */
    public static void writeBoolean(ByteBuf dst, boolean v) {
        dst.writeByte(v ? 1 : 0);
    }

    /** Writes a length-prefixed byte[]. */
    public static void writeBytes(ByteBuf dst, byte[] v) {
        writeLong(dst, v.length);
        dst.writeBytes(v);
    }

    /** Writes a length-prefixed UTF-8 string. */
    public static void writeString(ByteBuf dst, String v) {
        byte[] bytes = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeBytes(dst, bytes);
    }

    /** Writes a length-prefixed UTF-8 byte[] (when the caller already has the UTF-8 bytes). */
    public static void writeUtf8(ByteBuf dst, byte[] utf8) {
        writeBytes(dst, utf8);
    }

    /**
     * Writes a Fluss {@link BinaryString} as an Avro UTF-8 string. Goes through {@code toString()}
     * because {@code BinaryString} doesn't expose a direct byte[] accessor; the redundant UTF-8
     * round-trip is cheap (Latin-1 ascii fast-path inside {@code String}).
     */
    public static void writeBinaryString(ByteBuf dst, BinaryString v) {
        // Encode directly from the source segments: the string's sizeInBytes already reflects the
        // UTF-8 byte length, so we can write it as length-prefixed bytes with one copy.
        int len = v.getSizeInBytes();
        writeLong(dst, len);
        if (len == 0) {
            return;
        }
        byte[] tmp = new byte[len];
        org.apache.fluss.row.BinarySegmentUtils.copyToBytes(
                v.getSegments(), v.getOffset(), tmp, 0, len);
        dst.writeBytes(tmp);
    }

    /** Writes a fixed-size byte[]. */
    public static void writeFixed(ByteBuf dst, byte[] v, int size) {
        if (v.length != size) {
            throw new IllegalStateException(
                    "Expected fixed[" + size + "], got byte[" + v.length + "].");
        }
        dst.writeBytes(v);
    }

    /** Writes the branch index of a union. */
    public static void writeUnionIndex(ByteBuf dst, int idx) {
        writeInt(dst, idx);
    }

    // ------------------------------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------------------------------

    private static long readZigZagVarint(ByteBuf src, int maxBytes) {
        long result = 0L;
        int shift = 0;
        for (int i = 0; i < maxBytes; i++) {
            byte b = src.readByte();
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                // ZigZag decode
                return (result >>> 1) ^ -(result & 1);
            }
            shift += 7;
        }
        throw new IllegalStateException(
                "Avro varint exceeded " + maxBytes + " bytes (corrupt input).");
    }

    private static void writeZigZagVarint(ByteBuf dst, long v) {
        // ZigZag-encode then write as varint.
        long zz = (v << 1) ^ (v >> 63);
        while ((zz & ~0x7FL) != 0L) {
            dst.writeByte((byte) (((int) zz & 0x7F) | 0x80));
            zz >>>= 7;
        }
        dst.writeByte((byte) zz);
    }
}
