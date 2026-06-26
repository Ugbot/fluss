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

package org.apache.fluss.utils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Utility class for typed get/put on heap {@code byte[]} buffers.
 *
 * <p>This was historically implemented on {@code sun.misc.Unsafe} ({@code UNSAFE.putInt(target,
 * BYTE_ARRAY_BASE_OFFSET + offset, value)} etc.). It is now implemented on the Java Foreign
 * Function &amp; Memory API: each call wraps the target array once via {@link
 * MemorySegment#ofArray(byte[])} and reads/writes through native-order, unaligned {@link
 * ValueLayout} accessors. These layouts read and write multi-byte values in the platform's native
 * byte order, exactly matching the previous {@code Unsafe.get*}/{@code Unsafe.put*} semantics.
 *
 * <p>The {@code offset} arguments are relative byte indices into the target array. {@link
 * #BYTE_ARRAY_BASE_OFFSET} is retained for source compatibility with callers that add it to a
 * relative index before passing it in; under FFM the array base is the array itself, so the
 * constant is {@code 0} and {@code BYTE_ARRAY_BASE_OFFSET + index == index}.
 */
public class UnsafeUtils {

    /**
     * The base offset to add to a relative {@code byte[]} index.
     *
     * <p>Under the Foreign Function &amp; Memory API, accesses on a {@link
     * MemorySegment#ofArray(byte[])} view use relative offsets starting at {@code 0}, so this is
     * {@code 0}. It is kept (rather than removed) so existing callers that compute {@code
     * BYTE_ARRAY_BASE_OFFSET + relativeIndex} continue to produce the correct relative index.
     */
    public static final long BYTE_ARRAY_BASE_OFFSET = 0L;

    private static final int ADDRESS_BITS_PER_WORD = 3;
    private static final int BIT_BYTE_INDEX_MASK = 7;

    /**
     * Native-order, unaligned layouts that mirror the former {@code Unsafe} machine-order access.
     */
    private static final ValueLayout.OfShort SHORT_NE =
            ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.nativeOrder());

    private static final ValueLayout.OfInt INT_NE =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.nativeOrder());

    private static final ValueLayout.OfLong LONG_NE =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.nativeOrder());

    private static final ValueLayout.OfFloat FLOAT_NE =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.nativeOrder());

    private static final ValueLayout.OfDouble DOUBLE_NE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.nativeOrder());

    public static void putBoolean(byte[] target, long offset, boolean value) {
        MemorySegment.ofArray(target).set(ValueLayout.JAVA_BOOLEAN, offset, value);
    }

    public static void putByte(byte[] target, long offset, byte value) {
        MemorySegment.ofArray(target).set(ValueLayout.JAVA_BYTE, offset, value);
    }

    public static void putShort(byte[] target, long offset, short value) {
        MemorySegment.ofArray(target).set(SHORT_NE, offset, value);
    }

    public static void putInt(byte[] target, long offset, int value) {
        MemorySegment.ofArray(target).set(INT_NE, offset, value);
    }

    public static void putLong(byte[] target, long offset, long value) {
        MemorySegment.ofArray(target).set(LONG_NE, offset, value);
    }

    public static void putFloat(byte[] target, long offset, float value) {
        MemorySegment.ofArray(target).set(FLOAT_NE, offset, value);
    }

    public static void putDouble(byte[] target, long offset, double value) {
        MemorySegment.ofArray(target).set(DOUBLE_NE, offset, value);
    }

    public static void bitSet(byte[] bytes, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = getByte(bytes, offset);
        current |= (1 << (index & BIT_BYTE_INDEX_MASK));
        putByte(bytes, offset, current);
    }

    public static byte getByte(byte[] target, long offset) {
        return MemorySegment.ofArray(target).get(ValueLayout.JAVA_BYTE, offset);
    }

    public static int getInt(byte[] target, long offset) {
        return MemorySegment.ofArray(target).get(INT_NE, offset);
    }

    public static short getShort(byte[] target, long offset) {
        return MemorySegment.ofArray(target).get(SHORT_NE, offset);
    }

    public static long getLong(byte[] target, long offset) {
        return MemorySegment.ofArray(target).get(LONG_NE, offset);
    }

    private static int byteIndex(int bitIndex) {
        return bitIndex >>> ADDRESS_BITS_PER_WORD;
    }
}
