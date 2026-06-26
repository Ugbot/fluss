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

package org.apache.fluss.memory;

import org.apache.fluss.annotation.Internal;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utility class for memory operations. */
@Internal
public class MemoryUtils {
    /**
     * The "unsafe" handle.
     *
     * <p>The hot-path primitive accessors and off-heap lifecycle of {@link MemorySegment} no longer
     * use this handle: those have been migrated to the Java Foreign Function &amp; Memory API
     * ({@link java.lang.foreign.MemorySegment} / {@link java.lang.foreign.Arena}). The handle is
     * retained only for the few remaining heap {@code byte[]} base-offset / typed bulk-copy helpers
     * outside this class that have not yet been migrated. New code MUST NOT add usages.
     */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    /** The native byte order of the platform on which the system currently runs. */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new Error(
                    "Could not access the sun.misc.Unsafe handle, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", e);
        } catch (IllegalArgumentException e) {
            throw new Error("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new Error(
                    "Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

    /**
     * Get native memory address wrapped by the given {@link ByteBuffer}.
     *
     * <p>This is implemented on top of the Java Foreign Function &amp; Memory API: the direct buffer
     * is viewed as a {@link java.lang.foreign.MemorySegment} via {@link
     * java.lang.foreign.MemorySegment#ofBuffer(ByteBuffer)} and its base {@link
     * java.lang.foreign.MemorySegment#address()} is returned. No reflective access to the private
     * {@code java.nio.Buffer#address} field is required.
     *
     * @param buffer {@link ByteBuffer} which wraps the native memory address to get
     * @return native memory address wrapped by the given {@link ByteBuffer}
     */
    static long getByteBufferAddress(ByteBuffer buffer) {
        checkNotNull(buffer, "buffer is null");
        checkArgument(buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");

        long offHeapAddress;
        try {
            offHeapAddress = java.lang.foreign.MemorySegment.ofBuffer(buffer).address();
        } catch (Throwable t) {
            throw new Error("Could not access direct byte buffer address.", t);
        }

        checkState(offHeapAddress > 0, "negative pointer or size");
        checkState(
                offHeapAddress < Long.MAX_VALUE - Integer.MAX_VALUE,
                "Segment initialized with too large address: "
                        + offHeapAddress
                        + " ; Max allowed address is "
                        + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));

        return offHeapAddress;
    }

    /** Should not be instantiated. */
    private MemoryUtils() {}
}
