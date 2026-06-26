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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Utility class for memory operations.
 *
 * <p>This class no longer holds a {@code sun.misc.Unsafe} handle. All hot-path accessors, the
 * off-heap lifecycle of {@link MemorySegment}, and the byte-buffer address lookup below are
 * implemented on top of the Java Foreign Function &amp; Memory API ({@link
 * java.lang.foreign.MemorySegment} / {@link java.lang.foreign.Arena}). New code MUST NOT
 * reintroduce {@code sun.misc.Unsafe}.
 */
@Internal
public class MemoryUtils {

    /** The native byte order of the platform on which the system currently runs. */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /**
     * Get native memory address wrapped by the given {@link ByteBuffer}.
     *
     * <p>This is implemented on top of the Java Foreign Function &amp; Memory API: the direct
     * buffer is viewed as a {@link java.lang.foreign.MemorySegment} via {@link
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
            // MemorySegment.ofBuffer() spans [position, limit), so its address() is the address of
            // the buffer's CURRENT position. The historical Unsafe-based contract returned the
            // address of logical element 0 (position-independent), so subtract the position to
            // preserve that semantics for positioned/sliced buffers.
            offHeapAddress =
                    java.lang.foreign.MemorySegment.ofBuffer(buffer).address() - buffer.position();
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
