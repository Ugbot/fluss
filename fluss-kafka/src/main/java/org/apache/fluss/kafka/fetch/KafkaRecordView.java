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

import org.apache.kafka.common.header.Header;

/**
 * Decoded view of one Fluss row, ready to be appended to a Kafka {@code MemoryRecordsBuilder}.
 * Lifted from {@code KafkaFetchTranscoder.RowView} for the typed-Fetch hot path (design 0014 §5) so
 * both the passthrough and typed codecs can share a single output DTO.
 *
 * <p>Immutable, package-scope, no allocations on the read side. The {@link #key} / {@link #value}
 * arrays are shared with the codec that produced them — callers must not mutate them.
 */
@Internal
public final class KafkaRecordView {

    /** Empty headers sentinel; mirrors Kafka's {@code Record.EMPTY_HEADERS}. */
    public static final Header[] EMPTY_HEADERS = new Header[0];

    public final long offset;
    public final long timestamp;
    public final byte[] key;
    public final byte[] value;
    public final Header[] headers;

    public KafkaRecordView(
            long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers == null ? EMPTY_HEADERS : headers;
    }

    /** Cheap upper-bound estimate used to size the destination {@code ByteBuffer}. */
    public int estimatedSize() {
        int size = 32; // record overhead approximation
        size += key == null ? 0 : key.length;
        size += value == null ? 0 : value.length;
        if (headers != null) {
            for (Header h : headers) {
                size += 16;
                size += h.key() == null ? 0 : h.key().length();
                size += h.value() == null ? 0 : h.value().length;
            }
        }
        return size;
    }
}
