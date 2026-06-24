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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * Fetch-side seam between {@link KafkaFetchTranscoder} and the per-route row-shape rendering.
 * Design 0014 §5 — the transcoder picks one codec per batch (passthrough vs typed) and iterates
 * rows through {@link #rowToKafkaRecord}, leaving the change-type filter and {@code
 * MemoryRecordsBuilder} bookkeeping in the driver.
 *
 * <p>Implementations are stateless and safe to share across all fetch threads on a broker; the
 * driver caches one instance per partition for the life of a fetch call.
 */
@Internal
public interface KafkaFetchCodec {

    /**
     * Build the {@link LogRecordReadContext} the Fluss decoder uses to walk each {@code
     * LogRecordBatch}. The {@code schemaId} comes from the batch header — for the typed codec it is
     * the Fluss table's current schema id (not the Kafka SR schema id; the Kafka SR schema id is
     * recovered from the catalog binding).
     */
    LogRecordReadContext readContext(int schemaId);

    /**
     * Render a single Fluss {@link InternalRow} as the Kafka-shaped {@link KafkaRecordView}. The
     * driver has already filtered out {@link ChangeType#UPDATE_BEFORE} rows; this method MUST emit
     * a tombstone (null value) when the change type is {@link ChangeType#DELETE} on a compacted
     * topic.
     *
     * @return the rendered view, or {@code null} when the codec elects to skip this row (e.g.
     *     typed-encode failed and the codec already logged the underlying cause). Returning {@code
     *     null} keeps the rest of the batch healthy.
     */
    @Nullable
    KafkaRecordView rowToKafkaRecord(InternalRow row, ChangeType type, long logOffset);
}
