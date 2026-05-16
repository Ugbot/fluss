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
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Byte-copy Fetch codec — the pre-T.2 path lifted from {@code KafkaFetchTranscoder.encode} so the
 * passthrough behaviour is preserved verbatim while the typed branch lives alongside it. Reads
 * fixed columns of the {@link KafkaDataTable} shape: {@code 0=record_key (BYTES)}, {@code 1=payload
 * (BYTES)}, {@code 2=event_time (TIMESTAMP_LTZ)}, {@code 3=headers}.
 */
@Internal
public final class PassthroughKafkaFetchCodec implements KafkaFetchCodec {

    private final @Nullable TableInfo tableInfo;
    private final Schema effectiveSchema;

    public PassthroughKafkaFetchCodec(@Nullable TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        // Compacted topics expose a different RowType (record_key non-null PK) AND a different
        // CDC log format (PK tables default to ARROW). When tableInfo is null (handler error
        // path) fall back to the canonical KafkaDataTable shape so legacy callers still work.
        this.effectiveSchema = tableInfo != null ? tableInfo.getSchema() : KafkaDataTable.schema();
    }

    @Override
    public LogRecordReadContext readContext(int schemaId) {
        SchemaGetter schemaGetter = constantSchemaGetter(effectiveSchema, schemaId);
        if (tableInfo != null) {
            return LogRecordReadContext.createReadContext(
                    tableInfo, /* readFromRemote */ false, null, schemaGetter);
        }
        return LogRecordReadContext.createIndexedReadContext(
                effectiveSchema.getRowType(), schemaId, schemaGetter);
    }

    @Override
    public KafkaRecordView rowToKafkaRecord(InternalRow row, ChangeType type, long logOffset) {
        // Columns: 0=record_key (BYTES), 1=payload (BYTES), 2=event_time (TIMESTAMP_LTZ),
        //          3=headers (ARRAY<ROW<name STRING, value BYTES>>).
        boolean tombstone = type == ChangeType.DELETE;
        byte[] key = row.isNullAt(0) ? null : row.getBytes(0);
        byte[] value = tombstone || row.isNullAt(1) ? null : row.getBytes(1);
        long ts = row.getTimestampLtz(2, 3).toEpochMicros() / 1000L;
        Header[] headers;
        if (row.isNullAt(3)) {
            headers = KafkaRecordView.EMPTY_HEADERS;
        } else {
            InternalArray arr = row.getArray(3);
            headers = new Header[arr.size()];
            for (int i = 0; i < arr.size(); i++) {
                if (arr.isNullAt(i)) {
                    headers[i] = new RecordHeader((String) null, null);
                    continue;
                }
                InternalRow headerRow = arr.getRow(i, 2);
                String name = headerRow.isNullAt(0) ? null : headerRow.getString(0).toString();
                byte[] hval = headerRow.isNullAt(1) ? null : headerRow.getBytes(1);
                headers[i] = new RecordHeader(name, hval);
            }
        }
        return new KafkaRecordView(logOffset, ts, key, value, headers);
    }

    /**
     * Build a {@link SchemaGetter} that always answers with {@code schema} regardless of which id
     * was asked for. Mirrors the inline anonymous class previously inside {@code
     * KafkaFetchTranscoder.encode}; pulled out so {@link TypedKafkaFetchCodec} can reuse it.
     */
    static SchemaGetter constantSchemaGetter(Schema schema, int schemaId) {
        return new SchemaGetter() {
            @Override
            public Schema getSchema(int id) {
                return schema;
            }

            @Override
            public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int id) {
                return CompletableFuture.completedFuture(new SchemaInfo(schema, id));
            }

            @Override
            public SchemaInfo getLatestSchemaInfo() {
                return new SchemaInfo(schema, schemaId > 0 ? schemaId : 1);
            }

            @Override
            public void release() {}
        };
    }
}
