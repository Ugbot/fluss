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

package org.apache.fluss.kafka.metadata;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaDataTable}. */
class KafkaDataTableTest {

    @Test
    void columnsAreFlussIdiomatic() {
        Schema schema = KafkaDataTable.schema();
        assertThat(schema.getColumnNames())
                .containsExactly(
                        KafkaDataTable.COL_RECORD_KEY,
                        KafkaDataTable.COL_PAYLOAD,
                        KafkaDataTable.COL_EVENT_TIME,
                        KafkaDataTable.COL_HEADERS);
    }

    @Test
    void recordKeyAndPayloadAreNullableBytes() {
        Schema schema = KafkaDataTable.schema();
        assertThat(schema.getColumn(KafkaDataTable.COL_RECORD_KEY).getDataType())
                .isInstanceOf(BytesType.class)
                .matches(t -> t.isNullable());
        assertThat(schema.getColumn(KafkaDataTable.COL_PAYLOAD).getDataType())
                .isInstanceOf(BytesType.class)
                .matches(t -> t.isNullable());
    }

    @Test
    void eventTimeIsNonNullableLtzTimestamp() {
        Schema schema = KafkaDataTable.schema();
        LocalZonedTimestampType type =
                (LocalZonedTimestampType)
                        schema.getColumn(KafkaDataTable.COL_EVENT_TIME).getDataType();
        assertThat(type.isNullable()).isFalse();
        assertThat(type.getPrecision()).isEqualTo(3);
    }

    @Test
    void headersAreArrayOfRowNameValue() {
        Schema schema = KafkaDataTable.schema();
        ArrayType arr = (ArrayType) schema.getColumn(KafkaDataTable.COL_HEADERS).getDataType();
        RowType row = (RowType) arr.getElementType();
        assertThat(row.getFieldNames())
                .containsExactly(
                        KafkaDataTable.HEADER_FIELD_NAME, KafkaDataTable.HEADER_FIELD_VALUE);
        assertThat(row.getTypeAt(0).getClass().getSimpleName()).isEqualTo("StringType");
        assertThat(row.getTypeAt(1)).isInstanceOf(BytesType.class);
    }

    @Test
    void tableDescriptorBucketCountMatches() {
        TableDescriptor descriptor = KafkaDataTable.newTableDescriptor(7);
        assertThat(descriptor.getTableDistribution()).isPresent();
        assertThat(descriptor.getTableDistribution().get().getBucketCount()).hasValue(7);
    }
}
