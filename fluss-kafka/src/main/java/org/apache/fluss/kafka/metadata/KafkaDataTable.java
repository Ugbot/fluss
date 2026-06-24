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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

/**
 * The Fluss-native shape of a data table backing a Kafka topic. Column names and types are chosen
 * to be idiomatic Fluss, not shaped around Kafka. Kafka-specific per-topic metadata (timestamp
 * type, compression, Kafka UUID, ...) lives outside the data table — see {@link
 * org.apache.fluss.kafka.catalog.KafkaTopicsCatalog}.
 *
 * <p>When schema-registry-driven typing arrives (design 0002), the {@link #COL_PAYLOAD} column is
 * replaced (ALTER TABLE) by the expanded value columns produced by a compiled codec. The {@link
 * #COL_RECORD_KEY}, {@link #COL_EVENT_TIME} and {@link #COL_HEADERS} columns remain.
 */
@Internal
public final class KafkaDataTable {

    private KafkaDataTable() {}

    public static final String COL_RECORD_KEY = "record_key";
    public static final String COL_PAYLOAD = "payload";
    public static final String COL_EVENT_TIME = "event_time";
    public static final String COL_HEADERS = "headers";

    /** Inner ROW field name for a header element's key. */
    public static final String HEADER_FIELD_NAME = "name";

    /** Inner ROW field name for a header element's value. */
    public static final String HEADER_FIELD_VALUE = "value";

    /**
     * Build the Fluss {@link Schema} used by every Kafka-managed data table. When {@code compacted
     * == true} the {@link #COL_RECORD_KEY} column is marked non-null and made the primary key,
     * giving Fluss upsert-by-key semantics that match Kafka's {@code cleanup.policy=compact}.
     */
    public static Schema schema(boolean compacted) {
        Schema.Builder builder = Schema.newBuilder();
        if (compacted) {
            builder.column(COL_RECORD_KEY, DataTypes.BYTES().copy(false))
                    .column(COL_PAYLOAD, DataTypes.BYTES())
                    .column(COL_EVENT_TIME, DataTypes.TIMESTAMP_LTZ(3).copy(false))
                    .column(
                            COL_HEADERS,
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD(HEADER_FIELD_NAME, DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    HEADER_FIELD_VALUE, DataTypes.BYTES()))))
                    .primaryKey(COL_RECORD_KEY);
        } else {
            builder.column(COL_RECORD_KEY, DataTypes.BYTES())
                    .column(COL_PAYLOAD, DataTypes.BYTES())
                    .column(COL_EVENT_TIME, DataTypes.TIMESTAMP_LTZ(3).copy(false))
                    .column(
                            COL_HEADERS,
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD(HEADER_FIELD_NAME, DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    HEADER_FIELD_VALUE, DataTypes.BYTES()))));
        }
        return builder.build();
    }

    /** Legacy / default accessor — log-shaped (non-compacted) table. */
    public static Schema schema() {
        return schema(false);
    }

    /** Build a {@link TableDescriptor} for the data table behind a Kafka topic. */
    public static TableDescriptor newTableDescriptor(int numBuckets) {
        return TableDescriptor.builder().schema(schema()).distributedBy(numBuckets).build();
    }
}
