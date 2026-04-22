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

package org.apache.fluss.kafka.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

/**
 * Schema and {@link TableDescriptor} factory for the Fluss PK table that backs the Kafka consumer
 * offset store ({@code kafka.__consumer_offsets__}).
 *
 * <p>Layout as specified in design doc 0004 §1:
 *
 * <pre>
 *   CREATE TABLE kafka.__consumer_offsets__ (
 *     group_id          STRING NOT NULL,
 *     topic             STRING NOT NULL,
 *     partition         INT    NOT NULL,
 *     committed_offset  BIGINT NOT NULL,
 *     leader_epoch      INT,                -- nullable
 *     metadata          STRING,             -- nullable
 *     commit_time       TIMESTAMP_LTZ(3) NOT NULL,
 *     expire_time       TIMESTAMP_LTZ(3),   -- nullable
 *     PRIMARY KEY (group_id, topic, partition) NOT ENFORCED
 *   )
 *   DISTRIBUTED BY (32);
 * </pre>
 */
@Internal
public final class ConsumerOffsetsTable {

    /** Canonical Fluss-side name of the offsets table, without database qualifier. */
    public static final String TABLE_NAME = "__consumer_offsets__";

    public static final String COL_GROUP_ID = "group_id";
    public static final String COL_TOPIC = "topic";
    public static final String COL_PARTITION = "partition";
    public static final String COL_COMMITTED_OFFSET = "committed_offset";
    public static final String COL_LEADER_EPOCH = "leader_epoch";
    public static final String COL_METADATA = "metadata";
    public static final String COL_COMMIT_TIME = "commit_time";
    public static final String COL_EXPIRE_TIME = "expire_time";

    /** Default bucket count (design 0004 §1: "32 buckets until benchmarks say otherwise"). */
    public static final int DEFAULT_BUCKETS = 32;

    private ConsumerOffsetsTable() {}

    public static TablePath tablePath(String kafkaDatabase) {
        return new TablePath(kafkaDatabase, TABLE_NAME);
    }

    public static Schema schema() {
        return Schema.newBuilder()
                .column(COL_GROUP_ID, DataTypes.STRING().copy(false))
                .column(COL_TOPIC, DataTypes.STRING().copy(false))
                .column(COL_PARTITION, DataTypes.INT().copy(false))
                .column(COL_COMMITTED_OFFSET, DataTypes.BIGINT().copy(false))
                .column(COL_LEADER_EPOCH, DataTypes.INT())
                .column(COL_METADATA, DataTypes.STRING())
                .column(COL_COMMIT_TIME, DataTypes.TIMESTAMP_LTZ(3).copy(false))
                .column(COL_EXPIRE_TIME, DataTypes.TIMESTAMP_LTZ(3))
                .primaryKey(COL_GROUP_ID, COL_TOPIC, COL_PARTITION)
                .build();
    }

    public static TableDescriptor descriptor() {
        return TableDescriptor.builder().schema(schema()).distributedBy(DEFAULT_BUCKETS).build();
    }
}
