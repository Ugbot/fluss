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

package org.apache.fluss.kafka.catalog;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

import org.apache.kafka.common.Uuid;

import javax.annotation.Nullable;

import java.util.Objects;

/** Kafka-specific metadata for a single topic. Immutable. */
@Internal
public final class KafkaTopicInfo {

    private final String topic;
    private final TablePath dataTablePath;
    private final long flussTableId;
    private final TimestampType timestampType;
    private final @Nullable Compression compression;
    private final Uuid topicId;

    public KafkaTopicInfo(
            String topic,
            TablePath dataTablePath,
            long flussTableId,
            TimestampType timestampType,
            @Nullable Compression compression,
            Uuid topicId) {
        this.topic = topic;
        this.dataTablePath = dataTablePath;
        this.flussTableId = flussTableId;
        this.timestampType = timestampType;
        this.compression = compression;
        this.topicId = topicId;
    }

    public String topic() {
        return topic;
    }

    public TablePath dataTablePath() {
        return dataTablePath;
    }

    public long flussTableId() {
        return flussTableId;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    @Nullable
    public Compression compression() {
        return compression;
    }

    public Uuid topicId() {
        return topicId;
    }

    /** How timestamps on stored records are interpreted. */
    public enum TimestampType {
        CREATE_TIME("CreateTime"),
        LOG_APPEND_TIME("LogAppendTime");

        private final String wireName;

        TimestampType(String wireName) {
            this.wireName = wireName;
        }

        public String wireName() {
            return wireName;
        }

        public static TimestampType fromWireName(String name) {
            if (name == null) {
                return CREATE_TIME;
            }
            for (TimestampType t : values()) {
                if (t.wireName.equalsIgnoreCase(name)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Unknown timestamp type: " + name);
        }
    }

    /** Compression applied on the Kafka wire; the Fluss payload is always uncompressed. */
    public enum Compression {
        GZIP,
        SNAPPY,
        LZ4,
        ZSTD;

        @Nullable
        public static Compression fromWireName(@Nullable String name) {
            if (name == null || name.isEmpty() || "NONE".equalsIgnoreCase(name)) {
                return null;
            }
            for (Compression c : values()) {
                if (c.name().equalsIgnoreCase(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown compression type: " + name);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaTopicInfo)) {
            return false;
        }
        KafkaTopicInfo that = (KafkaTopicInfo) o;
        return flussTableId == that.flussTableId
                && Objects.equals(topic, that.topic)
                && Objects.equals(dataTablePath, that.dataTablePath)
                && timestampType == that.timestampType
                && compression == that.compression
                && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topic, dataTablePath, flussTableId, timestampType, compression, topicId);
    }

    @Override
    public String toString() {
        return "KafkaTopicInfo{"
                + "topic='"
                + topic
                + '\''
                + ", dataTablePath="
                + dataTablePath
                + ", flussTableId="
                + flussTableId
                + ", timestampType="
                + timestampType
                + ", compression="
                + compression
                + ", topicId="
                + topicId
                + '}';
    }
}
