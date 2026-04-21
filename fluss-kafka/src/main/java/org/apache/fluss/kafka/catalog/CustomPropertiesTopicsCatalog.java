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
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;

import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Topics catalog backed by Fluss custom properties on the data table.
 *
 * <p>Each Kafka-managed data table carries these custom properties, which together form the topic's
 * binding:
 *
 * <ul>
 *   <li>{@value #PROP_BINDING_MARKER} = {@code true} — marks the table as Kafka-managed.
 *   <li>{@value #PROP_TOPIC_NAME} — the canonical Kafka topic name.
 *   <li>{@value #PROP_TIMESTAMP_TYPE} — {@code CreateTime} or {@code LogAppendTime}.
 *   <li>{@value #PROP_COMPRESSION} — upper-case compression name; absent = NONE.
 *   <li>{@value #PROP_TOPIC_ID} — hex-encoded Kafka Uuid (16 bytes).
 * </ul>
 *
 * <p>Phase 2A chooses this storage because it avoids wiring tablet-server write access to a PK
 * table; a follow-up will migrate to {@code kafka.__kafka_topics__}.
 */
@Internal
public final class CustomPropertiesTopicsCatalog implements KafkaTopicsCatalog {

    public static final String PROP_BINDING_MARKER = "kafka.binding";
    public static final String PROP_TOPIC_NAME = "kafka.topic.name";
    public static final String PROP_TIMESTAMP_TYPE = "kafka.timestamp.type";
    public static final String PROP_COMPRESSION = "kafka.compression.type";
    public static final String PROP_TOPIC_ID = "kafka.topic.id";

    private final MetadataManager metadataManager;
    private final String database;

    public CustomPropertiesTopicsCatalog(MetadataManager metadataManager, String database) {
        this.metadataManager = metadataManager;
        this.database = database;
    }

    @Override
    public void register(KafkaTopicInfo info) {
        // Nothing to do here: the Fluss createTable call that preceded this has already written
        // the custom properties into ZK via the descriptor. This method exists so a future
        // PK-table-backed implementation can perform its extra write atomically with the table
        // creation.
    }

    @Override
    public void deregister(String topic) {
        // Same story as register(): the subsequent dropTable removes the custom properties with
        // the table itself.
    }

    @Override
    public Optional<KafkaTopicInfo> lookup(String topic) {
        TablePath path = new TablePath(database, topic);
        try {
            TableInfo table = metadataManager.getTable(path);
            return Optional.ofNullable(fromTableInfo(path, table));
        } catch (TableNotExistException notFound) {
            return Optional.empty();
        }
    }

    @Override
    public List<KafkaTopicInfo> listAll() {
        List<KafkaTopicInfo> result = new ArrayList<>();
        List<String> tables;
        try {
            tables = metadataManager.listTables(database);
        } catch (DatabaseNotExistException noDb) {
            return result;
        }
        for (String name : tables) {
            TablePath path = new TablePath(database, name);
            try {
                TableInfo info = metadataManager.getTable(path);
                KafkaTopicInfo topicInfo = fromTableInfo(path, info);
                if (topicInfo != null) {
                    result.add(topicInfo);
                }
            } catch (TableNotExistException removed) {
                // dropped between listTables and getTable - skip
            }
        }
        return result;
    }

    /**
     * @return {@code null} if the table is not a Kafka-managed binding.
     */
    private KafkaTopicInfo fromTableInfo(TablePath path, TableInfo table) {
        Map<String, String> props = table.getCustomProperties().toMap();
        if (!"true".equalsIgnoreCase(props.get(PROP_BINDING_MARKER))) {
            return null;
        }
        String topicName = props.getOrDefault(PROP_TOPIC_NAME, path.getTableName());
        KafkaTopicInfo.TimestampType timestampType =
                KafkaTopicInfo.TimestampType.fromWireName(props.get(PROP_TIMESTAMP_TYPE));
        KafkaTopicInfo.Compression compression =
                KafkaTopicInfo.Compression.fromWireName(props.get(PROP_COMPRESSION));
        Uuid topicId = parseTopicId(props.get(PROP_TOPIC_ID));
        return new KafkaTopicInfo(
                topicName, path, table.getTableId(), timestampType, compression, topicId);
    }

    private static Uuid parseTopicId(String hex) {
        if (hex == null || hex.isEmpty()) {
            return Uuid.ZERO_UUID;
        }
        try {
            return Uuid.fromString(hex);
        } catch (RuntimeException parseFail) {
            return Uuid.ZERO_UUID;
        }
    }
}
