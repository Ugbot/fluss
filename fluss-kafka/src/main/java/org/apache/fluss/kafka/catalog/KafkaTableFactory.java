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
import org.apache.fluss.kafka.admin.KafkaConfigTier;
import org.apache.fluss.kafka.admin.KafkaTopicConfigs;
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;

import org.apache.kafka.common.Uuid;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Builds the {@link TableDescriptor} for a new Kafka-managed data table, stamped with the custom
 * properties needed by {@link CustomPropertiesTopicsCatalog}.
 */
@Internal
public final class KafkaTableFactory {

    private KafkaTableFactory() {}

    public static TableDescriptor buildDescriptor(
            String topic,
            int numPartitions,
            KafkaTopicInfo.TimestampType timestampType,
            @Nullable KafkaTopicInfo.Compression compression,
            Uuid topicId) {
        return buildDescriptor(
                topic,
                numPartitions,
                timestampType,
                compression,
                topicId,
                false,
                Collections.emptyMap(),
                LogFormat.ARROW);
    }

    public static TableDescriptor buildDescriptor(
            String topic,
            int numPartitions,
            KafkaTopicInfo.TimestampType timestampType,
            @Nullable KafkaTopicInfo.Compression compression,
            Uuid topicId,
            boolean compacted) {
        return buildDescriptor(
                topic,
                numPartitions,
                timestampType,
                compression,
                topicId,
                compacted,
                Collections.emptyMap(),
                LogFormat.ARROW);
    }

    public static TableDescriptor buildDescriptor(
            String topic,
            int numPartitions,
            KafkaTopicInfo.TimestampType timestampType,
            @Nullable KafkaTopicInfo.Compression compression,
            Uuid topicId,
            boolean compacted,
            Map<String, String> kafkaTopicConfigs) {
        return buildDescriptor(
                topic,
                numPartitions,
                timestampType,
                compression,
                topicId,
                compacted,
                kafkaTopicConfigs,
                LogFormat.ARROW);
    }

    /**
     * Build a table descriptor. When {@code compacted == true} the schema makes {@code record_key}
     * the primary key, giving the backing Fluss table upsert-by-key semantics that map directly to
     * Kafka's {@code cleanup.policy=compact}.
     *
     * <p>{@code kafkaTopicConfigs} carries the entries from {@code NewTopic.configs()} that aren't
     * already consumed by the transcoder (timestamp type, compression, cleanup.policy). MAPPED
     * catalogue keys (e.g. {@code retention.ms}) are translated to their Fluss table-property
     * counterparts; STORED and unknown keys land on the {@code customProperties} map so
     * DescribeConfigs round-trips them. Phase K-CFG (plan §28.5).
     *
     * <p>{@code logFormat} pins the on-disk log format for non-compacted topics. Sourced from
     * {@link org.apache.fluss.config.ConfigOptions#KAFKA_LOG_FORMAT} at the call site; defaults to
     * {@link LogFormat#ARROW}. Ignored for compacted topics — those always use {@link
     * org.apache.fluss.metadata.KvFormat#INDEXED}.
     */
    public static TableDescriptor buildDescriptor(
            String topic,
            int numPartitions,
            KafkaTopicInfo.TimestampType timestampType,
            @Nullable KafkaTopicInfo.Compression compression,
            Uuid topicId,
            boolean compacted,
            Map<String, String> kafkaTopicConfigs,
            LogFormat logFormat) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(KafkaDataTable.schema(compacted))
                        .distributedBy(numPartitions)
                        .customProperty(CustomPropertiesTopicsCatalog.PROP_BINDING_MARKER, "true")
                        .customProperty(CustomPropertiesTopicsCatalog.PROP_TOPIC_NAME, topic)
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TIMESTAMP_TYPE,
                                timestampType.wireName())
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TOPIC_ID, topicId.toString());
        if (!compacted) {
            // Stamp the log format chosen by the operator (see KAFKA_LOG_FORMAT). The fetch
            // transcoder's LogRecordReadContext.createReadContext(TableInfo, ...) branches on
            // this so existing tables stamped with another format keep working.
            builder.property(
                    org.apache.fluss.config.ConfigOptions.TABLE_LOG_FORMAT.key(), logFormat.name());
        }
        if (compression != null) {
            builder.customProperty(
                    CustomPropertiesTopicsCatalog.PROP_COMPRESSION, compression.name());
        }
        if (compacted) {
            builder.customProperty("kafka.cleanup.policy", "compact")
                    // IndexedRow for row storage keeps the Kafka bolt-on's produce-path row
                    // construction the same for both log and compacted topics.
                    .property(
                            org.apache.fluss.config.ConfigOptions.TABLE_KV_FORMAT.key(),
                            org.apache.fluss.metadata.KvFormat.INDEXED.name());
        }
        applyKafkaTopicConfigs(builder, kafkaTopicConfigs);
        return builder.build();
    }

    /**
     * Translate catalogued Kafka topic configs from a {@code NewTopic.configs()} map into Fluss
     * properties. Special-cased: {@code retention.ms} → {@code table.log.ttl} (MAPPED at
     * create-time only; see the tier note in {@link KafkaTopicConfigs}). Other catalogued entries
     * and unknown keys pass through to {@code customProperties} so DescribeConfigs reads them back
     * cleanly.
     */
    private static void applyKafkaTopicConfigs(
            TableDescriptor.Builder builder, Map<String, String> kafkaTopicConfigs) {
        if (kafkaTopicConfigs == null || kafkaTopicConfigs.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> e : kafkaTopicConfigs.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key == null || value == null) {
                continue;
            }
            // Skip already-consumed keys (the transcoder extracted timestamp type / compression
            // / cleanup.policy into dedicated custom properties).
            if ("message.timestamp.type".equals(key)
                    || "compression.type".equals(key)
                    || "cleanup.policy".equals(key)) {
                continue;
            }
            KafkaTopicConfigs.Entry entry = KafkaTopicConfigs.get(key);
            if (entry != null && entry.tier == KafkaConfigTier.READONLY_DEFAULT) {
                // READONLY_DEFAULT entries are rejected upstream in the transcoder, but be
                // defensive: skip silently here rather than persisting a value we can't honour.
                continue;
            }
            if ("retention.ms".equals(key)) {
                // Create-time only: translate to Fluss's table-level retention so the runtime
                // honours the value. Also record the raw kafka key as a custom property so
                // DescribeConfigs reads back the caller's exact value (including the "-1 =
                // forever" edge case).
                String flussTtl = KafkaTopicConfigs.millisToDuration(value);
                if (flussTtl != null) {
                    builder.property(
                            org.apache.fluss.config.ConfigOptions.TABLE_LOG_TTL.key(), flussTtl);
                }
                builder.customProperty(key, value);
                continue;
            }
            builder.customProperty(key, value);
        }
    }
}
