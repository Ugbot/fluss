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
import org.apache.fluss.kafka.metadata.KafkaDataTable;
import org.apache.fluss.metadata.TableDescriptor;

import org.apache.kafka.common.Uuid;

import javax.annotation.Nullable;

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
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(KafkaDataTable.schema())
                        .distributedBy(numPartitions)
                        .customProperty(CustomPropertiesTopicsCatalog.PROP_BINDING_MARKER, "true")
                        .customProperty(CustomPropertiesTopicsCatalog.PROP_TOPIC_NAME, topic)
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TIMESTAMP_TYPE,
                                timestampType.wireName())
                        .customProperty(
                                CustomPropertiesTopicsCatalog.PROP_TOPIC_ID, topicId.toString());
        if (compression != null) {
            builder.customProperty(
                    CustomPropertiesTopicsCatalog.PROP_COMPRESSION, compression.name());
        }
        return builder.build();
    }
}
