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

import org.apache.fluss.metadata.TableDescriptor;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the round-trip between {@link KafkaTableFactory#buildDescriptor(String, int,
 * KafkaTopicInfo.TimestampType, KafkaTopicInfo.Compression, Uuid)} and the custom properties the
 * catalog reads back.
 */
class CustomPropertiesTopicsCatalogTest {

    @Test
    void roundTripWithCompression() {
        Uuid id = Uuid.randomUuid();
        TableDescriptor descriptor =
                KafkaTableFactory.buildDescriptor(
                        "topic-a",
                        4,
                        KafkaTopicInfo.TimestampType.LOG_APPEND_TIME,
                        KafkaTopicInfo.Compression.LZ4,
                        id);

        Map<String, String> props = descriptor.getCustomProperties();
        assertThat(props)
                .containsEntry(CustomPropertiesTopicsCatalog.PROP_BINDING_MARKER, "true")
                .containsEntry(CustomPropertiesTopicsCatalog.PROP_TOPIC_NAME, "topic-a")
                .containsEntry(CustomPropertiesTopicsCatalog.PROP_TIMESTAMP_TYPE, "LogAppendTime")
                .containsEntry(CustomPropertiesTopicsCatalog.PROP_COMPRESSION, "LZ4")
                .containsEntry(CustomPropertiesTopicsCatalog.PROP_TOPIC_ID, id.toString());
    }

    @Test
    void nullCompressionOmittedFromProperties() {
        TableDescriptor descriptor =
                KafkaTableFactory.buildDescriptor(
                        "no-comp",
                        1,
                        KafkaTopicInfo.TimestampType.CREATE_TIME,
                        null,
                        Uuid.randomUuid());
        assertThat(descriptor.getCustomProperties())
                .doesNotContainKey(CustomPropertiesTopicsCatalog.PROP_COMPRESSION);
    }

    @Test
    void timestampTypeParsing() {
        assertThat(KafkaTopicInfo.TimestampType.fromWireName("CreateTime"))
                .isEqualTo(KafkaTopicInfo.TimestampType.CREATE_TIME);
        assertThat(KafkaTopicInfo.TimestampType.fromWireName("logappendtime"))
                .isEqualTo(KafkaTopicInfo.TimestampType.LOG_APPEND_TIME);
        assertThat(KafkaTopicInfo.TimestampType.fromWireName(null))
                .isEqualTo(KafkaTopicInfo.TimestampType.CREATE_TIME);
    }

    @Test
    void compressionParsing() {
        assertThat(KafkaTopicInfo.Compression.fromWireName(null)).isNull();
        assertThat(KafkaTopicInfo.Compression.fromWireName("NONE")).isNull();
        assertThat(KafkaTopicInfo.Compression.fromWireName("gzip"))
                .isEqualTo(KafkaTopicInfo.Compression.GZIP);
    }
}
