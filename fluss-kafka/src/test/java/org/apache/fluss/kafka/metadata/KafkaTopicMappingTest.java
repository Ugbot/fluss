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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaTopicMapping}. */
class KafkaTopicMappingTest {

    private static final String DB = "kafka";

    @Test
    void nonPartitionedRoundTrip() {
        KafkaTopicMapping mapping = new KafkaTopicMapping(DB);
        String table = randomTableName();
        KafkaTopicMapping.ResolvedTopic resolved = mapping.resolve(table);

        assertThat(resolved).isNotNull();
        assertThat(resolved.isPartitioned()).isFalse();
        assertThat(resolved.partitionName()).isNull();
        assertThat(resolved.tablePath().getDatabaseName()).isEqualTo(DB);
        assertThat(resolved.tablePath().getTableName()).isEqualTo(table);

        assertThat(KafkaTopicMapping.topicFor(table)).isEqualTo(table);
    }

    @Test
    void partitionedRoundTripOnFirstDot() {
        KafkaTopicMapping mapping = new KafkaTopicMapping(DB);
        String table = "orders_" + Math.abs(new Random().nextInt());
        String partition = "2026-04-20";
        String topic = KafkaTopicMapping.topicFor(table, partition);

        KafkaTopicMapping.ResolvedTopic resolved = mapping.resolve(topic);

        assertThat(resolved).isNotNull();
        assertThat(resolved.isPartitioned()).isTrue();
        assertThat(resolved.tablePath().getTableName()).isEqualTo(table);
        assertThat(resolved.partitionName()).isEqualTo(partition);
    }

    @Test
    void splitsOnFirstDotOnlyAndPartitionMayContainDots() {
        // Partition name "2026.04.20" must survive a topic whose first dot is the separator.
        KafkaTopicMapping mapping = new KafkaTopicMapping(DB);
        KafkaTopicMapping.ResolvedTopic resolved = mapping.resolve("orders.2026.04.20");

        assertThat(resolved).isNotNull();
        assertThat(resolved.tablePath().getTableName()).isEqualTo("orders");
        assertThat(resolved.partitionName()).isEqualTo("2026.04.20");
    }

    @Test
    void emptyTableOrPartitionRejected() {
        KafkaTopicMapping mapping = new KafkaTopicMapping(DB);
        assertThat(mapping.resolve(".foo")).isNull();
        assertThat(mapping.resolve("foo.")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".", "..", "has space", "UPPER_CASE_OK", "dash-ok", "under_score"})
    void validatorHandlesSpecialCases(String topic) {
        boolean expected =
                !topic.isEmpty()
                        && !".".equals(topic)
                        && !"..".equals(topic)
                        && topic.chars()
                                .allMatch(
                                        c ->
                                                Character.isLetterOrDigit(c)
                                                        || c == '.'
                                                        || c == '_'
                                                        || c == '-');
        assertThat(KafkaTopicMapping.isValidTopicName(topic)).isEqualTo(expected);
    }

    @Test
    void nullRejected() {
        assertThat(KafkaTopicMapping.isValidTopicName(null)).isFalse();
    }

    @Test
    void tooLongRejected() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < KafkaTopicMapping.MAX_TOPIC_LENGTH + 1; i++) {
            builder.append('a');
        }
        assertThat(KafkaTopicMapping.isValidTopicName(builder.toString())).isFalse();
    }

    @Test
    void autoCreateRejectsDots() {
        String dotted = "kafka.events";
        assertThat(KafkaTopicMapping.isValidAutoCreateTopic(dotted)).isFalse();

        String plain = "events_" + Math.abs(new Random().nextLong());
        assertThat(KafkaTopicMapping.isValidAutoCreateTopic(plain)).isTrue();
    }

    @Test
    void autoCreateRejectsInvalidCharsEvenWithoutDots() {
        assertThat(KafkaTopicMapping.isValidAutoCreateTopic("bad=name")).isFalse();
        assertThat(KafkaTopicMapping.isValidAutoCreateTopic("bad,name")).isFalse();
    }

    @Test
    void randomizedValidNamesResolveCleanly() {
        KafkaTopicMapping mapping = new KafkaTopicMapping(DB);
        Random rng = new Random();
        for (int i = 0; i < 64; i++) {
            String table = randomTableName();
            KafkaTopicMapping.ResolvedTopic resolved = mapping.resolve(table);
            assertThat(resolved).as("topic=%s", table).isNotNull();
            assertThat(resolved.isPartitioned()).isFalse();
        }
    }

    private static String randomTableName() {
        // Produces a string matching Kafka's topic regex, without any dot.
        String raw = UUID.randomUUID().toString().replace("-", "_");
        return "t_" + raw;
    }
}
