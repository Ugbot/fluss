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
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.regex.Pattern;

/**
 * Maps Kafka topic names to Fluss {@link TablePath} and back.
 *
 * <p>Phase 1 convention:
 *
 * <ul>
 *   <li>Non-partitioned table {@code <kafkaDatabase>.<name>} &lrarr; topic {@code <name>}.
 *   <li>Partitioned table {@code <kafkaDatabase>.<name>} with partition {@code <partition>} &lrarr;
 *       topic {@code <name>.<partition>}.
 *   <li>Auto-created topics (future CreateTopics support) must not contain a dot; the first dot is
 *       reserved as the table/partition separator.
 * </ul>
 */
@Internal
public final class KafkaTopicMapping {

    /** Kafka topic legal character set: {@code [a-zA-Z0-9._-]+}. */
    private static final Pattern LEGAL_TOPIC = Pattern.compile("[a-zA-Z0-9._-]+");

    /** Kafka's hard topic-name length cap. */
    public static final int MAX_TOPIC_LENGTH = 249;

    private final String kafkaDatabase;

    public KafkaTopicMapping(String kafkaDatabase) {
        this.kafkaDatabase = kafkaDatabase;
    }

    /**
     * @return true iff the topic name is syntactically a legal Kafka topic.
     */
    public static boolean isValidTopicName(String topic) {
        return topic != null
                && !topic.isEmpty()
                && topic.length() <= MAX_TOPIC_LENGTH
                && !".".equals(topic)
                && !"..".equals(topic)
                && LEGAL_TOPIC.matcher(topic).matches();
    }

    /**
     * Resolve a Kafka topic name against the Fluss namespace. Returns {@code null} if the topic is
     * not a legal Kafka name.
     *
     * <p>The first {@code .} splits the topic into {@code <tableName>.<partitionName>}. If there is
     * no dot, the topic names a non-partitioned table.
     */
    @Nullable
    public ResolvedTopic resolve(String topic) {
        if (!isValidTopicName(topic)) {
            return null;
        }
        int dot = topic.indexOf('.');
        if (dot < 0) {
            return new ResolvedTopic(new TablePath(kafkaDatabase, topic), null);
        }
        String tableName = topic.substring(0, dot);
        String partitionName = topic.substring(dot + 1);
        if (tableName.isEmpty() || partitionName.isEmpty()) {
            return null;
        }
        return new ResolvedTopic(new TablePath(kafkaDatabase, tableName), partitionName);
    }

    /** Render a non-partitioned table as a Kafka topic name. */
    public static String topicFor(String tableName) {
        return tableName;
    }

    /** Render a single partition of a partitioned table as a Kafka topic name. */
    public static String topicFor(String tableName, String partitionName) {
        return tableName + "." + partitionName;
    }

    /**
     * Check whether the topic name is admissible for auto-creation via CreateTopics. Auto-created
     * topics must not contain the partition separator.
     */
    public static boolean isValidAutoCreateTopic(String topic) {
        return isValidTopicName(topic) && topic.indexOf('.') < 0;
    }

    /** Result of resolving a Kafka topic name. */
    public static final class ResolvedTopic {
        private final TablePath tablePath;
        private final @Nullable String partitionName;

        public ResolvedTopic(TablePath tablePath, @Nullable String partitionName) {
            this.tablePath = tablePath;
            this.partitionName = partitionName;
        }

        public TablePath tablePath() {
            return tablePath;
        }

        /** The Fluss partition name, or {@code null} if the topic names a non-partitioned table. */
        @Nullable
        public String partitionName() {
            return partitionName;
        }

        public boolean isPartitioned() {
            return partitionName != null;
        }
    }
}
