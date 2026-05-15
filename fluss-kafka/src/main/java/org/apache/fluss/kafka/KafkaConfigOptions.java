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

package org.apache.fluss.kafka;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.metadata.LogFormat;

/**
 * Kafka-bolt-on-specific {@link ConfigOption}s. Kept inside the {@code fluss-kafka} module so the
 * bolt-on stays self-contained and {@code fluss-common} doesn't accrue protocol-specific config.
 *
 * <p>The four pre-existing Kafka keys ({@code kafka.enabled}, {@code kafka.listener-names}, {@code
 * kafka.database}, {@code kafka.connection.max-idle-time}) continue to live on {@code
 * org.apache.fluss.config.ConfigOptions} because they predate this FIP.
 */
@Internal
public final class KafkaConfigOptions {

    private KafkaConfigOptions() {}

    /**
     * On-disk log format for new Kafka log (non-PK) topics created via the Kafka API. Defaults to
     * {@link LogFormat#ARROW}; pin to {@link LogFormat#INDEXED} to revert to the prior row-oriented
     * format. Existing topics are unaffected; the produce path reads the format per-table.
     * Compacted topics use {@code KvFormat.INDEXED} regardless of this setting. Read once at server
     * start; toggling requires a restart.
     */
    public static final ConfigOption<LogFormat> KAFKA_LOG_FORMAT =
            ConfigBuilder.key("kafka.log-format")
                    .enumType(LogFormat.class)
                    .defaultValue(LogFormat.ARROW)
                    .withDescription(
                            "On-disk log format for new Kafka log (non-PK) topics created via the "
                                    + "Kafka API. Defaults to ARROW; pin to INDEXED to revert to "
                                    + "the prior row-oriented format. Existing topics are "
                                    + "unaffected.");

    /**
     * Backing store for Kafka consumer-group committed offsets. {@code fluss_pk_table} (default)
     * persists offsets to the Fluss PK table {@code kafka.__consumer_offsets__} (design doc 0004),
     * survives tablet-server restart and coordinator failover. {@code zk} is the legacy
     * ZooKeeper-backed implementation, retained as a migration escape hatch.
     */
    public static final ConfigOption<String> KAFKA_OFFSETS_STORE =
            ConfigBuilder.key("kafka.offsets.store")
                    .stringType()
                    .defaultValue("fluss_pk_table")
                    .withDescription(
                            "Backing store for Kafka consumer-group committed offsets. Accepted "
                                    + "values: 'fluss_pk_table' (default, persists to the Fluss "
                                    + "PK table kafka.__consumer_offsets__) and 'zk' (legacy "
                                    + "ZooKeeper-backed, retained for one release as a migration "
                                    + "escape hatch).");

    /**
     * Whether to emit per-topic Kafka bolt-on metrics (bytesIn/Out, operations, errors). Disable on
     * clusters where topic cardinality would overwhelm the metric reporter.
     */
    public static final ConfigOption<Boolean> KAFKA_METRICS_PER_TOPIC_ENABLED =
            ConfigBuilder.key("kafka.metrics.per-topic.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to emit per-topic Kafka bolt-on metrics (bytesIn/Out, "
                                    + "operations, errors). Disable on clusters where the "
                                    + "cardinality of topics would overwhelm the metric "
                                    + "reporter.");

    /**
     * Cap on the number of distinct topic sub-groups emitted for Kafka bolt-on per-topic metrics.
     * Topics beyond the cap roll up into a single {@code __overflow__} bucket.
     */
    public static final ConfigOption<Integer> KAFKA_METRICS_PER_TOPIC_MAX_CARDINALITY =
            ConfigBuilder.key("kafka.metrics.per-topic.max-cardinality")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Cap on the number of distinct topic sub-groups emitted for Kafka "
                                    + "bolt-on per-topic metrics. Topics beyond the cap roll up "
                                    + "into a single __overflow__ bucket; a warning is logged on "
                                    + "the first overflow.");

    /** Whether to emit per-consumer-group Kafka bolt-on metrics. */
    public static final ConfigOption<Boolean> KAFKA_METRICS_PER_GROUP_ENABLED =
            ConfigBuilder.key("kafka.metrics.per-group.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to emit per-consumer-group Kafka bolt-on metrics.");

    /**
     * Cap on the number of distinct consumer-group sub-groups emitted for the Kafka bolt-on. Groups
     * beyond the cap roll up into {@code __overflow__}.
     */
    public static final ConfigOption<Integer> KAFKA_METRICS_PER_GROUP_MAX_CARDINALITY =
            ConfigBuilder.key("kafka.metrics.per-group.max-cardinality")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "Cap on the number of distinct consumer-group sub-groups emitted for "
                                    + "the Kafka bolt-on. Groups beyond the cap roll up into "
                                    + "__overflow__.");
}
