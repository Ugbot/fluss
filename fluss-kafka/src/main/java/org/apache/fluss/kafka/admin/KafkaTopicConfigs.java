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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.catalog.CustomPropertiesTopicsCatalog;
import org.apache.fluss.utils.TimeUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Catalogue of the standard Kafka topic-level config keys this broker reports via {@code
 * DescribeConfigs} and accepts on {@code AlterConfigs} / {@code CreateTopics}. See plan §28 for the
 * design.
 *
 * <p>Each entry assigns one of three {@link KafkaConfigTier tiers} — MAPPED (translated to a Fluss
 * table/custom property that the runtime actually honours), STORED (round-tripped through the
 * table's {@code customProperties} but not enforced), or READONLY_DEFAULT (reported with Kafka's
 * default value, alter rejected).
 *
 * <p>Keys outside this catalogue continue to round-trip as opaque custom properties — that
 * preserves the Phase D "annotation" behaviour ({@code ext.owner=team-foo} etc.) the existing ITs
 * cover.
 */
@Internal
public final class KafkaTopicConfigs {

    // ----------------------------------------------------------------
    //  Type tokens
    // ----------------------------------------------------------------

    /** Minimal subset of {@code org.apache.kafka.common.config.ConfigDef.Type} we care about. */
    public enum ConfigType {
        BOOLEAN,
        INT,
        LONG,
        DOUBLE,
        STRING,
        LIST
    }

    /** One topic-config entry. */
    public static final class Entry {
        public final String kafkaName;
        public final ConfigType type;
        public final String defaultValue; // stored as string so DescribeConfigs can return as-is
        public final String documentation;
        public final boolean isSensitive;
        public final KafkaConfigTier tier;

        /**
         * Non-null for {@link KafkaConfigTier#MAPPED} entries. Names a key on either the Fluss
         * table's {@code properties} map (preferred, so the Fluss runtime honours it) or its {@code
         * customProperties} map.
         */
        @Nullable public final String flussKey;

        /**
         * Whether {@link #flussKey} lives in {@code properties} (true) or {@code customProperties}.
         */
        public final boolean flussKeyIsTableProperty;

        /** Convert a raw Fluss-stored string to the Kafka-facing string; null = as-is. */
        @Nullable public final Function<String, String> flussToKafka;

        /** Convert a Kafka-supplied string to the Fluss-stored form; null = as-is. */
        @Nullable public final Function<String, String> kafkaToFluss;

        /**
         * Non-null for {@link #type} = {@link ConfigType#STRING} entries where only an enum of
         * values is legal; used to reject garbage on {@code AlterConfigs}.
         */
        @Nullable public final List<String> allowedValues;

        Entry(Builder b) {
            this.kafkaName = b.kafkaName;
            this.type = b.type;
            this.defaultValue = b.defaultValue;
            this.documentation = b.documentation;
            this.isSensitive = b.isSensitive;
            this.tier = b.tier;
            this.flussKey = b.flussKey;
            this.flussKeyIsTableProperty = b.flussKeyIsTableProperty;
            this.flussToKafka = b.flussToKafka;
            this.kafkaToFluss = b.kafkaToFluss;
            this.allowedValues = b.allowedValues;
        }

        static Builder of(String name, ConfigType type, String defaultValue) {
            return new Builder(name, type, defaultValue);
        }

        static final class Builder {
            private final String kafkaName;
            private final ConfigType type;
            private final String defaultValue;
            private String documentation = "";
            private boolean isSensitive = false;
            private KafkaConfigTier tier = KafkaConfigTier.STORED;
            private String flussKey;
            private boolean flussKeyIsTableProperty = false;
            private Function<String, String> flussToKafka;
            private Function<String, String> kafkaToFluss;
            private List<String> allowedValues;

            Builder(String name, ConfigType type, String defaultValue) {
                this.kafkaName = name;
                this.type = type;
                this.defaultValue = defaultValue;
            }

            Builder doc(String d) {
                this.documentation = d;
                return this;
            }

            Builder tier(KafkaConfigTier t) {
                this.tier = t;
                return this;
            }

            Builder mappedToTableProperty(
                    String flussKey,
                    Function<String, String> flussToKafka,
                    Function<String, String> kafkaToFluss) {
                this.tier = KafkaConfigTier.MAPPED;
                this.flussKey = flussKey;
                this.flussKeyIsTableProperty = true;
                this.flussToKafka = flussToKafka;
                this.kafkaToFluss = kafkaToFluss;
                return this;
            }

            Builder mappedToCustomProperty(
                    String flussKey,
                    Function<String, String> flussToKafka,
                    Function<String, String> kafkaToFluss) {
                this.tier = KafkaConfigTier.MAPPED;
                this.flussKey = flussKey;
                this.flussKeyIsTableProperty = false;
                this.flussToKafka = flussToKafka;
                this.kafkaToFluss = kafkaToFluss;
                return this;
            }

            Builder allowedValues(String... values) {
                this.allowedValues = Collections.unmodifiableList(Arrays.asList(values));
                return this;
            }

            Builder sensitive() {
                this.isSensitive = true;
                return this;
            }

            Entry build() {
                return new Entry(this);
            }
        }
    }

    // ----------------------------------------------------------------
    //  Catalogue
    // ----------------------------------------------------------------

    private static final Map<String, Entry> ENTRIES;

    static {
        Map<String, Entry> m = new LinkedHashMap<>();

        // --- MAPPED --------------------------------------------------
        m.put(
                "cleanup.policy",
                Entry.of("cleanup.policy", ConfigType.STRING, "delete")
                        .doc(
                                "Retention policy. Drives whether the backing Fluss table is a log"
                                        + " or PK table at create-time; cannot be altered after"
                                        + " create.")
                        .mappedToCustomProperty(
                                "kafka.cleanup.policy",
                                v -> v == null ? "delete" : v,
                                v -> v == null ? null : v.toLowerCase(Locale.ROOT))
                        .allowedValues("delete", "compact", "compact,delete")
                        .build());
        m.put(
                "compression.type",
                Entry.of("compression.type", ConfigType.STRING, "producer")
                        .doc(
                                "Compression hint reported to Kafka clients. Stored on the"
                                        + " table-binding; Fluss's internal Arrow codec may use a"
                                        + " different compression.")
                        .mappedToCustomProperty(
                                CustomPropertiesTopicsCatalog.PROP_COMPRESSION,
                                v -> v == null ? "producer" : v.toLowerCase(Locale.ROOT),
                                v -> v == null ? null : v.toUpperCase(Locale.ROOT))
                        .allowedValues(
                                "producer", "uncompressed", "none", "gzip", "snappy", "lz4", "zstd")
                        .build());
        m.put(
                "retention.ms",
                Entry.of("retention.ms", ConfigType.LONG, "604800000")
                        .doc(
                                "Message retention duration. -1 means retain forever. Honoured at"
                                        + " CreateTopic time by mapping to Fluss's table.log.ttl"
                                        + " (see KafkaTableFactory); post-create AlterConfigs"
                                        + " updates the reported custom property only, since"
                                        + " Fluss does not permit altering table.log.ttl on a live"
                                        + " table.")
                        .tier(KafkaConfigTier.STORED)
                        .build());

        // --- STORED (reported, not enforced) -------------------------
        String[][] stored = {
            {
                "message.timestamp.type",
                "STRING",
                "CreateTime",
                "How the broker sets record timestamps."
            },
            {
                "message.timestamp.difference.max.ms",
                "LONG",
                "9223372036854775807",
                "Max permitted timestamp skew between producer and broker."
            },
            {
                "retention.bytes",
                "LONG",
                "-1",
                "Per-partition byte budget for retention. -1 = unlimited."
            },
            {"segment.bytes", "LONG", "1073741824", "Log segment size target."},
            {"segment.ms", "LONG", "604800000", "Active segment roll interval."},
            {"segment.index.bytes", "INT", "10485760", "Per-segment index file size."},
            {"segment.jitter.ms", "LONG", "0", "Jitter applied to segment roll time."},
            {
                "min.insync.replicas",
                "INT",
                "1",
                "Minimum ISR required for acks=all writes. Reported; not enforced by Fluss."
            },
            {"min.cleanable.dirty.ratio", "DOUBLE", "0.5", "Log compaction cleaner threshold."},
            {"min.compaction.lag.ms", "LONG", "0", "Minimum age before a record can be compacted."},
            {
                "max.compaction.lag.ms",
                "LONG",
                "9223372036854775807",
                "Maximum age before a record must be compacted."
            },
            {
                "max.message.bytes",
                "INT",
                "1048588",
                "Maximum record batch size accepted on produce."
            },
            {
                "delete.retention.ms",
                "LONG",
                "86400000",
                "How long tombstones are retained after compaction."
            },
            {
                "file.delete.delay.ms",
                "LONG",
                "60000",
                "Delay before a retired segment file is deleted from disk."
            },
            {"flush.messages", "LONG", "9223372036854775807", "Hard flush message interval."},
            {"flush.ms", "LONG", "9223372036854775807", "Hard flush time interval."},
            {"index.interval.bytes", "INT", "4096", "Bytes between sparse index entries."},
            {
                "leader.replication.throttled.replicas",
                "LIST",
                "",
                "Leader-side replicas under a replication throttle."
            },
            {
                "follower.replication.throttled.replicas",
                "LIST",
                "",
                "Follower-side replicas under a replication throttle."
            },
        };
        for (String[] row : stored) {
            m.put(
                    row[0],
                    Entry.of(row[0], ConfigType.valueOf(row[1]), row[2])
                            .doc(row[3])
                            .tier(KafkaConfigTier.STORED)
                            .build());
        }

        // --- READONLY_DEFAULT ---------------------------------------
        m.put(
                "preallocate",
                Entry.of("preallocate", ConfigType.BOOLEAN, "false")
                        .doc(
                                "Whether new log segments are preallocated on disk. Fluss writes"
                                        + " never preallocate; alter is rejected.")
                        .tier(KafkaConfigTier.READONLY_DEFAULT)
                        .build());
        m.put(
                "message.format.version",
                Entry.of("message.format.version", ConfigType.STRING, "3.0-IV1")
                        .doc(
                                "Kafka log format version. Fluss speaks the 3.0 wire protocol"
                                        + " version; alter is rejected.")
                        .tier(KafkaConfigTier.READONLY_DEFAULT)
                        .build());
        m.put(
                "message.downconversion.enable",
                Entry.of("message.downconversion.enable", ConfigType.BOOLEAN, "true")
                        .doc(
                                "Whether the broker will down-convert message batches to older"
                                        + " clients. Fluss always serves clients at their advertised"
                                        + " API version; alter is rejected.")
                        .tier(KafkaConfigTier.READONLY_DEFAULT)
                        .build());
        m.put(
                "unclean.leader.election.enable",
                Entry.of("unclean.leader.election.enable", ConfigType.BOOLEAN, "false")
                        .doc(
                                "Whether out-of-sync replicas may be elected leader. Fluss's"
                                        + " leader-election model differs; alter is rejected.")
                        .tier(KafkaConfigTier.READONLY_DEFAULT)
                        .build());

        ENTRIES = Collections.unmodifiableMap(m);
    }

    private KafkaTopicConfigs() {}

    /** Returns the full catalogue in stable iteration order. */
    public static Map<String, Entry> entries() {
        return ENTRIES;
    }

    /** Look up one entry by Kafka key; returns null if the key isn't catalogued. */
    @Nullable
    public static Entry get(String kafkaKey) {
        return kafkaKey == null ? null : ENTRIES.get(kafkaKey);
    }

    /**
     * Validate a value against an entry's type + allowed-values set. Returns null when valid,
     * otherwise a human-readable error message.
     */
    @Nullable
    public static String validateValue(Entry entry, String value) {
        if (value == null) {
            return null; // null handled by DELETE/SET callers separately
        }
        try {
            switch (entry.type) {
                case BOOLEAN:
                    {
                        String lc = value.trim().toLowerCase(Locale.ROOT);
                        if (!("true".equals(lc) || "false".equals(lc))) {
                            return "Expected boolean for '"
                                    + entry.kafkaName
                                    + "', got '"
                                    + value
                                    + "'.";
                        }
                        break;
                    }
                case INT:
                    Integer.parseInt(value.trim());
                    break;
                case LONG:
                    Long.parseLong(value.trim());
                    break;
                case DOUBLE:
                    Double.parseDouble(value.trim());
                    break;
                case STRING:
                    if (entry.allowedValues != null
                            && !entry.allowedValues.contains(value.trim().toLowerCase(Locale.ROOT))
                            && !entry.allowedValues.contains(value.trim())) {
                        return "Invalid value '"
                                + value
                                + "' for '"
                                + entry.kafkaName
                                + "'. Allowed: "
                                + entry.allowedValues
                                + ".";
                    }
                    break;
                case LIST:
                    // Comma-separated; accept any (validation deferred).
                    break;
                default:
                    return "Unknown config type: " + entry.type;
            }
        } catch (NumberFormatException nfe) {
            return "Expected "
                    + entry.type
                    + " for '"
                    + entry.kafkaName
                    + "', got '"
                    + value
                    + "'.";
        }
        return null;
    }

    // ----------------------------------------------------------------
    //  retention.ms ↔ table.log.ttl converters (public for KafkaTableFactory)
    // ----------------------------------------------------------------

    /** Fluss {@code Duration} string → milliseconds string. */
    public static String durationToMillis(String duration) {
        if (duration == null) {
            return null;
        }
        try {
            return Long.toString(Duration.parse(duration).toMillis());
        } catch (Exception ignore) {
            try {
                return Long.toString(TimeUtils.parseDuration(duration).toMillis());
            } catch (Exception nested) {
                return duration;
            }
        }
    }

    /** Milliseconds string → Fluss {@code Duration} string. {@code -1} → {@code 0 ms} (forever). */
    public static String millisToDuration(String millis) {
        if (millis == null) {
            return null;
        }
        try {
            long ms = Long.parseLong(millis.trim());
            if (ms < 0) {
                // Kafka convention: -1 means retain forever. Fluss convention: 0 duration.
                return "0 ms";
            }
            return ms + " ms";
        } catch (NumberFormatException nfe) {
            return millis;
        }
    }
}
