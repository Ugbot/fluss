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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Broker-level config catalogue the bolt-on reports on {@code DescribeConfigs(BROKER, id)}. Every
 * entry is static + read-only; {@code AlterConfigs} on a BROKER resource continues to be rejected
 * (broker configs change only via {@code server.yaml} + restart, like today).
 *
 * <p>The point of this class is to unblock tools that enumerate cluster state ({@code
 * kafka-configs.sh --entity-type brokers}, Cruise Control, MirrorMaker 2) — they crash if
 * DescribeConfigs on BROKER returns {@code INVALID_REQUEST}. Values come from the running Fluss
 * configuration where a sensible mapping exists; everything else reports Kafka's documented default
 * so clients get a consistent shape.
 */
@Internal
public final class KafkaBrokerConfigs {

    /**
     * One broker-config entry. All are read-only from the Kafka surface — dynamic broker-config
     * alter stays out-of-scope (see plan §28.8).
     */
    public static final class Entry {
        public final String kafkaName;
        public final String value;
        public final String documentation;
        public final boolean isSensitive;

        Entry(String kafkaName, String value, String documentation, boolean isSensitive) {
            this.kafkaName = kafkaName;
            this.value = value;
            this.documentation = documentation;
            this.isSensitive = isSensitive;
        }
    }

    private KafkaBrokerConfigs() {}

    /**
     * Build the broker catalogue for the given running configuration + broker id. Values reflect
     * the running config where derivable; other entries get Kafka's documented default.
     */
    public static Map<String, Entry> entries(Configuration flussConf, int brokerId) {
        Map<String, Entry> m = new LinkedHashMap<>();

        // --- identity / network -----------------------------------
        put(m, "broker.id", Integer.toString(brokerId), "This broker's numeric id.");
        put(
                m,
                "broker.rack",
                readOrEmpty(flussConf, "tablet-server.rack"),
                "Rack identifier reported on ApiVersions responses.");
        put(
                m,
                "listeners",
                readOrDefault(flussConf, "bind.listeners", "FLUSS://0.0.0.0:9092"),
                "Listeners this broker binds to.");
        put(
                m,
                "advertised.listeners",
                readOrDefault(flussConf, "advertised.listeners", "FLUSS://0.0.0.0:9092"),
                "Listeners advertised to clients via METADATA.");
        put(m, "num.network.threads", "3", "Number of Netty worker threads serving Kafka traffic.");
        put(
                m,
                "num.io.threads",
                "8",
                "Number of IO threads the broker dispatches Kafka requests to.");
        put(m, "socket.send.buffer.bytes", "102400", "Per-connection send buffer.");
        put(m, "socket.receive.buffer.bytes", "102400", "Per-connection receive buffer.");
        put(
                m,
                "socket.request.max.bytes",
                "104857600",
                "Maximum Kafka-wire request size (equivalent to netty.server.max-request-size).");

        // --- log / retention --------------------------------------
        put(
                m,
                "log.dirs",
                readOrDefault(flussConf, "data.dir", "/tmp/fluss-data"),
                "Filesystem locations where log data is stored.");
        put(
                m,
                "log.dir",
                readOrDefault(flussConf, "data.dir", "/tmp/fluss-data"),
                "Alias for log.dirs (single value).");
        put(m, "log.retention.hours", "168", "Default retention window (168h = 7 days).");
        put(
                m,
                "log.retention.ms",
                Long.toString(ttlDefaultMs(flussConf)),
                "Default retention in milliseconds; effective default for newly-created topics.");
        put(
                m,
                "log.retention.bytes",
                "-1",
                "Default per-partition byte budget. -1 means unlimited.");
        put(m, "log.segment.bytes", "1073741824", "Default segment size target.");
        put(m, "log.roll.hours", "168", "Default active segment roll interval.");
        put(m, "log.roll.ms", "604800000", "Default active segment roll interval (ms).");
        put(m, "log.index.interval.bytes", "4096", "Default sparse-index spacing.");
        put(
                m,
                "log.flush.scheduler.interval.ms",
                "9223372036854775807",
                "Background scheduled-flush interval.");
        put(
                m,
                "log.flush.interval.messages",
                "9223372036854775807",
                "Messages between forced fsyncs.");
        put(
                m,
                "log.flush.interval.ms",
                "9223372036854775807",
                "Milliseconds between forced fsyncs.");

        // --- cluster / replication --------------------------------
        put(
                m,
                "num.partitions",
                Integer.toString(flussConf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER)),
                "Default bucket count for newly-created topics.");
        put(
                m,
                "default.replication.factor",
                Integer.toString(flussConf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR)),
                "Default replication factor for newly-created topics.");
        put(
                m,
                "min.insync.replicas",
                "1",
                "Default min-ISR. Reported; not enforced at the broker level.");
        put(
                m,
                "auto.create.topics.enable",
                "false",
                "Fluss never auto-creates topics on metadata lookups.");
        put(m, "delete.topic.enable", "true", "Topic deletion is supported.");
        put(
                m,
                "unclean.leader.election.enable",
                "false",
                "Out-of-sync replicas may not be elected leader.");
        put(
                m,
                "zookeeper.connect",
                readOrEmpty(flussConf, "zookeeper.address"),
                "ZooKeeper connect string used for Fluss metadata. Read-only here.");
        put(m, "zookeeper.connection.timeout.ms", "18000", "ZooKeeper connection timeout.");

        // --- group coordinator ------------------------------------
        put(
                m,
                "group.initial.rebalance.delay.ms",
                "3000",
                "Grace period before a rebalance triggers after JoinGroup.");
        put(m, "group.max.session.timeout.ms", "1800000", "Maximum consumer session timeout.");
        put(m, "group.min.session.timeout.ms", "6000", "Minimum consumer session timeout.");

        // --- SASL (if enabled) ------------------------------------
        String mechs = readOrEmpty(flussConf, "security.sasl.enabled.mechanisms");
        put(m, "sasl.enabled.mechanisms", mechs, "SASL mechanisms advertised by this broker.");
        put(
                m,
                "sasl.mechanism.inter.broker.protocol",
                "",
                "Inter-broker SASL mechanism (Fluss uses its own RPC auth).");

        return Collections.unmodifiableMap(m);
    }

    // ----------------------------------------------------------------
    //  helpers
    // ----------------------------------------------------------------

    private static void put(Map<String, Entry> m, String k, String v, String doc) {
        m.put(k, new Entry(k, v == null ? "" : v, doc, false));
    }

    private static String readOrEmpty(Configuration conf, String key) {
        return readOrDefault(conf, key, "");
    }

    private static String readOrDefault(Configuration conf, String key, String defaultVal) {
        // Untyped read: look up the raw string value from the configuration's backing map.
        Map<String, String> asMap = conf.toMap();
        String v = asMap.get(key);
        return v == null ? defaultVal : v;
    }

    private static long ttlDefaultMs(@Nullable Configuration conf) {
        if (conf == null) {
            return 7L * 24L * 60L * 60L * 1000L;
        }
        try {
            return conf.get(ConfigOptions.TABLE_LOG_TTL).toMillis();
        } catch (Exception ignore) {
            return 7L * 24L * 60L * 60L * 1000L;
        }
    }
}
