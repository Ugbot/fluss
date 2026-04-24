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

package org.apache.fluss.kafka.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.metrics.BoltOnMetricGroup;
import org.apache.fluss.rpc.metrics.BoltOnMetricNames;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.ResourceType;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;

/**
 * Kafka-flavoured bolt-on metric group. Adds Kafka-protocol-aware entity helpers on top of the
 * canonical {@link BoltOnMetricGroup} shape: per-topic bytes/in-out, per-consumer-group
 * join/heartbeat/member stats, and dimensioned auth/authz counters.
 */
@Internal
public final class KafkaMetricGroup extends BoltOnMetricGroup {

    public static final String SUBSYSTEM = "kafka";

    /**
     * Whether per-topic metrics are enabled. When {@code false}, {@link #topicMetrics} returns
     * null.
     */
    private final boolean perTopicEnabled;

    /**
     * Whether per-consumer-group metrics are enabled. When {@code false}, {@link #groupMetrics}
     * returns null.
     */
    private final boolean perGroupEnabled;

    /**
     * Tracks which consumer-group ids have had their {@code memberCount} gauge registered already,
     * so we register each gauge at most once even across concurrent JoinGroup races.
     */
    private final ConcurrentMap<String, Boolean> memberCountRegistered = new ConcurrentHashMap<>();

    public KafkaMetricGroup(
            MetricRegistry registry,
            AbstractMetricGroup parent,
            int topicMaxCardinality,
            int groupMaxCardinality) {
        this(
                registry,
                parent,
                topicMaxCardinality > 0,
                topicMaxCardinality,
                groupMaxCardinality > 0,
                groupMaxCardinality);
    }

    public KafkaMetricGroup(
            MetricRegistry registry,
            AbstractMetricGroup parent,
            boolean perTopicEnabled,
            int topicMaxCardinality,
            boolean perGroupEnabled,
            int groupMaxCardinality) {
        super(
                registry,
                parent,
                SUBSYSTEM,
                perTopicEnabled ? topicMaxCardinality : 0,
                perGroupEnabled ? groupMaxCardinality : 0);
        this.perTopicEnabled = perTopicEnabled;
        this.perGroupEnabled = perGroupEnabled;
    }

    @Override
    protected String subsystemName() {
        return SUBSYSTEM;
    }

    public boolean isPerTopicEnabled() {
        return perTopicEnabled;
    }

    public boolean isPerGroupEnabled() {
        return perGroupEnabled;
    }

    // ---- per-topic ------------------------------------------------------

    @Nullable
    public SessionEntityMetricGroup topicMetrics(String topic) {
        if (!perTopicEnabled) {
            return null;
        }
        return entityGroup("topic", topic);
    }

    public void recordProduce(String topic, long bytes, long records, boolean error) {
        recordBytesIn(bytes, records);
        SessionEntityMetricGroup g = topicMetrics(topic);
        if (g != null) {
            g.onBytesIn(bytes, records);
            g.onOperation();
            if (error) {
                g.onProduceError();
            }
        }
    }

    public void recordFetch(String topic, long bytes, boolean error) {
        recordBytesOut(bytes);
        SessionEntityMetricGroup g = topicMetrics(topic);
        if (g != null) {
            g.onBytesOut(bytes);
            g.onOperation();
            if (error) {
                g.onFetchError();
            }
        }
    }

    // ---- per-consumer-group --------------------------------------------

    @Nullable
    public SessionEntityMetricGroup groupMetrics(String groupId) {
        if (!perGroupEnabled) {
            return null;
        }
        return clientGroup("group", groupId);
    }

    public void recordJoin(String groupId, long joinLatencyMs) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            g.onRebalance(joinLatencyMs);
        }
    }

    public void recordHeartbeat(String groupId, boolean error) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            g.onHeartbeat();
            if (error) {
                g.onError();
            }
        }
    }

    public void recordOffsetCommit(String groupId, boolean error) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            g.onOffsetCommit();
            if (error) {
                g.onError();
            }
        }
    }

    /**
     * Idempotently register a {@code memberCount} gauge on the per-group sub-group. Called once per
     * consumer-group on its first {@code JoinGroup}. The gauge reads live state from {@code
     * memberCountSupplier} every poll, so no per-heartbeat write is needed.
     */
    public void registerMemberCountGauge(
            String groupId, ToIntFunction<String> memberCountSupplier) {
        if (!perGroupEnabled || groupId == null || groupId.isEmpty()) {
            return;
        }
        if (memberCountRegistered.putIfAbsent(groupId, Boolean.TRUE) != null) {
            return;
        }
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.registerGauge(
                    BoltOnMetricNames.MEMBER_COUNT,
                    (Gauge<Integer>) () -> memberCountSupplier.applyAsInt(groupId));
        }
    }

    // ---- auth / authz helpers ------------------------------------------

    /** Record a SASL outcome. Mechanism is a free-text tag (e.g. {@code PLAIN}, {@code SCRAM}). */
    public void onSaslOutcome(boolean ok, @Nullable String mechanism) {
        if (ok) {
            onAuthSuccess();
        } else {
            onAuthFailure();
        }
        if (mechanism != null && !mechanism.isEmpty()) {
            SessionEntityMetricGroup g =
                    clientGroup("sasl_mechanism", mechanism.toUpperCase(Locale.ROOT));
            if (g != null) {
                g.onOperation();
                if (!ok) {
                    g.onError();
                }
            }
        }
    }

    /** Record an authz outcome. {@code op} / {@code rt} may be null when not known. */
    public void onAuthzOutcome(
            boolean allowed, @Nullable OperationType op, @Nullable ResourceType rt) {
        if (allowed) {
            onAuthzAllow();
        } else {
            onAuthzDeny();
        }
        if (op != null && rt != null) {
            String dim = allowed ? "authz_allow_scope" : "authz_deny_scope";
            SessionEntityMetricGroup g = clientGroup(dim, op.name() + "/" + rt.name());
            if (g != null) {
                g.onOperation();
            }
        }
    }
}
