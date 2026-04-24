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
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.metrics.BoltOnMetricGroup;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.ResourceType;

import javax.annotation.Nullable;

import java.util.Locale;

/**
 * Kafka-flavoured bolt-on metric group. Adds Kafka-protocol-aware entity helpers on top of the
 * canonical {@link BoltOnMetricGroup} shape: per-topic bytes/in-out, per-consumer-group
 * join/heartbeat/member stats, and dimensioned auth/authz counters.
 */
@Internal
public final class KafkaMetricGroup extends BoltOnMetricGroup {

    public static final String SUBSYSTEM = "kafka";

    public KafkaMetricGroup(
            MetricRegistry registry,
            AbstractMetricGroup parent,
            int topicMaxCardinality,
            int groupMaxCardinality) {
        super(registry, parent, SUBSYSTEM, topicMaxCardinality, groupMaxCardinality);
    }

    @Override
    protected String subsystemName() {
        return SUBSYSTEM;
    }

    // ---- per-topic ------------------------------------------------------

    @Nullable
    public SessionEntityMetricGroup topicMetrics(String topic) {
        return entityGroup("topic", topic);
    }

    public void recordProduce(String topic, long bytes, long records, boolean error) {
        recordBytesIn(bytes, records);
        SessionEntityMetricGroup g = topicMetrics(topic);
        if (g != null) {
            g.onBytesIn(bytes, records);
            g.onOperation();
            if (error) {
                g.onError();
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
                g.onError();
            }
        }
    }

    // ---- per-consumer-group --------------------------------------------

    @Nullable
    public SessionEntityMetricGroup groupMetrics(String groupId) {
        return clientGroup("group", groupId);
    }

    public void recordJoin(String groupId, long joinLatencyMs) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            g.onBytesIn(0, 0);
            // Reuse the shared process-time histogram metric slot — readers look up by name.
            // The join-latency isn't a bytes count, so recorded via the shared op counter only.
        }
    }

    public void recordHeartbeat(String groupId, boolean error) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            if (error) {
                g.onError();
            }
        }
    }

    public void recordOffsetCommit(String groupId, boolean error) {
        SessionEntityMetricGroup g = groupMetrics(groupId);
        if (g != null) {
            g.onOperation();
            if (error) {
                g.onError();
            }
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
