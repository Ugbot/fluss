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

package org.apache.fluss.rpc.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;

/**
 * Canonical metric surface shared by every Fluss bolt-on (Kafka, Iceberg REST, Flink catalog
 * bridge). Keeps the observability story uniform so operators get the same connection / request /
 * auth / authz views regardless of which bolt-on a request came in on.
 *
 * <p>Subclasses supply a subsystem name (e.g. {@code "kafka"}) and obtain three lazy sub-group
 * factories — per API, per topic-like session entity, and per client-group-like session entity —
 * each with a cardinality cap to prevent runaway metric growth on high-churn deployments.
 */
@Internal
public abstract class BoltOnMetricGroup extends AbstractMetricGroup {

    protected static final int DEFAULT_HISTOGRAM_WINDOW = 1024;
    protected static final Logger LOG = LoggerFactory.getLogger(BoltOnMetricGroup.class);

    // ---- connection-level -------------------------------------------------
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final Counter connectionsCreated = new ThreadSafeSimpleCounter();
    private final Counter connectionsClosed = new ThreadSafeSimpleCounter();

    // ---- request-level aggregates ----------------------------------------
    private final Counter requests = new ThreadSafeSimpleCounter();
    private final Counter errors = new ThreadSafeSimpleCounter();
    private final Counter bytesIn = new ThreadSafeSimpleCounter();
    private final Counter bytesOut = new ThreadSafeSimpleCounter();
    private final Counter messagesIn = new ThreadSafeSimpleCounter();
    private final Histogram requestProcessingTime =
            new DescriptiveStatisticsHistogram(DEFAULT_HISTOGRAM_WINDOW);

    // ---- auth / authz ----------------------------------------------------
    private final Counter authSuccess = new ThreadSafeSimpleCounter();
    private final Counter authFailure = new ThreadSafeSimpleCounter();
    private final Counter authzAllow = new ThreadSafeSimpleCounter();
    private final Counter authzDeny = new ThreadSafeSimpleCounter();

    // ---- lazy sub-groups -------------------------------------------------
    private final ConcurrentMap<String, ApiMetricGroup> apiGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SessionEntityMetricGroup> entityGroups =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SessionEntityMetricGroup> clientGroups =
            new ConcurrentHashMap<>();
    private volatile boolean entityOverflowLogged;
    private volatile boolean clientOverflowLogged;

    private final int entityMaxCardinality;
    private final int clientMaxCardinality;

    protected BoltOnMetricGroup(
            MetricRegistry registry,
            AbstractMetricGroup parent,
            String subsystemName,
            int entityMaxCardinality,
            int clientMaxCardinality) {
        super(registry, makeScope(parent, subsystemName), parent);
        this.entityMaxCardinality = Math.max(1, entityMaxCardinality);
        this.clientMaxCardinality = Math.max(1, clientMaxCardinality);

        gauge(BoltOnMetricNames.ACTIVE_CONNECTIONS, (Gauge<Integer>) activeConnections::get);
        meter(BoltOnMetricNames.CONNECTIONS_CREATED_RATE, new MeterView(connectionsCreated));
        meter(BoltOnMetricNames.CONNECTIONS_CLOSED_RATE, new MeterView(connectionsClosed));

        meter(BoltOnMetricNames.REQUESTS_RATE, new MeterView(requests));
        meter(BoltOnMetricNames.ERRORS_RATE, new MeterView(errors));
        meter(BoltOnMetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        meter(BoltOnMetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
        meter(BoltOnMetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
        histogram(BoltOnMetricNames.REQUEST_PROCESS_TIME_MS, requestProcessingTime);

        meter(BoltOnMetricNames.AUTH_SUCCESS_RATE, new MeterView(authSuccess));
        meter(BoltOnMetricNames.AUTH_FAILURE_RATE, new MeterView(authFailure));
        meter(BoltOnMetricNames.AUTHZ_ALLOW_RATE, new MeterView(authzAllow));
        meter(BoltOnMetricNames.AUTHZ_DENY_RATE, new MeterView(authzDeny));
    }

    /** Subsystem name (lowercase, single token). Drives the metric scope and logger. */
    protected abstract String subsystemName();

    @Override
    protected final String getGroupName(CharacterFilter filter) {
        return subsystemName();
    }

    // ---- top-level event helpers -----------------------------------------

    public void onConnectionOpened() {
        activeConnections.incrementAndGet();
        connectionsCreated.inc();
    }

    public void onConnectionClosed() {
        activeConnections.updateAndGet(prev -> Math.max(0, prev - 1));
        connectionsClosed.inc();
    }

    /** Report one completed request through the bolt-on. Safe to call on hot paths. */
    public void onRequest(String apiName, long elapsedNanos, boolean isError) {
        requests.inc();
        if (isError) {
            errors.inc();
        }
        long elapsedMs = Math.max(0L, elapsedNanos / 1_000_000L);
        requestProcessingTime.update(elapsedMs);

        ApiMetricGroup api = apiGroup(apiName);
        if (api != null) {
            api.onRequest(elapsedMs, isError);
        }
    }

    public void onAuthSuccess() {
        authSuccess.inc();
    }

    public void onAuthFailure() {
        authFailure.inc();
    }

    public void onAuthzAllow() {
        authzAllow.inc();
    }

    public void onAuthzDeny() {
        authzDeny.inc();
    }

    /** Report produce-side bytes for aggregation across the bolt-on. */
    public void recordBytesIn(long bytes, long records) {
        if (bytes > 0) {
            bytesIn.inc(bytes);
        }
        if (records > 0) {
            messagesIn.inc(records);
        }
    }

    /** Report fetch-side bytes for aggregation across the bolt-on. */
    public void recordBytesOut(long bytes) {
        if (bytes > 0) {
            bytesOut.inc(bytes);
        }
    }

    // ---- lazy sub-groups -------------------------------------------------

    /**
     * Returns (or lazily creates) the per-API sub-group. API cardinality is bounded by the wire
     * protocol's fixed surface, so there is no cap here.
     */
    protected ApiMetricGroup apiGroup(String apiName) {
        if (apiName == null || apiName.isEmpty()) {
            return null;
        }
        return apiGroups.computeIfAbsent(apiName, n -> new ApiMetricGroup(registry, this, n));
    }

    /**
     * Returns (or lazily creates) a session-entity sub-group (e.g. per Kafka topic). Respects the
     * cardinality cap; names beyond the cap roll up into a shared {@code __overflow__} bucket.
     */
    @Nullable
    protected SessionEntityMetricGroup entityGroup(String dimension, String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return cappedLookup(entityGroups, entityMaxCardinality, dimension, name, "entity", false);
    }

    /**
     * Returns (or lazily creates) a client-grouping sub-group (e.g. per Kafka consumer group). Same
     * overflow-bucket behaviour as {@link #entityGroup}.
     */
    @Nullable
    protected SessionEntityMetricGroup clientGroup(String dimension, String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return cappedLookup(clientGroups, clientMaxCardinality, dimension, name, "client", true);
    }

    private SessionEntityMetricGroup cappedLookup(
            ConcurrentMap<String, SessionEntityMetricGroup> map,
            int cap,
            String dimension,
            String name,
            String flavour,
            boolean isClient) {
        SessionEntityMetricGroup existing = map.get(name);
        if (existing != null) {
            return existing;
        }
        if (map.size() >= cap) {
            if (isClient) {
                if (!clientOverflowLogged) {
                    clientOverflowLogged = true;
                    LOG.warn(
                            "{} metrics: {} cardinality cap ({}) reached; further names roll up"
                                    + " into __overflow__ sub-group.",
                            subsystemName(),
                            flavour,
                            cap);
                }
            } else if (!entityOverflowLogged) {
                entityOverflowLogged = true;
                LOG.warn(
                        "{} metrics: {} cardinality cap ({}) reached; further names roll up into"
                                + " __overflow__ sub-group.",
                        subsystemName(),
                        flavour,
                        cap);
            }
            return map.computeIfAbsent(
                    "__overflow__",
                    n -> new SessionEntityMetricGroup(registry, this, dimension, n));
        }
        return map.computeIfAbsent(
                name, n -> new SessionEntityMetricGroup(registry, this, dimension, n));
    }

    // ---- introspection (tests) ------------------------------------------

    public int activeConnectionCount() {
        return activeConnections.get();
    }

    public long requestCount() {
        return requests.getCount();
    }

    public long errorCount() {
        return errors.getCount();
    }

    public long bytesInCount() {
        return bytesIn.getCount();
    }

    public long bytesOutCount() {
        return bytesOut.getCount();
    }

    public long authSuccessCount() {
        return authSuccess.getCount();
    }

    public long authFailureCount() {
        return authFailure.getCount();
    }

    public long authzAllowCount() {
        return authzAllow.getCount();
    }

    public long authzDenyCount() {
        return authzDeny.getCount();
    }

    public Map<String, ApiMetricGroup> apiGroupsSnapshot() {
        return apiGroups;
    }

    public Map<String, SessionEntityMetricGroup> entityGroupsSnapshot() {
        return entityGroups;
    }

    public Map<String, SessionEntityMetricGroup> clientGroupsSnapshot() {
        return clientGroups;
    }

    // ---- nested sub-group types ------------------------------------------

    /** Per-API request rate + latency + error view. */
    public static class ApiMetricGroup extends AbstractMetricGroup {
        private final String apiName;
        private final Counter requests = new ThreadSafeSimpleCounter();
        private final Counter errors = new ThreadSafeSimpleCounter();
        private final Histogram processingTime =
                new DescriptiveStatisticsHistogram(DEFAULT_HISTOGRAM_WINDOW);

        ApiMetricGroup(MetricRegistry registry, BoltOnMetricGroup parent, String apiName) {
            super(registry, makeScope(parent, "api", apiName), parent);
            this.apiName = apiName;
            meter(BoltOnMetricNames.REQUESTS_RATE, new MeterView(requests));
            meter(BoltOnMetricNames.ERRORS_RATE, new MeterView(errors));
            histogram(BoltOnMetricNames.REQUEST_PROCESS_TIME_MS, processingTime);
        }

        void onRequest(long elapsedMs, boolean error) {
            requests.inc();
            if (error) {
                errors.inc();
            }
            processingTime.update(elapsedMs);
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return "api";
        }

        @Override
        protected void putVariables(Map<String, String> variables) {
            variables.put("api_name", apiName);
        }

        public long requestCount() {
            return requests.getCount();
        }

        public long errorCount() {
            return errors.getCount();
        }

        public Meter requestMeter() {
            // MeterView is the only registered metric; fetch via the map.
            Meter m = (Meter) getMetrics().get(BoltOnMetricNames.REQUESTS_RATE);
            return m;
        }
    }

    /** Per-session-entity (topic / consumer group / etc.) bytes + ops view. */
    public static class SessionEntityMetricGroup extends AbstractMetricGroup {
        private final String dimension;
        private final String name;
        private final Counter bytesIn = new ThreadSafeSimpleCounter();
        private final Counter bytesOut = new ThreadSafeSimpleCounter();
        private final Counter messagesIn = new ThreadSafeSimpleCounter();
        private final Counter operations = new ThreadSafeSimpleCounter();
        private final Counter errors = new ThreadSafeSimpleCounter();
        private final ConcurrentMap<String, Gauge<?>> customGauges = new ConcurrentHashMap<>();

        SessionEntityMetricGroup(
                MetricRegistry registry, BoltOnMetricGroup parent, String dimension, String name) {
            super(registry, makeScope(parent, dimension, name), parent);
            this.dimension = dimension;
            this.name = name;
            meter(BoltOnMetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
            meter(BoltOnMetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));
            meter(BoltOnMetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
            meter(BoltOnMetricNames.OPERATIONS_RATE, new MeterView(operations));
            meter(BoltOnMetricNames.ERRORS_RATE, new MeterView(errors));
        }

        public void onBytesIn(long bytes, long records) {
            if (bytes > 0) {
                bytesIn.inc(bytes);
            }
            if (records > 0) {
                messagesIn.inc(records);
            }
        }

        public void onBytesOut(long bytes) {
            if (bytes > 0) {
                bytesOut.inc(bytes);
            }
        }

        public void onOperation() {
            operations.inc();
        }

        public void onError() {
            errors.inc();
        }

        /** Register an external gauge under this entity group (idempotent). */
        public <T, G extends Gauge<T>> G registerGauge(String name, G gauge) {
            if (customGauges.putIfAbsent(name, gauge) == null) {
                gauge(name, gauge);
            }
            return gauge;
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return dimension;
        }

        @Override
        protected void putVariables(Map<String, String> variables) {
            variables.put(dimension, name);
        }

        public long bytesInCount() {
            return bytesIn.getCount();
        }

        public long bytesOutCount() {
            return bytesOut.getCount();
        }

        public long messagesInCount() {
            return messagesIn.getCount();
        }

        public long operationCount() {
            return operations.getCount();
        }

        public long errorCount() {
            return errors.getCount();
        }
    }
}
