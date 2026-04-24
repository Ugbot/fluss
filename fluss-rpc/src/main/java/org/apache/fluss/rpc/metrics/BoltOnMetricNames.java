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
import org.apache.fluss.metrics.MetricNames;

/**
 * Metric-name constants shared by every bolt-on. Reuses {@link MetricNames} where the semantics
 * match the Fluss-native convention so dashboards can share queries.
 */
@Internal
public final class BoltOnMetricNames {

    // Connection-lifecycle.
    public static final String ACTIVE_CONNECTIONS = "activeConnections";
    public static final String CONNECTIONS_CREATED_RATE = "connectionsCreatedPerSecond";
    public static final String CONNECTIONS_CLOSED_RATE = "connectionsClosedPerSecond";

    // Request-aggregate — names intentionally match Fluss-native request metric names.
    public static final String REQUESTS_RATE = MetricNames.REQUESTS_RATE;
    public static final String ERRORS_RATE = MetricNames.ERRORS_RATE;
    public static final String REQUEST_PROCESS_TIME_MS = MetricNames.REQUEST_PROCESS_TIME_MS;

    // Byte / message rates — matches Fluss-native user-scope metrics.
    public static final String BYTES_IN_RATE = MetricNames.BYTES_IN_RATE;
    public static final String BYTES_OUT_RATE = MetricNames.BYTES_OUT_RATE;
    public static final String MESSAGES_IN_RATE = MetricNames.MESSAGES_IN_RATE;

    // Entity-group operation rate — lightweight op counter separate from {@link #REQUESTS_RATE}.
    public static final String OPERATIONS_RATE = "operationsPerSecond";

    // Auth / authz.
    public static final String AUTH_SUCCESS_RATE = "authSuccessPerSecond";
    public static final String AUTH_FAILURE_RATE = "authFailurePerSecond";
    public static final String AUTHZ_ALLOW_RATE = "authzAllowPerSecond";
    public static final String AUTHZ_DENY_RATE = "authzDenyPerSecond";

    private BoltOnMetricNames() {}
}
