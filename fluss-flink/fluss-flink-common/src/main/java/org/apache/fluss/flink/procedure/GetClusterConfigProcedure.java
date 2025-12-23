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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.config.cluster.ConfigEntry;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Procedure to get cluster configuration(s).
 *
 * <p>This procedure allows querying dynamic cluster configurations. It can retrieve:
 *
 * <ul>
 *   <li>A specific configuration key
 *   <li>All configurations (when key parameter is null or empty)
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Get a specific configuration
 * CALL sys.get_cluster_config('kv.rocksdb.shared-rate-limiter.bytes-per-sec');
 *
 * -- Get all cluster configurations
 * CALL sys.get_cluster_config();
 * </pre>
 */
public class GetClusterConfigProcedure extends ProcedureBase {

    @ProcedureHint(
            output =
                    @DataTypeHint(
                            "ROW<config_key STRING, config_value STRING, config_source STRING>"))
    public Row[] call(ProcedureContext context) throws Exception {
        return getConfigs(null);
    }

    @ProcedureHint(
            argument = {@ArgumentHint(name = "config_key", type = @DataTypeHint("STRING"))},
            output =
                    @DataTypeHint(
                            "ROW<config_key STRING, config_value STRING, config_source STRING>"))
    public Row[] call(ProcedureContext context, String configKey) throws Exception {
        return getConfigs(configKey);
    }

    private Row[] getConfigs(@Nullable String configKey) throws Exception {
        try {
            // Get all cluster configurations
            Collection<ConfigEntry> configs = admin.describeClusterConfigs().get();

            List<Row> results = new ArrayList<>();

            if (configKey == null || configKey.isEmpty()) {
                // Return all configurations
                for (ConfigEntry entry : configs) {
                    results.add(
                            Row.of(
                                    entry.key(),
                                    entry.value(),
                                    entry.source() != null ? entry.source().name() : "UNKNOWN"));
                }
            } else {
                // Find specific configuration
                for (ConfigEntry entry : configs) {
                    if (entry.key().equals(configKey)) {
                        results.add(
                                Row.of(
                                        entry.key(),
                                        entry.value(),
                                        entry.source() != null
                                                ? entry.source().name()
                                                : "UNKNOWN"));
                        break;
                    }
                }
            }

            return results.toArray(new Row[0]);

        } catch (Exception e) {
            throw new RuntimeException("Failed to get cluster config: " + e.getMessage(), e);
        }
    }
}
