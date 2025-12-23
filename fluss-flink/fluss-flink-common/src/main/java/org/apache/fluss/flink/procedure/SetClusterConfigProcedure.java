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

import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * Procedure to set or delete cluster configuration dynamically.
 *
 * <p>This procedure allows modifying dynamic cluster configurations. The changes are:
 *
 * <ul>
 *   <li>Validated by the CoordinatorServer before persistence
 *   <li>Persisted in ZooKeeper for durability
 *   <li>Applied to all relevant servers (Coordinator and TabletServers)
 *   <li>Survive server restarts
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Set a configuration
 * CALL sys.set_cluster_config('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '200MB');
 * CALL sys.set_cluster_config('datalake.format', 'paimon');
 *
 * -- Delete a configuration (reset to default)
 * CALL sys.set_cluster_config('kv.rocksdb.shared-rate-limiter.bytes-per-sec', NULL);
 * CALL sys.set_cluster_config('kv.rocksdb.shared-rate-limiter.bytes-per-sec', '');
 * </pre>
 *
 * <p><b>Note:</b> Not all configurations support dynamic changes. The server will validate the
 * change and reject it if the configuration cannot be modified dynamically or if the new value is
 * invalid.
 */
public class SetClusterConfigProcedure extends ProcedureBase {

    @ProcedureHint(argument = {@ArgumentHint(name = "config_key", type = @DataTypeHint("STRING"))})
    public String[] call(ProcedureContext context, String configKey) throws Exception {
        return performSet(configKey, null);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "config_key", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "config_value", type = @DataTypeHint("STRING"))
            })
    public String[] call(ProcedureContext context, String configKey, String configValue)
            throws Exception {
        return performSet(configKey, configValue);
    }

    private String[] performSet(String configKey, @Nullable String configValue) throws Exception {

        try {
            // Validate config key
            if (configKey == null || configKey.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Config key cannot be null or empty. "
                                + "Please specify a valid configuration key.");
            }

            configKey = configKey.trim();

            // Determine operation type
            AlterConfigOpType opType;
            String operationDesc;

            if (configValue == null || configValue.trim().isEmpty()) {
                // Delete operation - reset to default
                opType = AlterConfigOpType.DELETE;
                operationDesc = "deleted (reset to default)";
            } else {
                // Set operation
                opType = AlterConfigOpType.SET;
                operationDesc = String.format("set to '%s'", configValue);
            }

            // Construct configuration modification operation.
            AlterConfig alterConfig = new AlterConfig(configKey, configValue, opType);

            // Call Admin API to modify cluster configuration
            // This will trigger validation on CoordinatorServer before persistence
            admin.alterClusterConfigs(Collections.singletonList(alterConfig)).get();

            return new String[] {
                String.format(
                        "Successfully %s configuration '%s'. "
                                + "The change is persisted in ZooKeeper and applied to all servers.",
                        operationDesc, configKey)
            };

        } catch (IllegalArgumentException e) {
            // Re-throw validation errors with original message
            throw e;
        } catch (Exception e) {
            // Wrap other exceptions with more context
            throw new RuntimeException(
                    String.format(
                            "Failed to set cluster config '%s': %s", configKey, e.getMessage()),
                    e);
        }
    }
}
