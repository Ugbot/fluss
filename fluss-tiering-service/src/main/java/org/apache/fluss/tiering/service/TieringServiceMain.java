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

package org.apache.fluss.tiering.service;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.plugin.PluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.fluss.tiering.service.TieringServiceOptions.DATA_LAKE_CONFIG_PREFIX;
import static org.apache.fluss.tiering.service.TieringServiceOptions.LAKE_TIERING_CONFIG_PREFIX;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
import static org.apache.fluss.utils.PropertiesUtils.extractPrefix;

/**
 * The command-line entrypoint for the standalone {@link TieringService} daemon.
 *
 * <p>Configuration is supplied as {@code --key value} (or {@code key=value}) pairs, mirroring how
 * the Flink tiering entrypoint ({@code FlussLakeTiering}) parses its parameters. The parameters are
 * split into three configuration blocks:
 *
 * <ul>
 *   <li><b>Fluss client config</b> — keys prefixed with {@code fluss.} (e.g. {@code
 *       fluss.bootstrap.servers}). The {@code fluss.} prefix is stripped. {@code bootstrap.servers}
 *       is mandatory. The daemon-specific options ({@code tiering.poll.table.interval}, {@code
 *       tiering.max-concurrent-tables}, {@code tiering.poll.timeout}) live in this block too.
 *   <li><b>Lake config</b> — keys prefixed with {@code datalake.<format>.} (e.g. {@code
 *       datalake.iceberg.warehouse}). The prefix is stripped and the remainder forwarded verbatim
 *       to {@code LakeStorage}'s {@code createLakeStorage} (S3 endpoint / region / credentials,
 *       catalog props).
 *   <li><b>Lake-tiering config</b> — keys prefixed with {@code lake.tiering.}, forwarded to the
 *       lake committer.
 * </ul>
 *
 * <p>The selected lake format is taken from the {@code datalake.format} parameter (the existing
 * cluster option {@link ConfigOptions#DATALAKE_FORMAT}).
 */
public class TieringServiceMain {

    private static final Logger LOG = LoggerFactory.getLogger(TieringServiceMain.class);

    private static final String FLUSS_CONF_PREFIX = "fluss.";

    private TieringServiceMain() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);

        // ---- fluss client config (includes the tiering.* daemon options) ----
        Map<String, String> flussConfigMap = extractAndRemovePrefix(params, FLUSS_CONF_PREFIX);
        String bootstrapServers = flussConfigMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The Fluss bootstrap servers are not configured; please pass %s.",
                            FLUSS_CONF_PREFIX + ConfigOptions.BOOTSTRAP_SERVERS.key()));
        }
        Configuration flussConfig = Configuration.fromMap(flussConfigMap);

        // ---- data lake format selection ----
        String dataLakeFormat = params.get(ConfigOptions.DATALAKE_FORMAT.key());
        if (dataLakeFormat == null || dataLakeFormat.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    ConfigOptions.DATALAKE_FORMAT.key() + " is not configured.");
        }

        // ---- lake config: datalake.<format>.* (prefix stripped) ----
        Map<String, String> lakeConfigMap =
                extractAndRemovePrefix(
                        params, String.format("%s%s.", DATA_LAKE_CONFIG_PREFIX, dataLakeFormat));
        Configuration dataLakeConfig = Configuration.fromMap(lakeConfigMap);

        // ---- lake-tiering config: lake.tiering.* ----
        Map<String, String> lakeTieringConfigMap =
                extractPrefix(params, LAKE_TIERING_CONFIG_PREFIX);
        Configuration lakeTieringConfig = Configuration.fromMap(lakeTieringConfigMap);

        // the plugin manager loads lake implementations from the plugins/ directory at runtime.
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(flussConfig);

        TieringService service =
                new TieringService(
                        flussConfig,
                        dataLakeConfig,
                        lakeTieringConfig,
                        dataLakeFormat,
                        pluginManager);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOG.info("Shutdown signal received; stopping tiering service.");
                                    try {
                                        service.close();
                                    } catch (Throwable t) {
                                        LOG.error("Error while closing tiering service.", t);
                                    } finally {
                                        shutdownLatch.countDown();
                                    }
                                },
                                "fluss-tiering-shutdown-hook"));

        try {
            service.start();
        } catch (Throwable t) {
            LOG.error("Failed to start tiering service.", t);
            try {
                service.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            throw t;
        }

        LOG.info(
                "Tiering service for data lake format '{}' is running; awaiting shutdown.",
                dataLakeFormat);
        // block the main thread until the JVM is asked to shut down.
        shutdownLatch.await();
        LOG.info("Tiering service main thread exiting.");
    }

    /**
     * Parses {@code --key value}, {@code -key value} and {@code key=value} CLI argument forms into
     * a flat parameter map. Mirrors the parameter forms accepted by the Flink entrypoint without
     * pulling in any Flink parameter-tool dependency.
     */
    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (arg == null || arg.trim().isEmpty()) {
                i++;
                continue;
            }
            if (arg.startsWith("--") || arg.startsWith("-")) {
                String key = stripLeadingDashes(arg);
                int eq = key.indexOf('=');
                if (eq >= 0) {
                    // --key=value
                    params.put(key.substring(0, eq), key.substring(eq + 1));
                    i++;
                } else if (i + 1 < args.length && !isFlag(args[i + 1])) {
                    // --key value
                    params.put(key, args[i + 1]);
                    i += 2;
                } else {
                    // valueless flag -> empty string
                    params.put(key, "");
                    i++;
                }
            } else {
                int eq = arg.indexOf('=');
                if (eq >= 0) {
                    // key=value
                    params.put(arg.substring(0, eq), arg.substring(eq + 1));
                } else {
                    params.put(arg, "");
                }
                i++;
            }
        }
        return params;
    }

    private static boolean isFlag(String arg) {
        return arg != null && (arg.startsWith("--") || arg.startsWith("-"));
    }

    private static String stripLeadingDashes(String arg) {
        int start = 0;
        while (start < arg.length() && arg.charAt(start) == '-') {
            start++;
        }
        return arg.substring(start);
    }
}
