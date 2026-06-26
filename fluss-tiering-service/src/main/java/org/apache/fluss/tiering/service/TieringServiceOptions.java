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

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.lake.tiering.reader.TieringTableReader;

import java.time.Duration;

import static org.apache.fluss.config.ConfigBuilder.key;

/**
 * Configuration options for the standalone {@link TieringService} daemon.
 *
 * <p>These options control the daemon's heartbeat / table-request cadence, the per-table log-poll
 * timeout and the worker parallelism. The Fluss client bootstrap configuration ({@code
 * bootstrap.servers}, etc.) and the lake {@code dataLakeConfig} block (S3 endpoint / region /
 * credentials, Iceberg catalog props) are passed through verbatim and are not redefined here.
 */
public class TieringServiceOptions {

    /**
     * Prefix for the lake-specific configuration block. The daemon extracts the {@code
     * datalake.<format>.*} keys and forwards them (with the prefix stripped) to {@code
     * LakeStorage}'s {@code createLakeStorage}. Mirrors {@code
     * TieringSourceOptions#DATA_LAKE_CONFIG_PREFIX}.
     */
    public static final String DATA_LAKE_CONFIG_PREFIX = "datalake.";

    /**
     * Prefix for keys forwarded verbatim to the lake committer as the lake-tiering config block.
     */
    public static final String LAKE_TIERING_CONFIG_PREFIX = "lake.tiering.";

    /**
     * The fixed interval to request a tiering table from / heartbeat the Fluss cluster.
     *
     * <p>This MUST stay well below {@code LakeTableTieringManager.TIERING_SERVICE_TIMEOUT_MS} (2
     * minutes); otherwise the coordinator's {@code checkTieringServiceTimeout} flips an in-flight
     * table back to {@code Pending}. The key intentionally matches the Flink connector's existing
     * {@code tiering.poll.table.interval} so operators can reuse the same configuration.
     */
    public static final ConfigOption<Duration> POLL_INTERVAL =
            key("tiering.poll.table.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The fixed interval at which the standalone tiering service requests a "
                                    + "tiering table from and heartbeats the Fluss coordinator. Must stay "
                                    + "well under the coordinator's 2 minute tiering-service timeout so "
                                    + "in-flight tables stay alive. Defaults to 30 seconds.");

    /**
     * The maximum number of tables the daemon tiers concurrently, i.e. the {@link
     * TieringWorkerPool} size. Defaults to {@code 1} to preserve the enumerator's
     * one-table-at-a-time property.
     */
    public static final ConfigOption<Integer> MAX_CONCURRENT_TABLES =
            key("tiering.max-concurrent-tables")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The maximum number of tables the tiering service tiers concurrently "
                                    + "(the worker pool size). Each worker requests its own table, "
                                    + "preserving the single-table-per-request property. Defaults to 1.");

    /**
     * The timeout for a single {@code LogScanner.poll} while tiering a table's log splits. Defaults
     * to {@link TieringTableReader#DEFAULT_POLL_TIMEOUT} (10 seconds).
     */
    public static final ConfigOption<Duration> POLL_TIMEOUT =
            key("tiering.poll.timeout")
                    .durationType()
                    .defaultValue(TieringTableReader.DEFAULT_POLL_TIMEOUT)
                    .withDescription(
                            "The timeout for a single log poll while tiering a table's log splits. "
                                    + "Defaults to 10 seconds.");

    private TieringServiceOptions() {}
}
