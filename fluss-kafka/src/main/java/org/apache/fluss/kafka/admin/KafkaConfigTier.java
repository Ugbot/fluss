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

/**
 * The three buckets a Kafka topic-level config entry can fall into when reflected onto Fluss' table
 * model. Drives {@code DescribeConfigs} / {@code AlterConfigs} dispatch (see plan §28.2).
 */
@Internal
public enum KafkaConfigTier {

    /**
     * Reported on {@code DescribeConfigs} and translated to a real Fluss table property that the
     * runtime honours. Examples: {@code retention.ms} → {@code table.log.ttl}; {@code
     * cleanup.policy} drives PK-vs-log schema at create-time.
     */
    MAPPED,

    /**
     * Reported on {@code DescribeConfigs} and round-tripped on {@code AlterConfigs}, but the value
     * isn't actually enforced by the Fluss runtime — it lives in the table's {@code
     * customProperties} map so external tools see a consistent round-trip. Documented as "reported,
     * not enforced".
     */
    STORED,

    /**
     * Reported with Kafka's documented default and {@code isReadOnly=true}; {@code AlterConfigs}
     * returns {@code INVALID_CONFIG_VALUE} regardless of the supplied value. Used for knobs whose
     * semantics contradict Fluss's runtime (e.g. {@code preallocate}, {@code
     * unclean.leader.election.enable}).
     */
    READONLY_DEFAULT
}
