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

package org.apache.fluss.kafka.catalog;

import org.apache.fluss.annotation.Internal;

import java.util.List;
import java.util.Optional;

/**
 * Read/write interface to Kafka topic metadata. Implementations own where topic bindings + Kafka
 * configs are persisted; callers see a topic name -> {@link KafkaTopicInfo} lookup interface.
 *
 * <p>The Phase-2A implementation persists topic metadata as Fluss custom properties on the data
 * table ({@link CustomPropertiesTopicsCatalog}). A follow-up will swap in a dedicated PK table
 * (`kafka.__kafka_topics__`) so Produce/Fetch hot paths can use a single-roundtrip lookup client
 * and avoid the listTables+filter scan done by the properties-backed impl.
 */
@Internal
public interface KafkaTopicsCatalog {

    /** Register a newly created Kafka topic. Called after the data table has been created. */
    void register(KafkaTopicInfo info) throws Exception;

    /** Remove the binding. Called before the data table is dropped. */
    void deregister(String topic) throws Exception;

    /** Look up a topic by name. Returns empty if no binding exists. */
    Optional<KafkaTopicInfo> lookup(String topic) throws Exception;

    /** Enumerate every known topic binding. Used for the Metadata "all topics" path. */
    List<KafkaTopicInfo> listAll() throws Exception;
}
