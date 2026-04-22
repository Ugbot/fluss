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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;

/**
 * Custom-property keys a Kafka-bound Fluss table carries once it has been associated with a Schema
 * Registry subject. Phase A1 stores the Avro schema text as a custom property — the "Fluss-first,
 * no stored Avro text" rule from design 0002 kicks in at Phase A2 once the Avro→Fluss translator is
 * real.
 */
@Internal
public final class SrTableProperties {

    private SrTableProperties() {}

    /** Canonical subject this table is bound to (e.g. {@code orders-value}). */
    public static final String SUBJECT = "kafka.sr.subject";

    /** Wire format. Phase A1 only supports {@code AVRO}. */
    public static final String FORMAT = "kafka.sr.format";

    /** Raw submitted Avro schema text. Phase A1 caches this verbatim; Phase A2 re-derives it. */
    public static final String AVRO_SCHEMA = "kafka.sr.avro-schema";

    /** Integer version label surfaced to Confluent REST; always 1 in Phase A1. */
    public static final String SCHEMA_VERSION = "kafka.sr.schema-version";

    /** The Confluent global id assigned for this table's current schema. */
    public static final String CONFLUENT_ID = "kafka.sr.confluent-id";

    public static final String FORMAT_AVRO = "AVRO";
}
