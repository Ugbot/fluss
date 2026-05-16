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

package org.apache.fluss.kafka.fetch;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Locale;

/**
 * Per-topic Kafka hot-path routing decision (design 0014 §3). The Produce and Fetch transcoders
 * resolve a {@link KafkaTopicRoute} once per call from the catalog row's {@code format}, then use
 * {@link #kind} to choose between the byte-copy passthrough path and the typed (compiled codec)
 * path.
 */
@Internal
public final class KafkaTopicRoute {

    /** Catalog format constant for Kafka byte-copy passthrough topics. */
    public static final String FORMAT_PASSTHROUGH = "KAFKA_PASSTHROUGH";

    /** Catalog format constant for Kafka typed-Avro topics. */
    public static final String FORMAT_TYPED_AVRO = "KAFKA_TYPED_AVRO";

    /** Catalog format constant for Kafka typed-JSON topics. */
    public static final String FORMAT_TYPED_JSON = "KAFKA_TYPED_JSON";

    /** Catalog format constant for Kafka typed-Protobuf topics. */
    public static final String FORMAT_TYPED_PROTOBUF = "KAFKA_TYPED_PROTOBUF";

    /** Singleton passthrough route — the default for every topic when the feature flag is off. */
    public static final KafkaTopicRoute PASSTHROUGH =
            new KafkaTopicRoute(Kind.PASSTHROUGH, null, null);

    /** Which hot path to take for a topic. */
    public enum Kind {
        PASSTHROUGH,
        TYPED_AVRO,
        TYPED_JSON,
        TYPED_PROTOBUF
    }

    private final Kind kind;
    private final @Nullable String formatId;
    private final @Nullable String catalogFormat;

    private KafkaTopicRoute(Kind kind, @Nullable String formatId, @Nullable String catalogFormat) {
        this.kind = kind;
        this.formatId = formatId;
        this.catalogFormat = catalogFormat;
    }

    public Kind kind() {
        return kind;
    }

    public boolean isTyped() {
        return kind != Kind.PASSTHROUGH;
    }

    /** {@code "AVRO"} / {@code "JSON"} / {@code "PROTOBUF"}; {@code null} for passthrough. */
    @Nullable
    public String formatId() {
        return formatId;
    }

    /** The verbatim catalog {@code format} string this route was derived from. */
    @Nullable
    public String catalogFormat() {
        return catalogFormat;
    }

    /**
     * Resolve a route from a catalog {@code __tables__.format} string. {@code null}, the empty
     * string, an unknown value, or {@link #FORMAT_PASSTHROUGH} all map to {@link #PASSTHROUGH}; the
     * three typed format strings map to the matching {@link Kind}. Case-insensitive.
     */
    public static KafkaTopicRoute fromCatalogFormat(@Nullable String catalogFormat) {
        if (catalogFormat == null || catalogFormat.isEmpty()) {
            return PASSTHROUGH;
        }
        String upper = catalogFormat.toUpperCase(Locale.ROOT);
        switch (upper) {
            case FORMAT_TYPED_AVRO:
                return new KafkaTopicRoute(Kind.TYPED_AVRO, "AVRO", upper);
            case FORMAT_TYPED_JSON:
                return new KafkaTopicRoute(Kind.TYPED_JSON, "JSON", upper);
            case FORMAT_TYPED_PROTOBUF:
                return new KafkaTopicRoute(Kind.TYPED_PROTOBUF, "PROTOBUF", upper);
            case FORMAT_PASSTHROUGH:
            default:
                return PASSTHROUGH;
        }
    }
}
