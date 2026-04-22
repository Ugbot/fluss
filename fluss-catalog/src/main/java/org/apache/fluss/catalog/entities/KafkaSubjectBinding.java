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

package org.apache.fluss.catalog.entities;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * One row in {@code _catalog.__kafka_bindings__}. The catalog doesn't care about Confluent subject
 * vocabulary; only the Kafka SR projection does. Keeping bindings as a separate entity keeps
 * Kafka-specific strings out of the core {@link CatalogTableEntity}.
 */
@PublicEvolving
public final class KafkaSubjectBinding {

    private final String subject;
    private final String tableId;
    private final String keyOrValue;
    private final String namingStrategy;

    public KafkaSubjectBinding(
            String subject, String tableId, String keyOrValue, String namingStrategy) {
        this.subject = subject;
        this.tableId = tableId;
        this.keyOrValue = keyOrValue;
        this.namingStrategy = namingStrategy;
    }

    public String subject() {
        return subject;
    }

    public String tableId() {
        return tableId;
    }

    /** {@code value} or {@code key}. */
    public String keyOrValue() {
        return keyOrValue;
    }

    public String namingStrategy() {
        return namingStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaSubjectBinding)) {
            return false;
        }
        return subject.equals(((KafkaSubjectBinding) o).subject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject);
    }
}
