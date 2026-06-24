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

package org.apache.fluss.kafka.sr.compat;

import org.apache.fluss.annotation.Internal;

import java.util.Locale;

/**
 * Kafka Schema Registry compatibility levels.
 *
 * <p>Each level controls how a proposed schema is validated against the existing version history:
 *
 * <ul>
 *   <li>{@link #NONE} — never reject.
 *   <li>{@link #BACKWARD} — new schema must be able to read data written with the <b>latest</b>
 *       prior version (consumer-first).
 *   <li>{@link #BACKWARD_TRANSITIVE} — as {@code BACKWARD} but checked against <b>every</b> prior
 *       version.
 *   <li>{@link #FORWARD} — the <b>latest</b> prior version must be able to read data written with
 *       the new schema (producer-first).
 *   <li>{@link #FORWARD_TRANSITIVE} — as {@code FORWARD} but checked against <b>every</b> prior
 *       version.
 *   <li>{@link #FULL} — both {@code BACKWARD} and {@code FORWARD} against the latest prior.
 *   <li>{@link #FULL_TRANSITIVE} — both directions against every prior version.
 * </ul>
 */
@Internal
public enum CompatLevel {
    NONE(false, false, false),
    BACKWARD(true, false, false),
    BACKWARD_TRANSITIVE(true, false, true),
    FORWARD(false, true, false),
    FORWARD_TRANSITIVE(false, true, true),
    FULL(true, true, false),
    FULL_TRANSITIVE(true, true, true);

    private final boolean backward;
    private final boolean forward;
    private final boolean transitive;

    CompatLevel(boolean backward, boolean forward, boolean transitive) {
        this.backward = backward;
        this.forward = forward;
        this.transitive = transitive;
    }

    /** True when the new schema must be able to read data written by the prior schema. */
    public boolean requiresBackward() {
        return backward;
    }

    /** True when the prior schema must be able to read data written by the new schema. */
    public boolean requiresForward() {
        return forward;
    }

    /** True when the check must compare against every prior version, not just the latest. */
    public boolean isTransitive() {
        return transitive;
    }

    /**
     * Parse a Kafka SR level name (case-insensitive). Throws {@link IllegalArgumentException} for
     * unknown names — SR service converts that to a {@code
     * SchemaRegistryException.Kind.INVALID_INPUT}.
     */
    public static CompatLevel fromString(String name) {
        if (name == null) {
            throw new IllegalArgumentException("compatibility level is required");
        }
        String normalised = name.trim().toUpperCase(Locale.ROOT);
        for (CompatLevel l : values()) {
            if (l.name().equals(normalised)) {
                return l;
            }
        }
        throw new IllegalArgumentException("Unknown compatibility level '" + name + "'");
    }
}
