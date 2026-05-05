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

package org.apache.fluss.kafka.sr.references;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Resolves a Kafka Schema Registry reference {@code name} to the referenced schema's text.
 * Implementations look up bound references by symbolic name (Avro fullname, JSON Schema {@code
 * $ref} URI, {@code .proto} import path) and return the referent's stored schema text.
 *
 * <p>The resolver is constructed per register / compatibility call and consumed by per-format
 * compatibility checkers (Avro, JSON, Protobuf) so that a referrer schema's named-type table can be
 * populated before the referrer itself is parsed. See design 0013.
 */
@PublicEvolving
public interface ReferenceResolver {

    /**
     * Resolve the schema text bound to {@code name}. Returns empty when the name is not bound (the
     * caller decides whether to fail open or closed).
     */
    Optional<String> resolve(String name);

    /**
     * Every name currently bound to this resolver. Useful for diagnostic messages enumerating
     * "available references" when a parser fails to resolve a symbol.
     */
    Collection<String> names();

    /** Empty resolver — used when the caller has no references (the common compat-only path). */
    static ReferenceResolver empty() {
        return EMPTY;
    }

    ReferenceResolver EMPTY =
            new ReferenceResolver() {
                @Override
                public Optional<String> resolve(String name) {
                    return Optional.empty();
                }

                @Override
                public Collection<String> names() {
                    return Collections.emptyList();
                }
            };
}
