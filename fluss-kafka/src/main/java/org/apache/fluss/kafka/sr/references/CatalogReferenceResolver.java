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

import org.apache.fluss.annotation.Internal;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@link ReferenceResolver} backed by a {@code name -> schemaText} map. Built by the SR
 * service after every reference has been resolved through the catalog and the referent's text
 * fetched.
 *
 * <p>This implementation is not catalog-aware on the lookup hot path — eagerly loading the full
 * resolution map up-front keeps the per-call resolver thread-safe and avoids unbounded recursion
 * when a transitive referent triggers another resolver call. The SR service is responsible for
 * flattening the closure if it ever needs cross-reference resolution beyond the directly-declared
 * edges.
 */
@Internal
public final class CatalogReferenceResolver implements ReferenceResolver {

    private final Map<String, String> nameToText;

    public CatalogReferenceResolver(Map<String, String> nameToText) {
        this.nameToText = new LinkedHashMap<>(nameToText);
    }

    /** Construct from parallel {@code names} and {@code texts} lists; sizes must match. */
    public static CatalogReferenceResolver of(List<String> names, List<String> texts) {
        if (names.size() != texts.size()) {
            throw new IllegalArgumentException(
                    "names/texts size mismatch: " + names.size() + " vs " + texts.size());
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i), texts.get(i));
        }
        return new CatalogReferenceResolver(map);
    }

    @Override
    public Optional<String> resolve(String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(nameToText.get(name));
    }

    @Override
    public Collection<String> names() {
        return Collections.unmodifiableCollection(nameToText.keySet());
    }
}
