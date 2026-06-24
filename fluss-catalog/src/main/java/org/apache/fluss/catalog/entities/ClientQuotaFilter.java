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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Filter passed to {@link org.apache.fluss.catalog.CatalogService#listClientQuotas}. Matches
 * Kafka's {@code DescribeClientQuotasRequest} filter shape.
 *
 * <p>A filter contains a list of {@link Component}s, each of which restricts one entity-type
 * dimension. A component either matches:
 *
 * <ul>
 *   <li>{@link Component#entityName} set — rows whose {@code entityName} equals the given value
 *       ("exact match"; equal to the empty string matches the "default entity").
 *   <li>{@link Component#matchDefault} true — rows whose {@code entityName} equals the empty string
 *       ("default-entity match").
 *   <li>Neither set — rows of the given entity type regardless of name ("specified match").
 * </ul>
 *
 * <p>With {@link #strict} true, only rows that have exactly one entry per requested {@code
 * entityType} match; otherwise (lenient, Kafka's default) rows with additional entity-type
 * dimensions the client didn't mention also match.
 */
@PublicEvolving
public final class ClientQuotaFilter {

    private final List<Component> components;
    private final boolean strict;

    public ClientQuotaFilter(List<Component> components, boolean strict) {
        this.components = Collections.unmodifiableList(components);
        this.strict = strict;
    }

    public static ClientQuotaFilter matchAll() {
        return new ClientQuotaFilter(Collections.emptyList(), false);
    }

    public List<Component> components() {
        return components;
    }

    public boolean strict() {
        return strict;
    }

    /** One per entity-type dimension the caller is filtering on. */
    public static final class Component {

        private final String entityType;
        private final @Nullable String entityName;
        private final boolean matchDefault;

        private Component(String entityType, @Nullable String entityName, boolean matchDefault) {
            this.entityType = entityType;
            this.entityName = entityName;
            this.matchDefault = matchDefault;
        }

        public static Component ofEntity(String entityType, String entityName) {
            return new Component(entityType, entityName, false);
        }

        public static Component ofDefault(String entityType) {
            return new Component(entityType, null, true);
        }

        /** Any entity of this type (used with {@link ClientQuotaFilter#strict}=false). */
        public static Component ofEntityType(String entityType) {
            return new Component(entityType, null, false);
        }

        public String entityType() {
            return entityType;
        }

        /** Non-null when this component does an exact-name match. */
        @Nullable
        public String entityName() {
            return entityName;
        }

        /** True when this component matches the "default entity" (empty name). */
        public boolean matchDefault() {
            return matchDefault;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Component)) {
                return false;
            }
            Component c = (Component) o;
            return matchDefault == c.matchDefault
                    && entityType.equals(c.entityType)
                    && Objects.equals(entityName, c.entityName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityType, entityName, matchDefault);
        }
    }
}
