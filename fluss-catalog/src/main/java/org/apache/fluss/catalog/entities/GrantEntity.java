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

import java.util.Objects;

/**
 * One row in {@code _catalog._grants}. A grant ties a principal to a privilege on an entity.
 *
 * <p>{@code entityKind} is one of {@link #KIND_CATALOG}, {@link #KIND_NAMESPACE}, {@link
 * #KIND_TABLE}, {@link #KIND_SCHEMA}. For {@link #KIND_CATALOG}, {@code entityId} is the literal
 * {@link #CATALOG_WILDCARD} — such a grant applies to every entity in the catalog. Otherwise {@code
 * entityId} is the stable UUID of the specific entity.
 *
 * <p>Privilege strings are free-form in the catalog itself — projections (the Kafka SR, future
 * Iceberg REST) interpret them. The well-known privilege constants below are what the SR uses
 * today.
 */
@PublicEvolving
public final class GrantEntity {

    public static final String KIND_CATALOG = "CATALOG";
    public static final String KIND_NAMESPACE = "NAMESPACE";
    public static final String KIND_TABLE = "TABLE";
    public static final String KIND_SCHEMA = "SCHEMA";

    /** Sentinel entity id paired with {@link #KIND_CATALOG} for catalog-wide grants. */
    public static final String CATALOG_WILDCARD = "*";

    public static final String PRIVILEGE_READ = "READ";
    public static final String PRIVILEGE_WRITE = "WRITE";
    public static final String PRIVILEGE_CREATE = "CREATE";
    public static final String PRIVILEGE_DROP = "DROP";

    private final String grantId;
    private final String principalId;
    private final String entityKind;
    private final String entityId;
    private final String privilege;
    private final @Nullable String grantedBy;
    private final long grantedAtMillis;

    public GrantEntity(
            String grantId,
            String principalId,
            String entityKind,
            String entityId,
            String privilege,
            @Nullable String grantedBy,
            long grantedAtMillis) {
        this.grantId = grantId;
        this.principalId = principalId;
        this.entityKind = entityKind;
        this.entityId = entityId;
        this.privilege = privilege;
        this.grantedBy = grantedBy;
        this.grantedAtMillis = grantedAtMillis;
    }

    public String grantId() {
        return grantId;
    }

    public String principalId() {
        return principalId;
    }

    public String entityKind() {
        return entityKind;
    }

    public String entityId() {
        return entityId;
    }

    public String privilege() {
        return privilege;
    }

    @Nullable
    public String grantedBy() {
        return grantedBy;
    }

    public long grantedAtMillis() {
        return grantedAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GrantEntity)) {
            return false;
        }
        return grantId.equals(((GrantEntity) o).grantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grantId);
    }
}
