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
 * One row in {@code _catalog._principals}. A principal is whoever is requesting access — typically
 * a user or a service account. Principal identity is exposed to catalog callers as the name string;
 * the stable {@code principalId} is internal and referenced from {@code _grants}.
 */
@PublicEvolving
public final class PrincipalEntity {

    /** Well-known principal used for un-authenticated requests. Created lazily on first use. */
    public static final String ANONYMOUS = "ANONYMOUS";

    /** Well-known type enumeration — kept open as a string to allow future extension. */
    public static final String TYPE_USER = "USER";

    public static final String TYPE_SERVICE = "SERVICE";

    private final String principalId;
    private final String name;
    private final String type;
    private final long createdAtMillis;

    public PrincipalEntity(String principalId, String name, String type, long createdAtMillis) {
        this.principalId = principalId;
        this.name = name;
        this.type = type;
        this.createdAtMillis = createdAtMillis;
    }

    public String principalId() {
        return principalId;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrincipalEntity)) {
            return false;
        }
        return principalId.equals(((PrincipalEntity) o).principalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principalId);
    }
}
