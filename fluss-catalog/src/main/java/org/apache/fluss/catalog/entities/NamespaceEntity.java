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

/** One row in {@code _catalog.__namespaces__}. Namespaces nest via {@code parentId}. */
@PublicEvolving
public final class NamespaceEntity {

    private final String namespaceId;
    private final @Nullable String parentId;
    private final String name;
    private final @Nullable String description;
    private final long createdAtMillis;

    public NamespaceEntity(
            String namespaceId,
            @Nullable String parentId,
            String name,
            @Nullable String description,
            long createdAtMillis) {
        this.namespaceId = namespaceId;
        this.parentId = parentId;
        this.name = name;
        this.description = description;
        this.createdAtMillis = createdAtMillis;
    }

    public String namespaceId() {
        return namespaceId;
    }

    @Nullable
    public String parentId() {
        return parentId;
    }

    public String name() {
        return name;
    }

    @Nullable
    public String description() {
        return description;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NamespaceEntity)) {
            return false;
        }
        NamespaceEntity that = (NamespaceEntity) o;
        return namespaceId.equals(that.namespaceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaceId);
    }
}
