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
 * Row-shaped mirror of {@code _catalog.__schema_references__}. PK is {@code (referrerSchemaId,
 * referenceName)}; the referent is identified by the trailing {@code (referencedSubject,
 * referencedVersion, referencedSchemaId)} triple — the Kafka SR wire shape plus the pinned catalog
 * UUID.
 */
@PublicEvolving
public final class SchemaReferenceEntity {

    private final String referrerSchemaId;
    private final String referenceName;
    private final String referencedSubject;
    private final int referencedVersion;
    private final String referencedSchemaId;
    private final long createdAtMillis;

    public SchemaReferenceEntity(
            String referrerSchemaId,
            String referenceName,
            String referencedSubject,
            int referencedVersion,
            String referencedSchemaId,
            long createdAtMillis) {
        this.referrerSchemaId = referrerSchemaId;
        this.referenceName = referenceName;
        this.referencedSubject = referencedSubject;
        this.referencedVersion = referencedVersion;
        this.referencedSchemaId = referencedSchemaId;
        this.createdAtMillis = createdAtMillis;
    }

    public String referrerSchemaId() {
        return referrerSchemaId;
    }

    public String referenceName() {
        return referenceName;
    }

    public String referencedSubject() {
        return referencedSubject;
    }

    public int referencedVersion() {
        return referencedVersion;
    }

    public String referencedSchemaId() {
        return referencedSchemaId;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    /** Wire-side projection: drop the row metadata, keep the Kafka SR wire-shape reference. */
    public SchemaReference toReference() {
        return new SchemaReference(
                referenceName, referencedSubject, referencedVersion, referencedSchemaId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaReferenceEntity)) {
            return false;
        }
        SchemaReferenceEntity that = (SchemaReferenceEntity) o;
        return Objects.equals(referrerSchemaId, that.referrerSchemaId)
                && Objects.equals(referenceName, that.referenceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referrerSchemaId, referenceName);
    }
}
