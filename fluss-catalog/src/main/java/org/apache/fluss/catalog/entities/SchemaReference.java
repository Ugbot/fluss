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
 * Kafka SR reference. Represents one edge in the directed reference graph: a referrer schema points
 * at a referent {@code (subject, version)} pair under a symbolic {@code name} (Avro fullname, JSON
 * Schema {@code $ref}, {@code .proto} import path).
 *
 * <p>The wire-side payload only carries {@code (name, subject, version)}; the Fluss catalog
 * additionally pins {@link #referencedSchemaId()} (the catalog UUID of the referent) at register
 * time so that the edge stays immutable across hard-deletes and re-registrations of the referent.
 *
 * <p>Storage row lives in {@code _catalog.__schema_references__}; see {@link
 * org.apache.fluss.catalog.SystemTables}.
 */
@PublicEvolving
public final class SchemaReference {

    private final String name;
    private final String subject;
    private final int version;
    private final String referencedSchemaId;

    public SchemaReference(String name, String subject, int version, String referencedSchemaId) {
        this.name = name;
        this.subject = subject;
        this.version = version;
        this.referencedSchemaId = referencedSchemaId;
    }

    /** Symbolic token the referrer uses to point at the referent (Avro fullname, etc). */
    public String name() {
        return name;
    }

    /** Kafka SR subject of the referent. */
    public String subject() {
        return subject;
    }

    /** Kafka SR version (1-based) of the referent. */
    public int version() {
        return version;
    }

    /** Catalog UUID of the referent's {@code _schemas} row, pinned at register time. */
    public String referencedSchemaId() {
        return referencedSchemaId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaReference)) {
            return false;
        }
        SchemaReference that = (SchemaReference) o;
        return version == that.version
                && Objects.equals(name, that.name)
                && Objects.equals(subject, that.subject)
                && Objects.equals(referencedSchemaId, that.referencedSchemaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, subject, version, referencedSchemaId);
    }

    @Override
    public String toString() {
        return "SchemaReference{name='"
                + name
                + "', subject='"
                + subject
                + "', version="
                + version
                + ", referencedSchemaId='"
                + referencedSchemaId
                + "'}";
    }
}
