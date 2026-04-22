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

package org.apache.fluss.catalog;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Classified failure raised by {@link CatalogService}. Projections (Kafka SR, Iceberg REST, …)
 * translate {@link Kind} to their own status-code vocabulary.
 */
@PublicEvolving
public final class CatalogException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Classifiers with clean HTTP / protocol status mapping. */
    public enum Kind {
        INVALID_INPUT,
        NOT_FOUND,
        ALREADY_EXISTS,
        CONFLICT,
        UNSUPPORTED,
        INTERNAL
    }

    private final Kind kind;

    public CatalogException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public CatalogException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
