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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;

/**
 * Translates cleanly to Confluent REST HTTP status codes in {@link SchemaRegistryHttpHandler}. The
 * {@link Kind} enum captures enough variants to cover every 4xx the Phase A1 endpoints need.
 */
@Internal
public final class SchemaRegistryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Maps 1:1 to an HTTP status code; see {@link SchemaRegistryHttpHandler#toHttpStatus}. */
    public enum Kind {
        INVALID_INPUT,
        UNSUPPORTED,
        NOT_FOUND,
        CONFLICT,
        FORBIDDEN,
        INTERNAL
    }

    private final Kind kind;

    public SchemaRegistryException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public SchemaRegistryException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
