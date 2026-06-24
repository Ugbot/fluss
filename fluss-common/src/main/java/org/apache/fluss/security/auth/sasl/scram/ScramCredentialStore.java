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

package org.apache.fluss.security.auth.sasl.scram;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * SPI for resolving and managing SCRAM credentials.
 *
 * <p>The read path ({@link #lookup}) is required for the SASL server handshake. The write path
 * ({@link #upsert}, {@link #delete}, {@link #list}) is optional and used by the {@code
 * DESCRIBE_USER_SCRAM_CREDENTIALS} / {@code ALTER_USER_SCRAM_CREDENTIALS} Kafka admin APIs.
 *
 * <p>Implementations are expected to be thread-safe.
 */
@PublicEvolving
public interface ScramCredentialStore {

    /**
     * Looks up the credential for the given principal and mechanism.
     *
     * @param principalName the SASL authcid (typically the user name)
     * @param mechanism the SCRAM mechanism name (for example {@code SCRAM-SHA-256})
     * @return the credential, or {@link Optional#empty()} if no row exists
     */
    Optional<ScramCredential> lookup(String principalName, String mechanism);

    /** Default no-op: read-only stores (JAAS file) reject mutations. */
    default void upsert(String principalName, String mechanism, ScramCredential credential) {
        throw new UnsupportedOperationException(
                "This ScramCredentialStore is read-only: " + getClass().getName());
    }

    /** Default no-op: read-only stores reject deletions. */
    default void delete(String principalName, String mechanism) {
        throw new UnsupportedOperationException(
                "This ScramCredentialStore is read-only: " + getClass().getName());
    }

    /**
     * Lists all (principal, mechanism) entries known to this store. Default implementation returns
     * an empty list — stores that back admin APIs must override this.
     */
    default List<Entry> list() {
        return Collections.emptyList();
    }

    /** Lightweight tuple used by {@link #list()}. */
    final class Entry {
        private final String principalName;
        private final String mechanism;
        private final int iterations;

        public Entry(String principalName, String mechanism, int iterations) {
            this.principalName = principalName;
            this.mechanism = mechanism;
            this.iterations = iterations;
        }

        public String principalName() {
            return principalName;
        }

        public String mechanism() {
            return mechanism;
        }

        public int iterations() {
            return iterations;
        }
    }
}
