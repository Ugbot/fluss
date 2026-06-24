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

import org.apache.fluss.annotation.Internal;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Immutable SCRAM credential for a (principal, mechanism) pair.
 *
 * <p>Holds the server-side stored key, server key, salt and iteration count. The byte layout is
 * byte-compatible with Kafka's {@code ExportScramCredentials} shape so that credentials created in
 * Fluss can be migrated to/from Kafka without recomputation.
 *
 * <p>The plain password is never held by this class — only the PBKDF2-derived artefacts.
 */
@Internal
public final class ScramCredential {

    private final byte[] salt;
    private final byte[] storedKey;
    private final byte[] serverKey;
    private final int iterations;

    public ScramCredential(byte[] salt, byte[] storedKey, byte[] serverKey, int iterations) {
        checkNotNull(salt, "salt");
        checkNotNull(storedKey, "storedKey");
        checkNotNull(serverKey, "serverKey");
        checkArgument(salt.length > 0, "salt must be non-empty");
        checkArgument(storedKey.length > 0, "storedKey must be non-empty");
        checkArgument(serverKey.length > 0, "serverKey must be non-empty");
        checkArgument(iterations > 0, "iterations must be positive, got %s", iterations);
        this.salt = salt.clone();
        this.storedKey = storedKey.clone();
        this.serverKey = serverKey.clone();
        this.iterations = iterations;
    }

    /** Returns a defensive copy of the salt bytes. */
    public byte[] salt() {
        return salt.clone();
    }

    /** Returns a defensive copy of the stored key (H(ClientKey)). */
    public byte[] storedKey() {
        return storedKey.clone();
    }

    /** Returns a defensive copy of the server key (HMAC(SaltedPassword, "Server Key")). */
    public byte[] serverKey() {
        return serverKey.clone();
    }

    /** Returns the PBKDF2 iteration count used to derive {@code SaltedPassword}. */
    public int iterations() {
        return iterations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScramCredential that = (ScramCredential) o;
        return iterations == that.iterations
                && Arrays.equals(salt, that.salt)
                && Arrays.equals(storedKey, that.storedKey)
                && Arrays.equals(serverKey, that.serverKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(iterations);
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + Arrays.hashCode(storedKey);
        result = 31 * result + Arrays.hashCode(serverKey);
        return result;
    }

    @Override
    public String toString() {
        // Deliberately omits salt / keys to avoid leaking credential material to logs.
        return "ScramCredential{iterations=" + iterations + "}";
    }
}
