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
import java.util.Collections;
import java.util.List;

/**
 * Supported SCRAM mechanisms — SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802, RFC 7677).
 *
 * <p>Each entry binds the SASL mechanism name as sent over the wire to the JCA hash and HMAC
 * algorithm names used to derive SCRAM credentials.
 */
@Internal
public enum ScramMechanism {
    SCRAM_SHA_256("SCRAM-SHA-256", "SHA-256", "HmacSHA256", 4096),
    SCRAM_SHA_512("SCRAM-SHA-512", "SHA-512", "HmacSHA512", 4096);

    private static final List<String> MECHANISM_NAMES =
            Collections.unmodifiableList(
                    Arrays.asList(SCRAM_SHA_256.mechanismName, SCRAM_SHA_512.mechanismName));

    private final String mechanismName;
    private final String hashAlgorithm;
    private final String macAlgorithm;
    private final int minIterations;

    ScramMechanism(
            String mechanismName, String hashAlgorithm, String macAlgorithm, int minIterations) {
        this.mechanismName = mechanismName;
        this.hashAlgorithm = hashAlgorithm;
        this.macAlgorithm = macAlgorithm;
        this.minIterations = minIterations;
    }

    /** Returns the SCRAM mechanism name as advertised on the wire. */
    public String mechanismName() {
        return mechanismName;
    }

    /** Returns the JCA {@link java.security.MessageDigest} algorithm name. */
    public String hashAlgorithm() {
        return hashAlgorithm;
    }

    /** Returns the JCA {@link javax.crypto.Mac} algorithm name. */
    public String macAlgorithm() {
        return macAlgorithm;
    }

    /** Returns the minimum iteration count accepted for this mechanism. */
    public int minIterations() {
        return minIterations;
    }

    /** Returns all supported SCRAM mechanism names. */
    public static List<String> mechanismNames() {
        return MECHANISM_NAMES;
    }

    /** Returns true if the given mechanism name is a supported SCRAM mechanism. */
    public static boolean isScram(String mechanismName) {
        return mechanismName != null && MECHANISM_NAMES.contains(mechanismName);
    }

    /** Resolves a mechanism by its SASL name, or returns null if unknown. */
    public static ScramMechanism forMechanismName(String mechanismName) {
        for (ScramMechanism mech : values()) {
            if (mech.mechanismName.equals(mechanismName)) {
                return mech;
            }
        }
        return null;
    }
}
