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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Byte-level SCRAM primitives (RFC 5802). Implements {@code Hi} (PBKDF2-HMAC), {@code HMAC}, {@code
 * H} and the {@code SaltedPassword → ClientKey / StoredKey / ServerKey} derivation path used by
 * SCRAM-SHA-256 / SCRAM-SHA-512.
 *
 * <p>Uses only JDK crypto — no BouncyCastle dependency. The derivation is byte-compatible with the
 * shapes stored by Apache Kafka's {@code ScramFormatter}, so credentials created here can be served
 * to a Kafka SCRAM client unchanged.
 */
@Internal
public final class ScramCredentialUtils {

    private static final byte[] CLIENT_KEY_LABEL = "Client Key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_KEY_LABEL = "Server Key".getBytes(StandardCharsets.UTF_8);

    private static final ThreadLocal<SecureRandom> SECURE_RANDOM =
            new ThreadLocal<SecureRandom>() {
                @Override
                protected SecureRandom initialValue() {
                    return new SecureRandom();
                }
            };

    private ScramCredentialUtils() {}

    /**
     * Parameter spec kept for API-shape parity with {@code PBEKeySpec} / legacy crypto factories.
     * We do PBKDF2 manually (Hi) because JDK 8's {@code PBEKeySpec} only accepts {@code char[]}
     * passwords — RFC 5802 requires a {@code byte[]}-derived {@code SaltedPassword} computed over
     * the SASLprep'd UTF-8 encoding, which we obtain directly.
     */
    public static final class PbeSpec implements AlgorithmParameterSpec {}

    /**
     * Generates cryptographically secure random salt of the given length in bytes.
     *
     * @param saltBytes the salt length in bytes (RFC 5802 recommends &gt;= 16)
     */
    public static byte[] randomSalt(int saltBytes) {
        checkArgument(saltBytes > 0, "saltBytes must be positive, got %s", saltBytes);
        byte[] salt = new byte[saltBytes];
        SECURE_RANDOM.get().nextBytes(salt);
        return salt;
    }

    /** Convenience: 24-byte salt (Kafka's default). */
    public static byte[] randomSalt() {
        return randomSalt(24);
    }

    /**
     * Derives a {@link ScramCredential} from a plain password.
     *
     * <p>Follows RFC 5802 §3:
     *
     * <pre>
     *   SaltedPassword = Hi(Normalize(password), salt, iterations)
     *   ClientKey      = HMAC(SaltedPassword, "Client Key")
     *   StoredKey      = H(ClientKey)
     *   ServerKey      = HMAC(SaltedPassword, "Server Key")
     * </pre>
     *
     * <p>{@code Normalize} is SASLprep; the JDK ships no public SASLprep. For common ASCII
     * passwords the raw UTF-8 bytes are identical to the normalised form. Passwords containing
     * non-ASCII control characters should be pre-normalised by the caller.
     *
     * @param mechanism the SCRAM mechanism (selects the MAC / hash algorithm)
     * @param password the plain password (never stored)
     * @param salt the salt bytes (caller-chosen; {@link #randomSalt()} for new rows)
     * @param iterations the PBKDF2 iteration count (RFC 7677 recommends &gt;= 4096)
     */
    public static ScramCredential deriveCredential(
            ScramMechanism mechanism, String password, byte[] salt, int iterations) {
        checkNotNull(mechanism, "mechanism");
        checkNotNull(password, "password");
        checkNotNull(salt, "salt");
        checkArgument(
                iterations >= mechanism.minIterations(),
                "iterations %s below mechanism minimum %s",
                iterations,
                mechanism.minIterations());
        byte[] saltedPassword =
                hi(mechanism, password.getBytes(StandardCharsets.UTF_8), salt, iterations);
        byte[] clientKey = hmac(mechanism, saltedPassword, CLIENT_KEY_LABEL);
        byte[] storedKey = h(mechanism, clientKey);
        byte[] serverKey = hmac(mechanism, saltedPassword, SERVER_KEY_LABEL);
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    /** RFC 5802 {@code Hi}: PBKDF2-HMAC-{@code mechanism}. */
    public static byte[] hi(
            ScramMechanism mechanism, byte[] password, byte[] salt, int iterations) {
        checkArgument(iterations > 0, "iterations must be positive");
        try {
            Mac mac = Mac.getInstance(mechanism.macAlgorithm());
            mac.init(new SecretKeySpec(password, mechanism.macAlgorithm()));
            // U1 = HMAC(password, salt || INT(1))
            mac.update(salt);
            mac.update(new byte[] {0, 0, 0, 1});
            byte[] u = mac.doFinal();
            byte[] result = u.clone();
            for (int i = 2; i <= iterations; i++) {
                mac.init(new SecretKeySpec(password, mechanism.macAlgorithm()));
                u = mac.doFinal(u);
                for (int j = 0; j < result.length; j++) {
                    result[j] ^= u[j];
                }
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "PBKDF2 derivation failed for " + mechanism.mechanismName(), e);
        }
    }

    /** RFC 5802 {@code HMAC}: keyed-hash MAC using the mechanism's MAC algorithm. */
    public static byte[] hmac(ScramMechanism mechanism, byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance(mechanism.macAlgorithm());
            mac.init(new SecretKeySpec(key, mechanism.macAlgorithm()));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "HMAC computation failed for " + mechanism.mechanismName(), e);
        }
    }

    /** RFC 5802 {@code H}: the cryptographic hash of the mechanism. */
    public static byte[] h(ScramMechanism mechanism, byte[] data) {
        try {
            return MessageDigest.getInstance(mechanism.hashAlgorithm()).digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "Hash algorithm not available: " + mechanism.hashAlgorithm(), e);
        }
    }

    /** Constant-time byte-array equality. */
    public static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a == null || b == null || a.length != b.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    /** XOR two equal-length byte arrays. */
    public static byte[] xor(byte[] a, byte[] b) {
        checkArgument(a.length == b.length, "byte arrays must have equal length");
        byte[] out = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return out;
    }
}
