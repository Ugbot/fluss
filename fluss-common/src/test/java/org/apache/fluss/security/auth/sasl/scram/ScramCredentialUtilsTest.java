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

import org.junit.jupiter.api.Test;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ScramCredentialUtils}. */
class ScramCredentialUtilsTest {

    @Test
    void hiMatchesReferencePbkdf2Sha256() throws Exception {
        byte[] password = "correct horse battery staple".getBytes(StandardCharsets.UTF_8);
        byte[] salt = "pepper".getBytes(StandardCharsets.UTF_8);
        int iterations = 4096;

        byte[] ours =
                ScramCredentialUtils.hi(ScramMechanism.SCRAM_SHA_256, password, salt, iterations);

        // Cross-check against JDK's PBKDF2WithHmacSHA256 (uses the same char[] password path,
        // but for a plain-ASCII password the bytes are identical).
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] ref =
                skf.generateSecret(
                                new PBEKeySpec(
                                        "correct horse battery staple".toCharArray(),
                                        salt,
                                        iterations,
                                        256))
                        .getEncoded();

        assertThat(ours).containsExactly(toBoxed(ref));
    }

    @Test
    void hiMatchesReferencePbkdf2Sha512() throws Exception {
        byte[] password = "hunter2".getBytes(StandardCharsets.UTF_8);
        byte[] salt = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        int iterations = 4096;

        byte[] ours =
                ScramCredentialUtils.hi(ScramMechanism.SCRAM_SHA_512, password, salt, iterations);
        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        byte[] ref =
                skf.generateSecret(new PBEKeySpec("hunter2".toCharArray(), salt, iterations, 512))
                        .getEncoded();
        assertThat(ours).containsExactly(toBoxed(ref));
    }

    @Test
    void deriveCredentialIsReproducible() {
        String password = UUID.randomUUID().toString();
        byte[] salt = ScramCredentialUtils.randomSalt(24);

        ScramCredential a =
                ScramCredentialUtils.deriveCredential(
                        ScramMechanism.SCRAM_SHA_256, password, salt, 4096);
        ScramCredential b =
                ScramCredentialUtils.deriveCredential(
                        ScramMechanism.SCRAM_SHA_256, password, salt, 4096);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void deriveCredentialDiffersWithDifferentPasswords() {
        byte[] salt = ScramCredentialUtils.randomSalt(16);
        ScramCredential a =
                ScramCredentialUtils.deriveCredential(
                        ScramMechanism.SCRAM_SHA_512, "alpha", salt, 4096);
        ScramCredential b =
                ScramCredentialUtils.deriveCredential(
                        ScramMechanism.SCRAM_SHA_512, "beta", salt, 4096);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void constantTimeEqualsDetectsDifference() {
        byte[] base = new byte[] {1, 2, 3, 4};
        assertThat(ScramCredentialUtils.constantTimeEquals(base, new byte[] {1, 2, 3, 4})).isTrue();
        assertThat(ScramCredentialUtils.constantTimeEquals(base, new byte[] {1, 2, 3, 5}))
                .isFalse();
        assertThat(ScramCredentialUtils.constantTimeEquals(base, new byte[] {1, 2, 3})).isFalse();
        assertThat(ScramCredentialUtils.constantTimeEquals(base, null)).isFalse();
    }

    @Test
    void rejectsIterationsBelowMechanismMinimum() {
        byte[] salt = ScramCredentialUtils.randomSalt();
        assertThatThrownBy(
                        () ->
                                ScramCredentialUtils.deriveCredential(
                                        ScramMechanism.SCRAM_SHA_256, "pw", salt, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations");
    }

    @Test
    void saltGenerationYieldsRequestedLength() {
        assertThat(ScramCredentialUtils.randomSalt(16)).hasSize(16);
        assertThat(ScramCredentialUtils.randomSalt(32)).hasSize(32);
    }

    @Test
    void mechanismLookupReturnsExpected() {
        assertThat(ScramMechanism.forMechanismName("SCRAM-SHA-256"))
                .isEqualTo(ScramMechanism.SCRAM_SHA_256);
        assertThat(ScramMechanism.forMechanismName("SCRAM-SHA-512"))
                .isEqualTo(ScramMechanism.SCRAM_SHA_512);
        assertThat(ScramMechanism.forMechanismName("PLAIN")).isNull();
        assertThat(ScramMechanism.isScram("SCRAM-SHA-256")).isTrue();
        assertThat(ScramMechanism.isScram("PLAIN")).isFalse();
    }

    private static Byte[] toBoxed(byte[] raw) {
        Byte[] out = new Byte[raw.length];
        for (int i = 0; i < raw.length; i++) {
            out[i] = raw[i];
        }
        return out;
    }
}
