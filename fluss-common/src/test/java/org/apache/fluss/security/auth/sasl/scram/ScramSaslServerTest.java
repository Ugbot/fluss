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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test of {@link ScramSaslServer} against a hand-rolled RFC 5802 SCRAM client. Exercises
 * both SHA-256 and SHA-512 mechanisms, plus the negative "wrong password" path.
 */
class ScramSaslServerTest {

    @Test
    void sha256HandshakeSucceedsWithCorrectPassword() throws Exception {
        runHandshake(ScramMechanism.SCRAM_SHA_256, "alice", "alice-secret", "alice-secret");
    }

    @Test
    void sha512HandshakeSucceedsWithCorrectPassword() throws Exception {
        runHandshake(ScramMechanism.SCRAM_SHA_512, "bob", "bob-secret", "bob-secret");
    }

    @Test
    void sha256WrongPasswordIsRejected() {
        assertThatThrownBy(
                        () ->
                                runHandshake(
                                        ScramMechanism.SCRAM_SHA_256,
                                        "alice",
                                        "alice-secret",
                                        "wrong"))
                .isInstanceOf(SaslException.class)
                .hasMessageContaining("invalid client proof");
    }

    @Test
    void unknownUserIsRejected() {
        // Credential store returns empty; server returns fabricated challenge but proof will not
        // match because stored/server keys are zeroed — outcome: SaslException from final step.
        InMemoryStore store = new InMemoryStore();
        ScramSaslServer server =
                new ScramSaslServer(
                        ScramMechanism.SCRAM_SHA_256, new ScramServerCallbackHandler(store));
        ScramClient client = new ScramClient(ScramMechanism.SCRAM_SHA_256, "ghost", "whatever");

        assertThatThrownBy(
                        () -> {
                            byte[] first = client.clientFirst();
                            byte[] serverFirst = server.evaluateResponse(first);
                            byte[] clientFinal = client.clientFinal(serverFirst);
                            server.evaluateResponse(clientFinal);
                        })
                .isInstanceOf(SaslException.class);
    }

    private void runHandshake(
            ScramMechanism mechanism, String userName, String serverPassword, String clientPassword)
            throws Exception {
        InMemoryStore store = new InMemoryStore();
        byte[] salt = ScramCredentialUtils.randomSalt();
        store.put(
                userName,
                mechanism.mechanismName(),
                ScramCredentialUtils.deriveCredential(mechanism, serverPassword, salt, 4096));

        ScramSaslServer server =
                new ScramSaslServer(mechanism, new ScramServerCallbackHandler(store));
        ScramClient client = new ScramClient(mechanism, userName, clientPassword);

        byte[] clientFirst = client.clientFirst();
        byte[] serverFirst = server.evaluateResponse(clientFirst);
        byte[] clientFinal = client.clientFinal(serverFirst);
        byte[] serverFinal = server.evaluateResponse(clientFinal);
        client.verifyServerFinal(serverFinal);

        assertThat(server.isComplete()).isTrue();
        assertThat(server.getAuthorizationID()).isEqualTo(userName);
        assertThat(server.getMechanismName()).isEqualTo(mechanism.mechanismName());
    }

    // ---------------------------------------------------------------------------------------------
    // In-memory store + hand-rolled SCRAM client (RFC 5802). Kept minimal — enough to drive the
    // server.
    // ---------------------------------------------------------------------------------------------

    private static final class InMemoryStore implements ScramCredentialStore {
        private final Map<String, ScramCredential> credentials = new HashMap<>();

        void put(String principal, String mechanism, ScramCredential credential) {
            credentials.put(principal + "|" + mechanism, credential);
        }

        @Override
        public java.util.Optional<ScramCredential> lookup(String principalName, String mechanism) {
            return java.util.Optional.ofNullable(credentials.get(principalName + "|" + mechanism));
        }
    }

    /** Not to be used outside tests — not SASLprep-compliant, no channel binding, etc. */
    private static final class ScramClient {
        private final ScramMechanism mechanism;
        private final String userName;
        private final String password;
        private final String clientNonce;
        private String clientFirstBare;
        private String serverFirstMessage;
        private String serverNonce;
        private byte[] serverKey;

        ScramClient(ScramMechanism mechanism, String userName, String password) {
            this.mechanism = mechanism;
            this.userName = userName;
            this.password = password;
            byte[] nonceBytes = new byte[24];
            new SecureRandom().nextBytes(nonceBytes);
            this.clientNonce = Base64.getEncoder().encodeToString(nonceBytes);
        }

        byte[] clientFirst() {
            clientFirstBare = "n=" + userName + ",r=" + clientNonce;
            return ("n,," + clientFirstBare).getBytes(StandardCharsets.UTF_8);
        }

        byte[] clientFinal(byte[] serverFirst) {
            serverFirstMessage = new String(serverFirst, StandardCharsets.UTF_8);
            String[] parts = serverFirstMessage.split(",");
            String nonce = null;
            String salt = null;
            int iterations = 0;
            for (String part : parts) {
                if (part.startsWith("r=")) {
                    nonce = part.substring(2);
                } else if (part.startsWith("s=")) {
                    salt = part.substring(2);
                } else if (part.startsWith("i=")) {
                    iterations = Integer.parseInt(part.substring(2));
                }
            }
            if (nonce == null || salt == null || iterations <= 0) {
                throw new IllegalStateException("Malformed server-first: " + serverFirstMessage);
            }
            this.serverNonce = nonce.substring(clientNonce.length());
            byte[] saltBytes = Base64.getDecoder().decode(salt);
            byte[] saltedPassword =
                    ScramCredentialUtils.hi(
                            mechanism,
                            password.getBytes(StandardCharsets.UTF_8),
                            saltBytes,
                            iterations);
            byte[] clientKey =
                    ScramCredentialUtils.hmac(
                            mechanism,
                            saltedPassword,
                            "Client Key".getBytes(StandardCharsets.UTF_8));
            byte[] storedKey = ScramCredentialUtils.h(mechanism, clientKey);
            this.serverKey =
                    ScramCredentialUtils.hmac(
                            mechanism,
                            saltedPassword,
                            "Server Key".getBytes(StandardCharsets.UTF_8));
            String channelBinding =
                    Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8));
            String clientFinalNoProof = "c=" + channelBinding + ",r=" + nonce;
            String authMessage =
                    clientFirstBare + "," + serverFirstMessage + "," + clientFinalNoProof;
            byte[] clientSignature =
                    ScramCredentialUtils.hmac(
                            mechanism, storedKey, authMessage.getBytes(StandardCharsets.UTF_8));
            byte[] clientProof = ScramCredentialUtils.xor(clientKey, clientSignature);
            String proofB64 = Base64.getEncoder().encodeToString(clientProof);
            return (clientFinalNoProof + ",p=" + proofB64).getBytes(StandardCharsets.UTF_8);
        }

        void verifyServerFinal(byte[] serverFinal) {
            String message = new String(serverFinal, StandardCharsets.UTF_8);
            if (!message.startsWith("v=")) {
                throw new IllegalStateException("Unexpected server-final: " + message);
            }
            byte[] receivedSig = Base64.getDecoder().decode(message.substring(2));
            String channelBinding =
                    Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8));
            String clientFinalNoProof = "c=" + channelBinding + ",r=" + clientNonce + serverNonce;
            String authMessage =
                    clientFirstBare + "," + serverFirstMessage + "," + clientFinalNoProof;
            byte[] expectedSig =
                    ScramCredentialUtils.hmac(
                            mechanism, serverKey, authMessage.getBytes(StandardCharsets.UTF_8));
            if (!ScramCredentialUtils.constantTimeEquals(expectedSig, receivedSig)) {
                throw new IllegalStateException("Server signature mismatch");
            }
        }
    }

    /** Helper for Callback unit tests that need it — unused here but keeps tooling happy. */
    @SuppressWarnings("unused")
    private static void handleNoop(Callback[] callbacks) throws UnsupportedCallbackException {
        throw new UnsupportedCallbackException(callbacks[0]);
    }
}
