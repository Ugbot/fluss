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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * A {@link SaslServer} implementation for {@code SCRAM-SHA-256} and {@code SCRAM-SHA-512} (RFC
 * 5802, RFC 7677).
 *
 * <p>The server performs a two-round-trip challenge-response authentication:
 *
 * <ol>
 *   <li>Client sends {@code client-first-message}; server replies with {@code server-first-message}
 *       containing the combined nonce, salt and iteration count.
 *   <li>Client sends {@code client-final-message} with its {@code ClientProof}; server verifies the
 *       proof against the stored credential and replies with {@code server-final-message}
 *       containing the {@code ServerSignature}.
 * </ol>
 *
 * <p>Channel binding is declined ({@code n,,} / {@code y,,} GS2 headers only) — SASL channel
 * binding (plus / tls-unique) is not yet supported.
 */
public class ScramSaslServer implements SaslServer {

    private static final int SERVER_NONCE_BYTES = 24;

    private enum State {
        RECEIVE_CLIENT_FIRST_MESSAGE,
        RECEIVE_CLIENT_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    }

    private final ScramMechanism mechanism;
    private final CallbackHandler callbackHandler;
    private final SecureRandom secureRandom = new SecureRandom();

    private State state = State.RECEIVE_CLIENT_FIRST_MESSAGE;
    private String authorizationId;
    private String userName;
    private String clientNonce;
    private String serverNonce;
    private String clientFirstMessageBare;
    private String serverFirstMessage;
    private ScramCredential credential;

    public ScramSaslServer(ScramMechanism mechanism, CallbackHandler callbackHandler) {
        this.mechanism = mechanism;
        this.callbackHandler = callbackHandler;
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        try {
            switch (state) {
                case RECEIVE_CLIENT_FIRST_MESSAGE:
                    return handleClientFirst(response);
                case RECEIVE_CLIENT_FINAL_MESSAGE:
                    return handleClientFinal(response);
                default:
                    throw new IllegalSaslStateException("Unexpected SCRAM state: " + state);
            }
        } catch (SaslException e) {
            state = State.FAILED;
            throw e;
        } catch (RuntimeException e) {
            state = State.FAILED;
            throw new SaslException("SCRAM authentication failed", e);
        }
    }

    private byte[] handleClientFirst(byte[] response) throws SaslException {
        String message = new String(response, StandardCharsets.UTF_8);
        // GS2 header: "n,[a=authzid],"
        if (message.length() < 3) {
            throw new SaslException("Invalid SCRAM client-first-message: too short");
        }
        char gs2Flag = message.charAt(0);
        if (gs2Flag != 'n' && gs2Flag != 'y') {
            throw new SaslException("SCRAM channel binding requested but not supported by server");
        }
        int commaOne = message.indexOf(',');
        int commaTwo = message.indexOf(',', commaOne + 1);
        if (commaOne < 0 || commaTwo < 0) {
            throw new SaslException("Invalid SCRAM client-first-message: malformed GS2 header");
        }
        String authzidPart = message.substring(commaOne + 1, commaTwo);
        if (authzidPart.startsWith("a=")) {
            authorizationId = authzidPart.substring(2);
        }
        clientFirstMessageBare = message.substring(commaTwo + 1);

        String[] attrs = clientFirstMessageBare.split(",");
        String parsedUser = null;
        String parsedNonce = null;
        for (String attr : attrs) {
            if (attr.startsWith("n=")) {
                parsedUser = attr.substring(2);
            } else if (attr.startsWith("r=")) {
                parsedNonce = attr.substring(2);
            }
        }
        if (parsedUser == null || parsedNonce == null) {
            throw new SaslException(
                    "Invalid SCRAM client-first-message: missing username or nonce");
        }
        this.userName = parsedUser;
        this.clientNonce = parsedNonce;
        if (authorizationId == null || authorizationId.isEmpty()) {
            authorizationId = userName;
        } else if (!authorizationId.equals(userName)) {
            throw new SaslException("SCRAM authorization id does not match authentication id");
        }

        // Look up the stored credential via the callback handler.
        ScramCredentialCallback callback = new ScramCredentialCallback(mechanism.mechanismName());
        callback.userName(userName);
        try {
            callbackHandler.handle(new Callback[] {callback});
        } catch (Exception e) {
            throw new SaslException("SCRAM credential lookup failed", e);
        }
        this.credential = callback.credential();
        if (credential == null) {
            // Per RFC 5802 §6 — respond with a fabricated server-first so that unknown user
            // cannot be distinguished from an invalid proof via timing / error messages.
            byte[] fakeSalt = new byte[24];
            secureRandom.nextBytes(fakeSalt);
            this.credential =
                    new ScramCredential(
                            fakeSalt, new byte[32], new byte[32], mechanism.minIterations());
        }

        byte[] nonceBytes = new byte[SERVER_NONCE_BYTES];
        secureRandom.nextBytes(nonceBytes);
        this.serverNonce = Base64.getEncoder().encodeToString(nonceBytes);
        String combinedNonce = clientNonce + serverNonce;
        String saltB64 = Base64.getEncoder().encodeToString(credential.salt());
        this.serverFirstMessage =
                "r=" + combinedNonce + ",s=" + saltB64 + ",i=" + credential.iterations();
        state = State.RECEIVE_CLIENT_FINAL_MESSAGE;
        return serverFirstMessage.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] handleClientFinal(byte[] response) throws SaslException {
        String message = new String(response, StandardCharsets.UTF_8);
        // client-final-message = channel-binding,nonce,proof
        String[] parts = message.split(",");
        String channelBinding = null;
        String nonce = null;
        String proof = null;
        for (String part : parts) {
            if (part.startsWith("c=")) {
                channelBinding = part.substring(2);
            } else if (part.startsWith("r=")) {
                nonce = part.substring(2);
            } else if (part.startsWith("p=")) {
                proof = part.substring(2);
            }
        }
        if (channelBinding == null || nonce == null || proof == null) {
            throw new SaslException("Invalid SCRAM client-final-message");
        }
        String expectedNonce = clientNonce + serverNonce;
        if (!expectedNonce.equals(nonce)) {
            throw new SaslException("SCRAM nonce mismatch");
        }
        // channel-binding for n,, / y,, handshake: base64("n,,") or base64("y,,")
        String expectedCbind =
                Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8));
        String altCbind =
                Base64.getEncoder().encodeToString("y,,".getBytes(StandardCharsets.UTF_8));
        if (!expectedCbind.equals(channelBinding) && !altCbind.equals(channelBinding)) {
            throw new SaslException("SCRAM channel-binding attribute invalid");
        }

        String clientFinalMessageWithoutProof = "c=" + channelBinding + ",r=" + nonce;
        String authMessage =
                clientFirstMessageBare
                        + ","
                        + serverFirstMessage
                        + ","
                        + clientFinalMessageWithoutProof;
        byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.UTF_8);

        byte[] clientSignature =
                ScramCredentialUtils.hmac(mechanism, credential.storedKey(), authMessageBytes);
        byte[] clientProof;
        try {
            clientProof = Base64.getDecoder().decode(proof);
        } catch (IllegalArgumentException e) {
            throw new SaslException("SCRAM client proof is not valid base64", e);
        }
        if (clientProof.length != clientSignature.length) {
            throw new SaslException("SCRAM client proof length mismatch");
        }
        byte[] recoveredClientKey = ScramCredentialUtils.xor(clientProof, clientSignature);
        byte[] recoveredStoredKey = ScramCredentialUtils.h(mechanism, recoveredClientKey);
        if (!ScramCredentialUtils.constantTimeEquals(recoveredStoredKey, credential.storedKey())) {
            throw new SaslException("SCRAM authentication failed: invalid client proof");
        }

        byte[] serverSignature =
                ScramCredentialUtils.hmac(mechanism, credential.serverKey(), authMessageBytes);
        String serverFinal = "v=" + Base64.getEncoder().encodeToString(serverSignature);
        state = State.COMPLETE;
        return serverFinal.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getAuthorizationID() {
        if (state != State.COMPLETE) {
            throw new IllegalStateException("SCRAM authentication not complete");
        }
        return authorizationId;
    }

    @Override
    public String getMechanismName() {
        return mechanism.mechanismName();
    }

    @Override
    public boolean isComplete() {
        return state == State.COMPLETE;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!isComplete()) {
            throw new IllegalStateException("SCRAM authentication not complete");
        }
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!isComplete()) {
            throw new IllegalStateException("SCRAM authentication not complete");
        }
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!isComplete()) {
            throw new IllegalStateException("SCRAM authentication not complete");
        }
        return null;
    }

    @Override
    public void dispose() {}

    /** {@link SaslServerFactory} for the SCRAM-SHA-256 / SCRAM-SHA-512 mechanisms. */
    public static class ScramSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(
                String mechanism,
                String protocol,
                String serverName,
                Map<String, ?> props,
                CallbackHandler cbh)
                throws SaslException {
            ScramMechanism resolved = ScramMechanism.forMechanismName(mechanism);
            if (resolved == null) {
                throw new SaslException(
                        "Unsupported SASL mechanism: "
                                + mechanism
                                + "; expected one of "
                                + ScramMechanism.mechanismNames());
            }
            return new ScramSaslServer(resolved, cbh);
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            if (props != null && "true".equals(props.get(Sasl.POLICY_NOPLAINTEXT))) {
                // SCRAM is not a plaintext mechanism — still advertised when noplaintext is set.
            }
            return ScramMechanism.mechanismNames().toArray(new String[0]);
        }
    }

    /** Thrown when the SASL state machine is driven into an invalid state. */
    private static final class IllegalSaslStateException extends SaslException {
        private static final long serialVersionUID = 1L;

        IllegalSaslStateException(String message) {
            super(message);
        }
    }
}
