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

package org.apache.fluss.kafka.auth;

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-connection SASL state machine for a Kafka channel.
 *
 * <p>Bridges Kafka's {@code SASL_HANDSHAKE} (api key 17) and {@code SASL_AUTHENTICATE} (api key 36)
 * wire frames onto a Fluss {@link ServerAuthenticator}. The opaque auth bytes carried in Kafka's
 * {@code SaslAuthenticateRequest.authBytes()} are the same bytes the SPI's {@link
 * ServerAuthenticator#evaluateResponse(byte[])} already consumes — this is a translation, not a
 * re-encoding. No {@code javax.security.sasl.*} type is referenced here; everything flows through
 * the Fluss SPI.
 *
 * <p>State transitions (one instance per Netty channel):
 *
 * <pre>
 *   START
 *     └─ handleSaslHandshake(valid mechanism) ──▶ HANDSHAKE_DONE
 *     └─ handleSaslHandshake(invalid)         ──▶ START (error returned; client will retry)
 *
 *   HANDSHAKE_DONE
 *     └─ handleSaslAuthenticate(not complete) ──▶ HANDSHAKE_DONE (challenge returned)
 *     └─ handleSaslAuthenticate(completed)    ──▶ AUTHED(principal)
 *     └─ handleSaslAuthenticate(AuthError)    ──▶ AUTH_FAILED
 *
 *   AUTHED   — non-SASL APIs accepted; SASL APIs rejected as ILLEGAL_SASL_STATE.
 *   AUTH_FAILED — all future APIs rejected.
 * </pre>
 *
 * <p>Not thread-safe; callers must ensure a single Netty thread drives the state machine (the
 * decoder's {@code channelRead0} is single-threaded per channel, which satisfies this).
 */
public final class KafkaSaslTranscoder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSaslTranscoder.class);

    private enum State {
        START,
        HANDSHAKE_DONE,
        AUTHED,
        AUTH_FAILED
    }

    private final KafkaListenerAuthConfig authConfig;
    private final String remoteAddress;
    private final String listenerName;

    private State state = State.START;
    private String chosenMechanism;
    private ServerAuthenticator authenticator;
    private FlussPrincipal principal = FlussPrincipal.ANONYMOUS;

    public KafkaSaslTranscoder(
            KafkaListenerAuthConfig authConfig, String listenerName, String remoteAddress) {
        this.authConfig = checkNotNull(authConfig, "authConfig");
        this.listenerName = checkNotNull(listenerName, "listenerName");
        this.remoteAddress = remoteAddress == null ? "unknown" : remoteAddress;
        if (!authConfig.requiresSasl()) {
            throw new IllegalArgumentException(
                    "KafkaSaslTranscoder may only be constructed for a SASL listener.");
        }
    }

    /**
     * Handle a {@code SASL_HANDSHAKE} request. Selects a mechanism from the enabled list, creates a
     * fresh {@link ServerAuthenticator}, and initializes it with the connection context. The
     * response advertises all enabled mechanisms regardless of success/failure (Kafka protocol
     * contract).
     */
    public AbstractResponse handleSaslHandshake(SaslHandshakeRequest request) {
        checkNotNull(request, "request");
        String requested = request.data().mechanism();
        String requestedUpper = requested == null ? "" : requested.toUpperCase(Locale.ROOT);
        List<String> enabled = authConfig.enabledMechanisms();

        SaslHandshakeResponseData data = new SaslHandshakeResponseData();
        data.setMechanisms(new ArrayList<>(enabled));

        if (!enabled.contains(requestedUpper)) {
            LOG.info(
                    "Rejecting Kafka SASL handshake from {}: mechanism '{}' not in enabled list {}",
                    remoteAddress,
                    requested,
                    enabled);
            data.setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());
            return new SaslHandshakeResponse(data);
        }

        try {
            ServerAuthenticator fresh = authConfig.authenticatorSupplier().get();
            fresh.initialize(new SaslAuthenticateContext(requestedUpper));
            this.authenticator = fresh;
            this.chosenMechanism = requestedUpper;
            this.state = State.HANDSHAKE_DONE;
            data.setErrorCode(Errors.NONE.code());
            LOG.debug(
                    "Kafka SASL handshake accepted for listener={} mechanism={} remote={}",
                    listenerName,
                    requestedUpper,
                    remoteAddress);
            return new SaslHandshakeResponse(data);
        } catch (Throwable t) {
            // initialize() can throw RuntimeException on JAAS misconfiguration; surface this as an
            // unsupported-mechanism error (we do not want to tear down the channel — the client
            // will retry or surface the failure itself).
            LOG.warn(
                    "Failed to initialize SASL authenticator for listener={} mechanism={}: {}",
                    listenerName,
                    requestedUpper,
                    t.toString());
            data.setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());
            return new SaslHandshakeResponse(data);
        }
    }

    /** Handle a {@code SASL_AUTHENTICATE} request. */
    public AbstractResponse handleSaslAuthenticate(SaslAuthenticateRequest request) {
        checkNotNull(request, "request");

        if (state == State.START) {
            return saslAuthError(
                    Errors.ILLEGAL_SASL_STATE, "SaslAuthenticate received before SaslHandshake");
        }
        if (state == State.AUTH_FAILED) {
            return saslAuthError(
                    Errors.ILLEGAL_SASL_STATE,
                    "SaslAuthenticate received after authentication failed");
        }
        if (state == State.AUTHED) {
            // Re-authentication is out of scope for this phase; treat as illegal state.
            return saslAuthError(
                    Errors.ILLEGAL_SASL_STATE,
                    "SaslAuthenticate received after authentication completed");
        }
        // state == HANDSHAKE_DONE
        byte[] clientBytes = request.data().authBytes();
        if (clientBytes == null) {
            clientBytes = new byte[0];
        }

        try {
            byte[] challenge = authenticator.evaluateResponse(clientBytes);
            SaslAuthenticateResponseData data = new SaslAuthenticateResponseData();
            data.setErrorCode(Errors.NONE.code());
            data.setAuthBytes(challenge == null ? new byte[0] : challenge);
            if (authenticator.isCompleted()) {
                FlussPrincipal p = authenticator.createPrincipal();
                this.principal = p == null ? FlussPrincipal.ANONYMOUS : p;
                this.state = State.AUTHED;
                // sessionLifetimeMs = 0 disables broker-initiated re-auth; the client stays
                // authenticated for the connection's lifetime.
                data.setSessionLifetimeMs(0L);
                LOG.info(
                        "Kafka SASL authentication complete for listener={} mechanism={} "
                                + "remote={} principal={}",
                        listenerName,
                        chosenMechanism,
                        remoteAddress,
                        this.principal);
            }
            return new SaslAuthenticateResponse(data);
        } catch (AuthenticationException ae) {
            this.state = State.AUTH_FAILED;
            LOG.info(
                    "Kafka SASL authentication failed for listener={} mechanism={} remote={}: {}",
                    listenerName,
                    chosenMechanism,
                    remoteAddress,
                    ae.getMessage());
            return saslAuthError(
                    Errors.SASL_AUTHENTICATION_FAILED,
                    ae.getMessage() == null ? "Authentication failed" : ae.getMessage());
        } catch (Throwable t) {
            this.state = State.AUTH_FAILED;
            LOG.warn(
                    "SASL authenticator threw on listener={} mechanism={} remote={}",
                    listenerName,
                    chosenMechanism,
                    remoteAddress,
                    t);
            return saslAuthError(
                    Errors.SASL_AUTHENTICATION_FAILED,
                    t.getMessage() == null ? "Authentication failed" : t.getMessage());
        }
    }

    private static AbstractResponse saslAuthError(Errors err, String message) {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData();
        data.setErrorCode(err.code());
        data.setErrorMessage(message);
        data.setAuthBytes(new byte[0]);
        return new SaslAuthenticateResponse(data);
    }

    /** {@code true} once SASL has completed successfully. */
    public boolean isAuthenticated() {
        return state == State.AUTHED;
    }

    /**
     * {@code true} iff this listener requires SASL but the connection has not completed it yet —
     * used by the decoder to gate non-SASL, non-ApiVersions APIs.
     */
    public boolean isSaslRequiredButNotDone() {
        return state != State.AUTHED;
    }

    /**
     * Principal attributed to requests on this connection. Defaults to {@link
     * FlussPrincipal#ANONYMOUS} until SASL completes.
     */
    public FlussPrincipal principal() {
        return principal;
    }

    /** Release any resources held by the underlying authenticator. Safe to call repeatedly. */
    public void close() {
        ServerAuthenticator a = this.authenticator;
        this.authenticator = null;
        if (a == null) {
            return;
        }
        try {
            a.close();
        } catch (Throwable t) {
            LOG.debug("Ignoring error on SASL authenticator close", t);
        }
    }

    /** Anonymous {@link ServerAuthenticator.AuthenticateContext} bound to this connection. */
    private final class SaslAuthenticateContext implements ServerAuthenticator.AuthenticateContext {
        private final String mechanism;

        SaslAuthenticateContext(String mechanism) {
            this.mechanism = mechanism;
        }

        @Override
        public String ipAddress() {
            return remoteAddress;
        }

        @Override
        public String listenerName() {
            return listenerName;
        }

        @Override
        public String protocol() {
            // The Fluss SPI reuses "protocol" for the concrete SASL mechanism name (e.g. PLAIN).
            return mechanism;
        }
    }
}
