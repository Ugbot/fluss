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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainLoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-listener authentication posture resolved from Fluss's public {@link ConfigOptions} surface.
 *
 * <p>Two protocols are modelled:
 *
 * <ul>
 *   <li>{@link Protocol#PLAINTEXT} — no authentication; every request on the listener carries the
 *       anonymous {@link org.apache.fluss.security.acl.FlussPrincipal}.
 *   <li>{@link Protocol#SASL_PLAINTEXT} — Kafka's {@code SASL_HANDSHAKE} + {@code
 *       SASL_AUTHENTICATE} frames are translated onto a Fluss {@link ServerAuthenticator}; the
 *       principal produced by {@link ServerAuthenticator#createPrincipal()} is attached to every
 *       subsequent request on the connection.
 * </ul>
 *
 * <p>A listener is SASL iff {@link ConfigOptions#SERVER_SECURITY_PROTOCOL_MAP} maps its name to
 * {@code sasl} (case-insensitive). Anything else (including an absent mapping) is PLAINTEXT. When
 * the listener is SASL, the authenticator supplier is taken from {@link
 * AuthenticationFactory#loadServerAuthenticatorSuppliers(Configuration)} — that factory is the
 * single public entry point into Fluss's auth SPI, so this class does not import any {@code
 * javax.security.sasl.*} or SASL implementation type.
 */
public final class KafkaListenerAuthConfig {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaListenerAuthConfig.class);

    /**
     * Name used by both Fluss's and kafka-clients' SASL/PLAIN {@link Provider}. When both jars are
     * on the classpath, whichever {@code addProvider} call runs first wins; subsequent adds with
     * the same name are silently ignored. We resolve this on SASL-listener setup by dropping any
     * prior registration (which will be Kafka's, since kafka-clients is almost always loaded by the
     * client-side code paths before the server-side JAAS login runs) and re-registering Fluss's,
     * whose {@link org.apache.fluss.security.auth.sasl.plain.PlainSaslServer} delivers callbacks
     * that {@link org.apache.fluss.security.auth.sasl.plain.PlainServerCallbackHandler} recognises.
     */
    private static final String PLAIN_SASL_PROVIDER_NAME = "Simple SASL/PLAIN Server Provider";

    /** Authentication protocol for a single Kafka listener. */
    public enum Protocol {
        /** No authentication; every request is attributed to {@code ANONYMOUS}. */
        PLAINTEXT,
        /** SASL over plaintext transport; Kafka SASL frames flow through the Fluss SPI. */
        SASL_PLAINTEXT
    }

    private static final String SASL_PROTOCOL = "sasl";

    private final Protocol protocol;
    private final List<String> enabledMechanisms;
    @Nullable private final Supplier<ServerAuthenticator> authenticatorSupplier;

    private KafkaListenerAuthConfig(
            Protocol protocol,
            List<String> enabledMechanisms,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier) {
        this.protocol = checkNotNull(protocol, "protocol");
        this.enabledMechanisms = Collections.unmodifiableList(new ArrayList<>(enabledMechanisms));
        this.authenticatorSupplier = authenticatorSupplier;
    }

    /** Resolve the auth posture for {@code listenerName} from {@code conf}. */
    public static KafkaListenerAuthConfig resolve(Configuration conf, String listenerName) {
        checkNotNull(conf, "conf");
        checkNotNull(listenerName, "listenerName");

        Map<String, String> protocolMap = conf.getMap(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        String protocolValue = null;
        if (protocolMap != null) {
            // Protocol map keys may be cased differently by operators; match case-insensitively.
            for (Map.Entry<String, String> entry : protocolMap.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(listenerName)) {
                    protocolValue = entry.getValue();
                    break;
                }
            }
        }

        if (protocolValue == null || !SASL_PROTOCOL.equalsIgnoreCase(protocolValue.trim())) {
            return new KafkaListenerAuthConfig(
                    Protocol.PLAINTEXT, Collections.<String>emptyList(), null);
        }

        List<String> enabled = conf.get(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        if (enabled == null || enabled.isEmpty()) {
            throw new IllegalArgumentException(
                    "Listener '"
                            + listenerName
                            + "' is configured as SASL via "
                            + ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key()
                            + " but "
                            + ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG.key()
                            + " is not set.");
        }
        List<String> mechanismsUpper = new ArrayList<>(enabled.size());
        for (String m : enabled) {
            mechanismsUpper.add(m.toUpperCase(Locale.ROOT));
        }

        Map<String, Supplier<ServerAuthenticator>> suppliers =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(conf);
        Supplier<ServerAuthenticator> supplier = null;
        if (suppliers != null) {
            for (Map.Entry<String, Supplier<ServerAuthenticator>> entry : suppliers.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(listenerName)) {
                    supplier = entry.getValue();
                    break;
                }
            }
        }
        if (supplier == null) {
            throw new IllegalStateException(
                    "Listener '"
                            + listenerName
                            + "' is SASL but no ServerAuthenticator was discovered for it. "
                            + "Check "
                            + ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key()
                            + " and that the matching auth plugin is on the classpath.");
        }

        // Ensure Fluss's SASL/PLAIN provider wins the Sasl.createSaslServer lookup even when
        // kafka-clients is on the same classpath (kafka-clients ships its own PLAIN provider
        // under the same name, which returns a SaslServer that delivers kafka-sourced callbacks
        // that Fluss's PlainServerCallbackHandler rejects as UnsupportedCallbackException).
        ensureFlussPlainProviderPrecedence();

        return new KafkaListenerAuthConfig(Protocol.SASL_PLAINTEXT, mechanismsUpper, supplier);
    }

    /**
     * Drop any prior registration of the SASL/PLAIN provider (there can be at most one, by name)
     * and re-register Fluss's by loading {@link PlainLoginModule} — its static initialiser calls
     * {@code PlainSaslServerProvider.initialize()}. Safe to call repeatedly; the effect is
     * idempotent after the first successful swap.
     */
    private static synchronized void ensureFlussPlainProviderPrecedence() {
        Provider existing = Security.getProvider(PLAIN_SASL_PROVIDER_NAME);
        String existingClassName = existing == null ? null : existing.getClass().getName();
        if (existingClassName != null && existingClassName.startsWith("org.apache.fluss.")) {
            // Already Fluss's — nothing to do.
            return;
        }
        if (existing != null) {
            Security.removeProvider(PLAIN_SASL_PROVIDER_NAME);
            LOG.info(
                    "Removed SASL/PLAIN provider '{}' (class={}) to give Fluss's provider "
                            + "precedence for Kafka SASL listeners.",
                    PLAIN_SASL_PROVIDER_NAME,
                    existingClassName);
        }
        // Class reference loads PlainLoginModule, whose <clinit> registers Fluss's provider.
        try {
            Class.forName(
                    PlainLoginModule.class.getName(),
                    true,
                    PlainLoginModule.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Fluss SASL/PLAIN login module is missing from the classpath.", e);
        }
    }

    public Protocol protocol() {
        return protocol;
    }

    /** Unmodifiable list of enabled SASL mechanisms, upper-cased. Empty for {@code PLAINTEXT}. */
    public List<String> enabledMechanisms() {
        return enabledMechanisms;
    }

    /**
     * Authenticator supplier for this listener. {@code null} when the listener is {@link
     * Protocol#PLAINTEXT}; each new Kafka connection on a SASL listener calls {@code
     * supplier.get()} to obtain a fresh {@link ServerAuthenticator} instance.
     */
    @Nullable
    public Supplier<ServerAuthenticator> authenticatorSupplier() {
        return authenticatorSupplier;
    }

    /** {@code true} iff this listener requires SASL authentication. */
    public boolean requiresSasl() {
        return protocol == Protocol.SASL_PLAINTEXT;
    }
}
