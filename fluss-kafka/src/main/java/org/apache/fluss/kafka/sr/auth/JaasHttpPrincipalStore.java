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

package org.apache.fluss.kafka.sr.auth;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.security.auth.sasl.jaas.JaasConfig;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@code username → password} map hydrated from a JAAS-syntax string that uses the same
 * {@code user_<name>="<password>"} convention as Fluss's SASL PLAIN server-side JAAS config (see
 * {@code PlainServerCallbackHandler}). Constructed once at {@code SchemaRegistryBootstrap}
 * start-up; not reloaded on config changes.
 *
 * <p>Parsing is delegated to {@link JaasConfig} so the tokenisation rules (comments, escapes,
 * semicolon termination) match the rest of the Fluss SASL stack exactly.
 *
 * <p>An empty JAAS string produces an empty store — {@link #lookup(String)} always returns empty,
 * which disables the HTTP Basic-auth fallback.
 */
@Internal
public final class JaasHttpPrincipalStore {

    private static final String CONTEXT_NAME = "KafkaSchemaRegistryHttpBasicAuth";
    private static final String USER_OPTION_PREFIX = "user_";

    private final Map<String, String> credentials;

    public JaasHttpPrincipalStore(Map<String, String> credentials) {
        this.credentials = Collections.unmodifiableMap(new HashMap<>(credentials));
    }

    /**
     * Parse {@code jaasConfigText} — the contents of {@code
     * kafka.schema-registry.basic-auth-jaas-config} — into a store. Empty / {@code null} text
     * yields an empty store (Basic auth disabled).
     *
     * @throws IllegalArgumentException if the string is not empty but fails to parse as JAAS.
     */
    public static JaasHttpPrincipalStore fromJaasText(String jaasConfigText) {
        if (jaasConfigText == null || jaasConfigText.trim().isEmpty()) {
            return new JaasHttpPrincipalStore(Collections.<String, String>emptyMap());
        }
        JaasConfig parsed;
        try {
            parsed = new JaasConfig(CONTEXT_NAME, jaasConfigText);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Failed to parse kafka.schema-registry.basic-auth-jaas-config: "
                            + e.getMessage(),
                    e);
        }
        AppConfigurationEntry[] entries = parsed.getAppConfigurationEntry(CONTEXT_NAME);
        Map<String, String> creds = new HashMap<>();
        if (entries != null) {
            for (AppConfigurationEntry entry : entries) {
                for (Map.Entry<String, ?> opt : entry.getOptions().entrySet()) {
                    String key = opt.getKey();
                    if (key != null
                            && key.startsWith(USER_OPTION_PREFIX)
                            && key.length() > USER_OPTION_PREFIX.length()
                            && opt.getValue() != null) {
                        creds.put(
                                key.substring(USER_OPTION_PREFIX.length()),
                                opt.getValue().toString());
                    }
                }
            }
        }
        return new JaasHttpPrincipalStore(creds);
    }

    /**
     * Returns the password registered for {@code username}, or empty if no such user is defined or
     * {@code username} is {@code null}.
     */
    public Optional<String> lookup(String username) {
        if (username == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(credentials.get(username));
    }

    /** Whether any credentials were loaded (false disables HTTP Basic auth). */
    public boolean isEmpty() {
        return credentials.isEmpty();
    }
}
