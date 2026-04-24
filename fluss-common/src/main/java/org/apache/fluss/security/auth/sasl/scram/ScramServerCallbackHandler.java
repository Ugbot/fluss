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

import org.apache.fluss.security.auth.sasl.jaas.AuthenticateCallbackHandler;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.util.List;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Server-side callback handler for SCRAM authentication. Delegates credential lookup to a {@link
 * ScramCredentialStore}.
 */
public class ScramServerCallbackHandler implements AuthenticateCallbackHandler {

    private final ScramCredentialStore credentialStore;
    private String mechanism;

    public ScramServerCallbackHandler(ScramCredentialStore credentialStore) {
        this.credentialStore = checkNotNull(credentialStore, "credentialStore");
    }

    @Override
    public void configure(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.mechanism = saslMechanism;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String userName = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                userName = ((NameCallback) callback).getDefaultName();
            } else if (callback instanceof ScramCredentialCallback) {
                ScramCredentialCallback cb = (ScramCredentialCallback) callback;
                String lookupUser = cb.userName() != null ? cb.userName() : userName;
                String lookupMechanism = cb.mechanism() != null ? cb.mechanism() : mechanism;
                if (lookupUser != null && lookupMechanism != null) {
                    Optional<ScramCredential> maybe =
                            credentialStore.lookup(lookupUser, lookupMechanism);
                    maybe.ifPresent(cb::credential);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
