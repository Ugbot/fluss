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

package org.apache.fluss.security.auth.sasl.jaas;

import org.apache.fluss.security.auth.sasl.plain.PlainServerCallbackHandler;
import org.apache.fluss.security.auth.sasl.scram.JaasFileScramCredentialStore;
import org.apache.fluss.security.auth.sasl.scram.ScramCredentialStore;
import org.apache.fluss.security.auth.sasl.scram.ScramMechanism;
import org.apache.fluss.security.auth.sasl.scram.ScramSaslServerProvider;
import org.apache.fluss.security.auth.sasl.scram.ScramServerCallbackHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Factory class for creating SASL servers and clients. */
public class SaslServerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerFactory.class);

    /**
     * Globally-registered SCRAM credential store for server-side credential lookup.
     *
     * <p>Tests and bolt-on integrations install a store before the first {@link #createSaslServer}
     * call. If unset, a dev-mode {@link JaasFileScramCredentialStore} is used; credentials can be
     * registered at runtime via {@link JaasFileScramCredentialStore#upsert}.
     */
    private static volatile ScramCredentialStore scramCredentialStore =
            new JaasFileScramCredentialStore();

    /** Overrides the SCRAM credential store used by all SASL servers created afterwards. */
    public static void setScramCredentialStore(ScramCredentialStore store) {
        if (store == null) {
            throw new IllegalArgumentException("scram credential store must not be null");
        }
        scramCredentialStore = store;
    }

    /** Returns the currently-registered SCRAM credential store. */
    public static ScramCredentialStore scramCredentialStore() {
        return scramCredentialStore;
    }

    public static SaslServer createSaslServer(
            String mechanism,
            String hostName,
            Map<String, ?> props,
            LoginManager loginManager,
            List<AppConfigurationEntry> configurationEntries)
            throws SaslException {

        try {

            AuthenticateCallbackHandler callbackHandler;
            if (mechanism.equals("PLAIN")) {
                callbackHandler = new PlainServerCallbackHandler();
            } else if (ScramMechanism.isScram(mechanism)) {
                ScramSaslServerProvider.initialize();
                callbackHandler = new ScramServerCallbackHandler(scramCredentialStore);
            } else {
                throw new IllegalArgumentException("Unsupported mechanism: " + mechanism);
            }

            callbackHandler.configure(mechanism, configurationEntries);
            SaslServer saslServer =
                    Subject.doAs(
                            loginManager.subject(),
                            (PrivilegedExceptionAction<SaslServer>)
                                    () ->
                                            Sasl.createSaslServer(
                                                    mechanism,
                                                    "fluss",
                                                    hostName,
                                                    props,
                                                    callbackHandler));
            if (saslServer == null) {
                throw new SaslException(
                        "Fluss Server failed to create a SaslServer to interact with a client during session authentication with server mechanism "
                                + mechanism);
            }

            return saslServer;
        } catch (PrivilegedActionException e) {
            throw new SaslException(
                    "Fluss Server failed to create a SaslServer to interact with a client during session authentication with server mechanism "
                            + mechanism,
                    e.getCause());
        }
    }

    public static SaslClient createSaslClient(
            String mechanism, String hostAddress, Map<String, ?> props, LoginManager loginManager)
            throws PrivilegedActionException {

        return Subject.doAs(
                loginManager.subject(),
                (PrivilegedExceptionAction<SaslClient>)
                        () -> {
                            String[] mechs = {mechanism};
                            String serviceName = loginManager.serviceName();
                            LOG.debug(
                                    "Creating SaslClient: service={};mechs={}",
                                    serviceName,
                                    Arrays.toString(mechs));

                            return Sasl.createSaslClient(
                                    mechs,
                                    null,
                                    serviceName,
                                    hostAddress,
                                    props,
                                    new SaslClientCallbackHandler());
                        });
    }
}
