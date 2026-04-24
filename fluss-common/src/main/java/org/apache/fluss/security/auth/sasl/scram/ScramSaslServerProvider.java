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

import java.security.Provider;
import java.security.Security;

/**
 * JCA provider registering Fluss's {@link ScramSaslServer.ScramSaslServerFactory} for the SCRAM
 * mechanisms. {@link #initialize()} is idempotent.
 */
public class ScramSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;
    private static volatile boolean initialized;

    @SuppressWarnings("this-escape")
    protected ScramSaslServerProvider() {
        super(
                "Fluss SASL/SCRAM Server Provider",
                1.0,
                "Fluss SASL/SCRAM Server Provider for SCRAM-SHA-256 and SCRAM-SHA-512");
        for (String mechanism : ScramMechanism.mechanismNames()) {
            put(
                    "SaslServerFactory." + mechanism,
                    ScramSaslServer.ScramSaslServerFactory.class.getName());
        }
    }

    /** Idempotently registers this provider with the JCA. */
    public static synchronized void initialize() {
        if (initialized) {
            return;
        }
        Security.addProvider(new ScramSaslServerProvider());
        initialized = true;
    }
}
