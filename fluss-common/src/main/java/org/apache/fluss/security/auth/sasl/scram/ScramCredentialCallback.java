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

/**
 * Callback used by the SCRAM server to retrieve a {@link ScramCredential} for a given principal.
 *
 * <p>The callback handler (see {@link ScramServerCallbackHandler}) sets the credential via {@link
 * #credential(ScramCredential)} when the lookup succeeds. A null credential signals an unknown
 * user; authentication fails without leaking that information to the peer.
 */
public class ScramCredentialCallback implements Callback {

    private final String mechanism;
    private String userName;
    private ScramCredential credential;

    public ScramCredentialCallback(String mechanism) {
        this.mechanism = mechanism;
    }

    public String mechanism() {
        return mechanism;
    }

    public String userName() {
        return userName;
    }

    public void userName(String userName) {
        this.userName = userName;
    }

    public ScramCredential credential() {
        return credential;
    }

    public void credential(ScramCredential credential) {
        this.credential = credential;
    }
}
