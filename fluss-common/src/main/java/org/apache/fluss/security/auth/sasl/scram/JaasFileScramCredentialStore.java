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

import org.apache.fluss.annotation.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Dev-mode {@link ScramCredentialStore} backed by an in-memory map typically populated from a JAAS
 * options file or a server-startup config block.
 *
 * <p>The expected JAAS options shape is:
 *
 * <pre>
 *   user_alice = "SCRAM-SHA-256=[iterations=4096,password=alice-secret]"
 *   user_bob   = "SCRAM-SHA-512=[iterations=4096,password=bob-secret]"
 * </pre>
 *
 * <p>At load time, the store derives the stored-key / server-key via {@link
 * ScramCredentialUtils#deriveCredential} using a random per-user salt. The store is mutable so that
 * unit tests can add credentials after construction — production deployments should prefer the
 * catalog-backed implementation (see Phase H.2).
 */
public final class JaasFileScramCredentialStore implements ScramCredentialStore {

    private final Map<Key, ScramCredential> credentials = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public JaasFileScramCredentialStore() {}

    /** Constructs a store pre-populated with plain-password entries for dev / test. */
    public JaasFileScramCredentialStore(Map<String, Map<String, String>> users) {
        checkNotNull(users, "users");
        for (Map.Entry<String, Map<String, String>> userEntry : users.entrySet()) {
            String principal = userEntry.getKey();
            for (Map.Entry<String, String> mechEntry : userEntry.getValue().entrySet()) {
                String mechName = mechEntry.getKey();
                String password = mechEntry.getValue();
                ScramMechanism mech = ScramMechanism.forMechanismName(mechName);
                if (mech == null) {
                    throw new IllegalArgumentException("Unknown SCRAM mechanism: " + mechName);
                }
                addPlainPassword(principal, mech, password, mech.minIterations());
            }
        }
    }

    /** Derives a credential from a plain password and stores it. */
    public void addPlainPassword(
            String principalName, ScramMechanism mechanism, String password, int iterations) {
        byte[] salt = ScramCredentialUtils.randomSalt();
        ScramCredential credential =
                ScramCredentialUtils.deriveCredential(mechanism, password, salt, iterations);
        upsert(principalName, mechanism.mechanismName(), credential);
    }

    @Override
    public Optional<ScramCredential> lookup(String principalName, String mechanism) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(credentials.get(new Key(principalName, mechanism)));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String principalName, String mechanism, ScramCredential credential) {
        lock.writeLock().lock();
        try {
            credentials.put(new Key(principalName, mechanism), credential);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String principalName, String mechanism) {
        lock.writeLock().lock();
        try {
            credentials.remove(new Key(principalName, mechanism));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<Entry> list() {
        lock.readLock().lock();
        try {
            List<Entry> entries = new ArrayList<>(credentials.size());
            for (Map.Entry<Key, ScramCredential> e : credentials.entrySet()) {
                entries.add(
                        new Entry(
                                e.getKey().principalName,
                                e.getKey().mechanism,
                                e.getValue().iterations()));
            }
            return Collections.unmodifiableList(entries);
        } finally {
            lock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public int size() {
        lock.readLock().lock();
        try {
            return credentials.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static final class Key {
        final String principalName;
        final String mechanism;

        Key(String principalName, String mechanism) {
            this.principalName = principalName;
            this.mechanism = mechanism;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key other = (Key) o;
            return principalName.equals(other.principalName) && mechanism.equals(other.mechanism);
        }

        @Override
        public int hashCode() {
            return 31 * principalName.hashCode() + mechanism.hashCode();
        }
    }
}
