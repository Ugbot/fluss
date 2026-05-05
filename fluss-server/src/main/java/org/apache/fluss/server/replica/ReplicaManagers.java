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

package org.apache.fluss.server.replica;

import org.apache.fluss.annotation.Internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * JVM-wide registry of all live {@link ReplicaManager} instances. In a single-process test cluster
 * every tablet server registers here; the transaction marker fan-out path uses the registry to
 * locate the leader replica when the marker is being written by a non-leader tablet server.
 *
 * <p>In a real multi-process deployment each process holds exactly one entry. Iteration over a
 * single-entry list degrades to the existing behaviour: try local, fail if not leader.
 */
@Internal
public final class ReplicaManagers {

    private static final List<ReplicaManager> MANAGERS = new CopyOnWriteArrayList<>();

    private ReplicaManagers() {}

    public static void register(ReplicaManager manager) {
        MANAGERS.add(manager);
    }

    public static void unregister(ReplicaManager manager) {
        MANAGERS.remove(manager);
    }

    /** Returns a snapshot of all currently registered managers. */
    public static List<ReplicaManager> all() {
        return MANAGERS;
    }
}
