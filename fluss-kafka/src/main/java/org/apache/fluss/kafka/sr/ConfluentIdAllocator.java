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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Allocates Confluent-style global schema ids as a deterministic function of {@code (tableId,
 * schemaVersion, format)} with ZK CAS to resolve hash collisions.
 *
 * <p>Base id = {@code hash(tableId, schemaVersion, format) & 0x7fffffff}. If the resulting znode at
 * {@link SchemaRegistryPaths#idReservation(int)} already exists and holds the exact same {@code
 * (tableId, schemaVersion, format)} tuple, we treat the allocation as idempotent and return that
 * id. Otherwise we linearly probe the next id (bounded to 8 tries). A collision past the probe
 * budget is an {@link SchemaRegistryException} with {@link SchemaRegistryException.Kind#INTERNAL} —
 * astronomically unlikely with a 31-bit space and a reasonable cluster size, but we still fail
 * loudly rather than silently reuse a stranger's id.
 */
@Internal
public final class ConfluentIdAllocator {

    private static final int PROBE_LIMIT = 8;
    private static final String SEP = "\t";

    private final ZooKeeperClient zk;

    public ConfluentIdAllocator(ZooKeeperClient zk) {
        this.zk = zk;
    }

    public int allocate(long tableId, int schemaVersion, String format) {
        int base = stableHash(tableId, schemaVersion, format) & 0x7fffffff;
        String expected = encode(tableId, schemaVersion, format);
        CuratorFramework curator = zk.getCuratorClient();
        for (int probe = 0; probe < PROBE_LIMIT; probe++) {
            int id = (base + probe) & 0x7fffffff;
            String path = SchemaRegistryPaths.idReservation(id);
            try {
                curator.create()
                        .creatingParentsIfNeeded()
                        .forPath(path, expected.getBytes(StandardCharsets.UTF_8));
                return id;
            } catch (KeeperException.NodeExistsException collision) {
                try {
                    byte[] existing = curator.getData().forPath(path);
                    if (expected.equals(new String(existing, StandardCharsets.UTF_8))) {
                        return id;
                    }
                } catch (Exception readFailure) {
                    throw new SchemaRegistryException(
                            SchemaRegistryException.Kind.INTERNAL,
                            "Failed to inspect existing SR id reservation at " + path,
                            readFailure);
                }
                // else: different tuple at this id, probe next.
            } catch (Exception other) {
                throw new SchemaRegistryException(
                        SchemaRegistryException.Kind.INTERNAL,
                        "Failed to reserve SR id at " + path,
                        other);
            }
        }
        throw new SchemaRegistryException(
                SchemaRegistryException.Kind.INTERNAL,
                "Could not reserve a Confluent SR id for ("
                        + tableId
                        + ", "
                        + schemaVersion
                        + ", "
                        + format
                        + ") after "
                        + PROBE_LIMIT
                        + " probes");
    }

    /** Look up a reservation, returning the stored tuple or empty if the id is free. */
    public Optional<Reservation> lookup(int id) {
        String path = SchemaRegistryPaths.idReservation(id);
        try {
            byte[] data = zk.getCuratorClient().getData().forPath(path);
            return Optional.of(decode(new String(data, StandardCharsets.UTF_8)));
        } catch (KeeperException.NoNodeException missing) {
            return Optional.empty();
        } catch (Exception other) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "Failed to read SR id reservation at " + path,
                    other);
        }
    }

    private static int stableHash(long tableId, int schemaVersion, String format) {
        // A fixed polynomial hash so that the same tuple always produces the same base id, across
        // restarts and across JVM runs. {@link Object#hashCode()} is not stable, and {@link
        // String#hashCode()} is stable but combining with {@code ^} collapses bits — use 31.
        int h = (int) (tableId ^ (tableId >>> 32));
        h = h * 31 + schemaVersion;
        h = h * 31 + format.hashCode();
        return h;
    }

    private static String encode(long tableId, int schemaVersion, String format) {
        return tableId + SEP + schemaVersion + SEP + format;
    }

    private static Reservation decode(String text) {
        String[] parts = text.split(SEP, 3);
        if (parts.length != 3) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INTERNAL,
                    "Malformed SR id reservation payload: " + text);
        }
        return new Reservation(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), parts[2]);
    }

    /** Tuple stored at {@code /fluss/kafka-sr/id-reservations/<id>}. */
    public static final class Reservation {
        private final long tableId;
        private final int schemaVersion;
        private final String format;

        public Reservation(long tableId, int schemaVersion, String format) {
            this.tableId = tableId;
            this.schemaVersion = schemaVersion;
            this.format = format;
        }

        public long tableId() {
            return tableId;
        }

        public int schemaVersion() {
            return schemaVersion;
        }

        public String format() {
            return format;
        }
    }
}
