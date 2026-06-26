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

package org.apache.fluss.lake.tiering.coordinator;

import org.apache.fluss.annotation.Internal;

/**
 * A small mutable holder for the Fluss coordinator epoch tracked by the tiering service.
 *
 * <p>The coordinator epoch is obtained from the registration handshake (the first {@code
 * lakeTieringHeartbeat} response) and is refreshed on every subsequent heartbeat response. It must
 * be echoed back on every per-table heartbeat request so the coordinator can fence stale requests
 * (see {@code LakeTableTieringManager#validateTieringServiceRequest} on the server side). This
 * replaces the {@code int flussCoordinatorEpoch} field that {@code TieringSourceEnumerator} tracked
 * inline.
 *
 * <p>This class is thread-safe: the epoch is stored in a {@code volatile} field so a reader thread
 * always observes the most recently set value.
 */
@Internal
public final class CoordinatorEpoch {

    /** Sentinel value indicating the epoch has not yet been established via the handshake. */
    public static final int UNKNOWN = -1;

    private volatile int epoch;

    public CoordinatorEpoch() {
        this.epoch = UNKNOWN;
    }

    public CoordinatorEpoch(int epoch) {
        this.epoch = epoch;
    }

    /** Returns the current coordinator epoch, or {@link #UNKNOWN} if not yet established. */
    public int get() {
        return epoch;
    }

    /** Updates the tracked coordinator epoch (typically from a heartbeat response). */
    public void set(int epoch) {
        this.epoch = epoch;
    }

    /** Returns {@code true} once the epoch has been established via the registration handshake. */
    public boolean isEstablished() {
        return epoch != UNKNOWN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordinatorEpoch that = (CoordinatorEpoch) o;
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(epoch);
    }

    @Override
    public String toString() {
        return "CoordinatorEpoch{" + "epoch=" + epoch + '}';
    }
}
