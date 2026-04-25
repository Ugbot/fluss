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

package org.apache.fluss.kafka.tx;

import org.apache.fluss.annotation.Internal;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-local registry for the {@link TransactionCoordinator} singleton, mirroring {@code
 * org.apache.fluss.catalog.CatalogServices}. The coordinator is hosted on the elected
 * coordinator-leader; the {@code TransactionCoordinatorBootstrap} registers it here on leadership
 * acquisition and clears it on leadership loss.
 *
 * <p>Kafka request handlers running on tablet servers consult this registry to find the
 * coordinator. When the local JVM is not the coordinator-leader the registry is empty and the
 * request handler must route the call to the leader (Phase J.1: a routing failure is acceptable
 * because {@code INIT_PRODUCER_ID} is bracketed by {@code FindCoordinator}, which already returns
 * the leader's endpoint).
 */
@Internal
public final class TransactionCoordinators {

    private static final AtomicReference<TransactionCoordinator> INSTANCE = new AtomicReference<>();

    private TransactionCoordinators() {}

    /** Register the process-wide coordinator instance; called once by the bootstrap. */
    public static void set(TransactionCoordinator coord) {
        INSTANCE.set(coord);
    }

    /** Clear on leadership loss / shutdown. */
    public static void clear() {
        INSTANCE.set(null);
    }

    /** Current instance, or empty if the coordinator isn't running on this JVM. */
    public static Optional<TransactionCoordinator> current() {
        return Optional.ofNullable(INSTANCE.get());
    }
}
