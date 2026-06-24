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

package org.apache.fluss.catalog;

import org.apache.fluss.annotation.Internal;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Process-local registry holding the {@link CatalogService} instance started by {@link
 * FlussCatalogBootstrap} on the coordinator leader. In-process projections (the Kafka Schema
 * Registry service today) resolve the catalog through this registry so they neither have to
 * duplicate the bootstrap path nor directly depend on {@link FlussCatalogService}.
 *
 * <p>This is a deliberately simple global. The catalog has one instance per JVM — it lives and dies
 * with the elected coordinator's leadership, and every projection inside that JVM shares it. A
 * proper dependency-injection container is not warranted for one object.
 */
@Internal
public final class CatalogServices {

    private static final AtomicReference<CatalogService> INSTANCE = new AtomicReference<>();

    private CatalogServices() {}

    /** Register the process-wide catalog instance; called once by {@link FlussCatalogBootstrap}. */
    public static void set(CatalogService service) {
        INSTANCE.set(service);
    }

    /** Clear the registration on leadership loss / shutdown. */
    public static void clear() {
        INSTANCE.set(null);
    }

    /** Current instance, or empty if the catalog isn't running in this JVM. */
    public static Optional<CatalogService> current() {
        return Optional.ofNullable(INSTANCE.get());
    }
}
