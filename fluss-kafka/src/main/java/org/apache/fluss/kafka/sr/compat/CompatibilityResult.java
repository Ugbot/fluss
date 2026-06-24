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

package org.apache.fluss.kafka.sr.compat;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Immutable outcome of a Schema Registry compatibility check. {@code compatible=true} when the
 * proposed schema satisfies the requested level against the provided prior versions; {@code
 * messages} carries Avro's per-incompatibility diagnostics (empty on success).
 */
@Internal
public final class CompatibilityResult {

    private final boolean compatible;
    private final List<String> messages;

    private CompatibilityResult(boolean compatible, List<String> messages) {
        this.compatible = compatible;
        this.messages = Collections.unmodifiableList(new ArrayList<>(messages));
    }

    public static CompatibilityResult compatible() {
        return new CompatibilityResult(true, Collections.<String>emptyList());
    }

    public static CompatibilityResult incompatible(List<String> messages) {
        return new CompatibilityResult(false, messages);
    }

    public boolean isCompatible() {
        return compatible;
    }

    /** Per-incompatibility diagnostics from the format-specific checker. Never {@code null}. */
    public List<String> messages() {
        return messages;
    }
}
