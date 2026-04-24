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

import org.apache.fluss.annotation.PublicEvolving;

import java.util.List;

/**
 * Per-format compatibility checker SPI. Looked up by {@link #formatId()} from the {@code
 * FormatRegistry} so the Schema Registry service can dispatch the right checker for a given
 * subject's stored {@code format} column.
 *
 * <p>Plan §27.3. The {@link AvroCompatibilityChecker} was the first concrete implementation (and
 * became this SPI's reference shape); JSON Schema and Protobuf checkers follow the same contract.
 *
 * <p>Implementations MUST be thread-safe and stateless.
 *
 * @since 0.9
 */
@PublicEvolving
public interface CompatibilityChecker {

    /**
     * Check {@code proposedText} against {@code priorTexts} at {@code level}.
     *
     * @param proposedText the proposed schema text in this checker's format
     * @param priorTexts prior schema texts in registration order (oldest first). May be empty, in
     *     which case any first registration is compatible.
     * @param level the Confluent SR compatibility level to apply
     * @return a {@link CompatibilityResult} with the outcome and any per-incompatibility messages
     */
    CompatibilityResult check(String proposedText, List<String> priorTexts, CompatLevel level);

    /**
     * Stable format id this checker handles (for example {@code "AVRO"}, {@code "JSON"}, {@code
     * "PROTOBUF"}). Case-insensitive on lookup but this method should return the canonical
     * upper-case form.
     */
    String formatId();
}
