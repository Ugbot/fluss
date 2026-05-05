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
import org.apache.fluss.kafka.sr.references.ReferenceResolver;

import java.util.List;

/**
 * Per-format compatibility checker SPI. Looked up by {@link #formatId()} from the {@code
 * FormatRegistry} so the Schema Registry service can dispatch the right checker for a given
 * subject's stored {@code format} column.
 *
 * <p>Plan §27.3. The {@link AvroCompatibilityChecker} was the first concrete implementation (and
 * became this SPI's reference shape); JSON Schema and Protobuf checkers follow the same contract.
 * Phase SR-X.5 (design 0013) widens the SPI with a {@link ReferenceResolver} parameter so that
 * format parsers can hydrate cross-subject named types (Avro fullnames, JSON {@code $ref}, {@code
 * .proto} imports) before parsing the referrer.
 *
 * <p>Implementations MUST be thread-safe and stateless.
 *
 * @since 0.9
 */
@PublicEvolving
public interface CompatibilityChecker {

    /**
     * Check {@code proposedText} against {@code priorTexts} at {@code level}, with no references
     * available. Equivalent to {@link #check(String, List, CompatLevel, ReferenceResolver)} with
     * {@link ReferenceResolver#empty()}.
     *
     * <p>Default-method bridge — concrete implementations should override the 4-argument form
     * instead. Kept for source compatibility with callers that haven't migrated to the resolver
     * variant yet.
     *
     * @param proposedText the proposed schema text in this checker's format
     * @param priorTexts prior schema texts in registration order (oldest first). May be empty, in
     *     which case any first registration is compatible.
     * @param level the Kafka SR compatibility level to apply
     * @return a {@link CompatibilityResult} with the outcome and any per-incompatibility messages
     */
    default CompatibilityResult check(
            String proposedText, List<String> priorTexts, CompatLevel level) {
        return check(proposedText, priorTexts, level, ReferenceResolver.empty());
    }

    /**
     * Check {@code proposedText} against {@code priorTexts} at {@code level}, allowing the parser
     * to consult {@code resolver} for any cross-schema references the proposed text declares (Avro
     * named-type imports, JSON {@code $ref}, Protobuf imports).
     *
     * @param proposedText the proposed schema text in this checker's format
     * @param priorTexts prior schema texts in registration order (oldest first)
     * @param level the Kafka SR compatibility level to apply
     * @param resolver reference resolver; never {@code null} — pass {@link
     *     ReferenceResolver#empty()} when there are no references
     */
    CompatibilityResult check(
            String proposedText,
            List<String> priorTexts,
            CompatLevel level,
            ReferenceResolver resolver);

    /**
     * Stable format id this checker handles (for example {@code "AVRO"}, {@code "JSON"}, {@code
     * "PROTOBUF"}). Case-insensitive on lookup but this method should return the canonical
     * upper-case form.
     */
    String formatId();
}
