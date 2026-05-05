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
import org.apache.fluss.kafka.sr.references.ReferenceResolver;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.SchemaParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thin wrapper around {@link SchemaCompatibility#checkReaderWriterCompatibility(Schema, Schema)}
 * that implements the seven Kafka SR compatibility levels. Stateless — safe to share.
 *
 * <p>Kafka SR semantics mapping (reader/writer direction is load-bearing):
 *
 * <ul>
 *   <li><b>BACKWARD / BACKWARD_TRANSITIVE</b> — proposed schema must read old data → proposed is
 *       <i>reader</i>, prior is <i>writer</i>.
 *   <li><b>FORWARD / FORWARD_TRANSITIVE</b> — prior schema must read new data → prior is
 *       <i>reader</i>, proposed is <i>writer</i>.
 *   <li><b>FULL / FULL_TRANSITIVE</b> — both of the above.
 *   <li><b>NONE</b> — always compatible, never parses.
 * </ul>
 *
 * <p>Non-transitive levels check only against the most recent prior (the last element of the {@code
 * priorTexts} list as passed by the SR service, which orders prior versions ascending).
 */
@Internal
public final class AvroCompatibilityChecker implements CompatibilityChecker {

    public AvroCompatibilityChecker() {}

    @Override
    public String formatId() {
        return "AVRO";
    }

    /**
     * Check {@code proposedText} against {@code priorTexts} at {@code level} with {@code
     * resolver}'s referent named-types pre-loaded into every Avro {@link Schema.Parser}. The
     * resolver supplies texts for any cross-subject named types the proposed (or any prior) schema
     * imports — Avro accepts repeated {@code parse(String)} calls and accumulates the named-type
     * table across them, so we feed every referent text first, then the schema under test.
     *
     * @param proposedText proposed Avro schema JSON (required)
     * @param priorTexts prior Avro schema JSONs in registration order (oldest first). May be empty
     *     — a first registration is always compatible.
     * @param level Kafka SR compatibility level
     * @param resolver references bound at register time; pass {@link ReferenceResolver#empty()}
     *     when none.
     */
    @Override
    public CompatibilityResult check(
            String proposedText,
            List<String> priorTexts,
            CompatLevel level,
            ReferenceResolver resolver) {
        if (level == CompatLevel.NONE) {
            return CompatibilityResult.compatible();
        }
        if (priorTexts == null || priorTexts.isEmpty()) {
            // First registration — nothing to be incompatible with.
            return CompatibilityResult.compatible();
        }
        Schema proposed;
        try {
            proposed = parseWithReferences(proposedText, resolver);
        } catch (SchemaParseException spe) {
            return CompatibilityResult.incompatible(
                    Collections.singletonList(
                            "Proposed schema is not valid Avro: " + spe.getMessage()));
        }

        List<String> targetPriors;
        if (level.isTransitive()) {
            targetPriors = priorTexts;
        } else {
            // Only the latest prior — priors are supplied oldest-first, latest is last.
            targetPriors = Collections.singletonList(priorTexts.get(priorTexts.size() - 1));
        }

        List<String> accumulated = new ArrayList<>();
        for (int i = 0; i < targetPriors.size(); i++) {
            String priorText = targetPriors.get(i);
            Schema prior;
            try {
                prior = parseWithReferences(priorText, resolver);
            } catch (SchemaParseException spe) {
                accumulated.add(
                        "Prior schema at index " + i + " failed to parse: " + spe.getMessage());
                continue;
            }
            if (level.requiresBackward()) {
                // Proposed (reader) must read data written by prior (writer).
                collectIncompatibilities(proposed, prior, "backward", accumulated);
            }
            if (level.requiresForward()) {
                // Prior (reader) must read data written by proposed (writer).
                collectIncompatibilities(prior, proposed, "forward", accumulated);
            }
        }

        if (accumulated.isEmpty()) {
            return CompatibilityResult.compatible();
        }
        return CompatibilityResult.incompatible(accumulated);
    }

    /**
     * Parse {@code schemaText} with every referent named-type from {@code resolver} pre-registered.
     * Avro's {@link Schema.Parser} accumulates named types across {@code parse(String)} calls; a
     * referent that fails to parse is skipped (the referrer's parse will then surface a cleaner
     * "unknown type" error rather than the referent's syntax error).
     */
    private static Schema parseWithReferences(String schemaText, ReferenceResolver resolver) {
        Schema.Parser parser = new Schema.Parser();
        if (resolver != null) {
            for (String name : resolver.names()) {
                resolver.resolve(name)
                        .ifPresent(
                                referentText -> {
                                    try {
                                        parser.parse(referentText);
                                    } catch (SchemaParseException ignored) {
                                        // Defer: referrer parse will surface the unknown name.
                                    }
                                });
            }
        }
        return parser.parse(schemaText);
    }

    private static void collectIncompatibilities(
            Schema reader, Schema writer, String direction, List<String> out) {
        SchemaPairCompatibility result =
                SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
        if (result.getType() == SchemaCompatibilityType.COMPATIBLE) {
            return;
        }
        List<Incompatibility> details = result.getResult().getIncompatibilities();
        if (details == null || details.isEmpty()) {
            out.add("[" + direction + "] " + result.getDescription());
            return;
        }
        for (Incompatibility inc : details) {
            StringBuilder sb = new StringBuilder();
            sb.append('[').append(direction).append("] ");
            if (inc.getType() != null) {
                sb.append(inc.getType().name()).append(": ");
            }
            String message = inc.getMessage();
            sb.append(message == null ? "incompatible" : message);
            String location = inc.getLocation();
            if (location != null && !location.isEmpty()) {
                sb.append(" (at ").append(location).append(')');
            }
            out.add(sb.toString());
        }
    }
}
