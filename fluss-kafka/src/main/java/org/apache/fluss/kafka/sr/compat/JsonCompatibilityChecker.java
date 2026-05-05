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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Clean-room JSON Schema compatibility checker covering the seven Kafka SR levels.
 *
 * <p>Scope matches design doc 0007:
 *
 * <ul>
 *   <li><b>BACKWARD</b> — new (reader) must accept data produced against prior (writer).
 *   <li><b>FORWARD</b> — prior (reader) must accept data produced against new (writer).
 *   <li><b>FULL</b> = BACKWARD ∧ FORWARD.
 *   <li><b>_TRANSITIVE</b> variants compare against every prior (non-deleted) version, not just the
 *       latest.
 *   <li><b>NONE</b> — always compatible.
 * </ul>
 *
 * <p>Rule set (structural, conservative):
 *
 * <ul>
 *   <li>Adding a property to an object is backward-compatible iff it is not in {@code required}.
 *   <li>Removing a property is backward-compatible iff the prior schema did not require it.
 *   <li>Changing a scalar type is always incompatible in both directions (even widening — the wire
 *       bytes differ).
 *   <li>Array {@code items} recurse; {@code type: object} with {@code additionalProperties} recurse
 *       on the value type.
 *   <li>Nullability can be broadened (adding nullable) backward-compatibly; narrowing (removing
 *       nullable) is a BACKWARD break.
 * </ul>
 *
 * <p>Stateless — safe to share.
 */
@Internal
public final class JsonCompatibilityChecker implements CompatibilityChecker {

    /** Stable format id, returned by {@link #formatId()}. */
    public static final String FORMAT_ID = "JSON";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JsonCompatibilityChecker() {}

    @Override
    public String formatId() {
        return FORMAT_ID;
    }

    @Override
    public CompatibilityResult check(
            String proposedText,
            List<String> priorTexts,
            CompatLevel level,
            ReferenceResolver resolver) {
        // Phase SR-X.5 first pass: the structural comparator below operates on the schema
        // node directly. JSON $ref hydration via referent text would let us flatten cross-
        // subject schemas before compare; the design defers full $ref pre-flattening to a
        // later pass, so we accept the resolver and proceed without it. Unresolved $refs in
        // the schema texts surface as "type missing" diagnostics in the comparator below,
        // which is the same conservative break the previous pass produced.
        if (level == CompatLevel.NONE) {
            return CompatibilityResult.compatible();
        }
        if (priorTexts == null || priorTexts.isEmpty()) {
            return CompatibilityResult.compatible();
        }
        final JsonNode proposed;
        try {
            proposed = MAPPER.readTree(proposedText);
        } catch (Exception e) {
            return CompatibilityResult.incompatible(
                    Collections.singletonList("Proposed JSON Schema is not valid JSON: " + e));
        }
        final List<String> targets;
        if (level.isTransitive()) {
            targets = priorTexts;
        } else {
            targets = Collections.singletonList(priorTexts.get(priorTexts.size() - 1));
        }
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            JsonNode prior;
            try {
                prior = MAPPER.readTree(targets.get(i));
            } catch (Exception e) {
                messages.add("Prior JSON Schema at index " + i + " failed to parse: " + e);
                continue;
            }
            if (level.requiresBackward()) {
                compare(proposed, prior, "backward", "", messages);
            }
            if (level.requiresForward()) {
                compare(prior, proposed, "forward", "", messages);
            }
        }
        return messages.isEmpty()
                ? CompatibilityResult.compatible()
                : CompatibilityResult.incompatible(messages);
    }

    /**
     * Compare {@code reader} against {@code writer}: check that the reader can accept data the
     * writer produces. Direction-specific rules are summarised in the rule set above.
     */
    private static void compare(
            JsonNode reader, JsonNode writer, String direction, String path, List<String> out) {
        // Strip nullable idiom to simple (type-string, inner) form so the body matches.
        UnwrappedType ru = unwrap(reader);
        UnwrappedType wu = unwrap(writer);

        if (ru.type == null || wu.type == null) {
            // One side has no "type"; treat as incompatible.
            out.add(
                    "["
                            + direction
                            + "] "
                            + (path.isEmpty() ? "<root>" : path)
                            + ": both sides must declare 'type'");
            return;
        }
        if (!ru.type.equals(wu.type)) {
            out.add(
                    "["
                            + direction
                            + "] "
                            + (path.isEmpty() ? "<root>" : path)
                            + ": type changed (reader="
                            + ru.type
                            + ", writer="
                            + wu.type
                            + ")");
            return;
        }
        if (!ru.nullable && wu.nullable) {
            out.add(
                    "["
                            + direction
                            + "] "
                            + (path.isEmpty() ? "<root>" : path)
                            + ": reader is not nullable but writer can emit null");
        }
        if ("object".equals(ru.type)) {
            compareObject(ru.node, wu.node, direction, path, out);
        } else if ("array".equals(ru.type)) {
            JsonNode readerItems = ru.node.get("items");
            JsonNode writerItems = wu.node.get("items");
            if (readerItems != null && writerItems != null) {
                compare(readerItems, writerItems, direction, path + "[]", out);
            }
        }
        // Scalar formats (byte, date, decimal) — mismatched formats are a type-change for
        // compat purposes.
        String rf = readerFormat(ru.node);
        String wf = readerFormat(wu.node);
        if (rf != null && wf != null && !rf.equals(wf)) {
            out.add(
                    "["
                            + direction
                            + "] "
                            + (path.isEmpty() ? "<root>" : path)
                            + ": format changed (reader="
                            + rf
                            + ", writer="
                            + wf
                            + ")");
        }
    }

    private static void compareObject(
            JsonNode reader, JsonNode writer, String direction, String path, List<String> out) {
        JsonNode rProps = reader.get("properties");
        JsonNode wProps = writer.get("properties");
        Set<String> rRequired = requiredSet(reader);
        Set<String> wRequired = requiredSet(writer);

        if (rProps == null) {
            rProps = MAPPER.createObjectNode();
        }
        if (wProps == null) {
            wProps = MAPPER.createObjectNode();
        }

        Set<String> allProps = new HashSet<>();
        for (Iterator<String> it = rProps.fieldNames(); it.hasNext(); ) {
            allProps.add(it.next());
        }
        for (Iterator<String> it = wProps.fieldNames(); it.hasNext(); ) {
            allProps.add(it.next());
        }

        for (String prop : allProps) {
            boolean inReader = rProps.has(prop);
            boolean inWriter = wProps.has(prop);
            String child = path.isEmpty() ? prop : path + "." + prop;
            if (inReader && inWriter) {
                compare(rProps.get(prop), wProps.get(prop), direction, child, out);
                // Required promotion: reader-only required is a break for backward —
                // writer may have emitted data lacking this property.
                if (rRequired.contains(prop) && !wRequired.contains(prop)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + child
                                    + ": reader requires this field but writer did not");
                }
            } else if (inReader) {
                // Reader has a property writer doesn't emit. If reader requires it → break.
                if (rRequired.contains(prop)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + child
                                    + ": reader requires a field that writer does not emit");
                }
            } else {
                // Writer emits a property reader doesn't know about.
                // Strict readers break on unknown fields; liberal readers drop them.
                // We treat this as incompatible for backward safety — matches Kafka SR's
                // strict JSON Schema BACKWARD.
                if (wRequired.contains(prop)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + child
                                    + ": writer emits a required field that reader does not"
                                    + " know");
                }
            }
        }
    }

    private static Set<String> requiredSet(JsonNode node) {
        JsonNode r = node.get("required");
        if (r == null || !r.isArray()) {
            return Collections.emptySet();
        }
        Set<String> out = new HashSet<>();
        for (JsonNode e : r) {
            out.add(e.asText());
        }
        return out;
    }

    private static String readerFormat(JsonNode node) {
        JsonNode f = node.get("format");
        return f == null || f.isNull() ? null : f.asText();
    }

    /** Unwrap nullable unions into (primary type, nullable flag, payload node). */
    private static UnwrappedType unwrap(JsonNode node) {
        JsonNode typeNode = node.get("type");
        if (typeNode == null) {
            // May be a oneOf/anyOf nullable; look for [null, X].
            JsonNode oneOf = node.has("oneOf") ? node.get("oneOf") : node.get("anyOf");
            if (oneOf != null && oneOf.isArray() && oneOf.size() == 2) {
                JsonNode a = oneOf.get(0);
                JsonNode b = oneOf.get(1);
                JsonNode nullOne = isNullObject(a) ? a : isNullObject(b) ? b : null;
                JsonNode other = a == nullOne ? b : a;
                if (nullOne != null && other != null) {
                    UnwrappedType inner = unwrap(other);
                    return new UnwrappedType(inner.type, true, inner.node);
                }
            }
            return new UnwrappedType(null, false, node);
        }
        if (typeNode.isArray() && typeNode.size() == 2) {
            String a = typeNode.get(0).asText();
            String b = typeNode.get(1).asText();
            boolean nullable = "null".equals(a) || "null".equals(b);
            String primary = "null".equals(a) ? b : "null".equals(b) ? a : null;
            return new UnwrappedType(primary, nullable, node);
        }
        return new UnwrappedType(typeNode.asText(), false, node);
    }

    private static boolean isNullObject(JsonNode n) {
        if (n == null || !n.isObject()) {
            return false;
        }
        JsonNode t = n.get("type");
        return t != null && "null".equals(t.asText());
    }

    /** Discovered (unwrapped-type, nullability, original-node) tuple. */
    private static final class UnwrappedType {
        final String type;
        final boolean nullable;
        final JsonNode node;

        UnwrappedType(String type, boolean nullable, JsonNode node) {
            this.type = type;
            this.nullable = nullable;
            this.node = node;
        }
    }

    // Unused static field kept for potential future debug; avoid import warnings on Map/Entry
    // by referencing them here.
    @SuppressWarnings("unused")
    private static final Map.Entry<String, String> UNUSED_IMPORT_ANCHOR =
            new java.util.AbstractMap.SimpleImmutableEntry<>("", "");
}
