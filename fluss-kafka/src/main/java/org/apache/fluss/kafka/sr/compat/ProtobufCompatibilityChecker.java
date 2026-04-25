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

/*
 * Design note: this file is a clean-room implementation of Protobuf compatibility checking.
 * Confluent's own kafka-protobuf-provider.jar is released under the Confluent Community License,
 * which is not ASL 2.0 — so we can't shade it. The rules below derive from Google's Protocol
 * Buffers v3 specification (rules for adding/removing/renaming fields and tag numbers).
 */

package org.apache.fluss.kafka.sr.compat;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.sr.references.ReferenceResolver;
import org.apache.fluss.kafka.sr.typed.ProtoParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Clean-room Protobuf compatibility checker covering the seven Confluent SR levels.
 *
 * <p>Rule set (both directions share the underlying "tag numbers are load-bearing; names are not"
 * invariant; direction determines which tag-additions are breaking):
 *
 * <ul>
 *   <li><b>Tag immutability</b>: a tag number that exists in both sides must carry the same wire
 *       type — removing a tag and re-using its number with a different type is a break in every
 *       level except {@code NONE}.
 *   <li><b>BACKWARD</b>: new (reader) must accept data produced against prior (writer). Adding a
 *       new tag is allowed (reader skips unknowns); removing a tag the writer still emits is
 *       allowed (reader discards it); changing a tag's wire type is broken.
 *   <li><b>FORWARD</b>: prior (reader) must accept data produced against new (writer). Same
 *       tag-immutability; adding a tag the reader doesn't know is allowed (reader skips it);
 *       removing a tag the reader requires (repeated/required) is a break.
 *   <li><b>FULL</b> = BACKWARD ∧ FORWARD.
 *   <li><b>_TRANSITIVE</b> variants compare against every prior (non-deleted) version, not just the
 *       latest.
 *   <li><b>NONE</b> — always compatible.
 * </ul>
 *
 * <p>For proto2, changing a field's cardinality (optional ↔ required) is treated as a type change.
 * For proto3, adding {@code optional} is an additive change (presence tracking) and does not break
 * compat.
 *
 * <p>Stateless — safe to share.
 */
@Internal
public final class ProtobufCompatibilityChecker implements CompatibilityChecker {

    /** Stable format id. */
    public static final String FORMAT_ID = "PROTOBUF";

    public ProtobufCompatibilityChecker() {}

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
        // Phase SR-X.5 first pass: the comparator works against the proto's parsed message
        // tree. Cross-file imports would let the parser see referent message definitions,
        // but our checker compares wire shapes (tag numbers, types) of the top-level
        // message; an unresolved import doesn't affect that. The resolver is accepted to
        // satisfy the SPI; richer resolution lands when the typed Protobuf descriptor
        // builder is wired in (design 0008 §6).
        if (level == CompatLevel.NONE) {
            return CompatibilityResult.compatible();
        }
        if (priorTexts == null || priorTexts.isEmpty()) {
            return CompatibilityResult.compatible();
        }
        final ProtoParser.ProtoFile proposed;
        try {
            proposed = parse(proposedText);
        } catch (Exception e) {
            return CompatibilityResult.incompatible(
                    Collections.singletonList(
                            "Proposed Protobuf schema failed to parse: " + e.getMessage()));
        }
        final List<String> targets;
        if (level.isTransitive()) {
            targets = priorTexts;
        } else {
            targets = Collections.singletonList(priorTexts.get(priorTexts.size() - 1));
        }
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            ProtoParser.ProtoFile prior;
            try {
                prior = parse(targets.get(i));
            } catch (Exception e) {
                messages.add(
                        "Prior Protobuf schema at index "
                                + i
                                + " failed to parse: "
                                + e.getMessage());
                continue;
            }
            if (proposed.messages.isEmpty() || prior.messages.isEmpty()) {
                messages.add("Both schemas must declare at least one top-level message");
                continue;
            }
            ProtoParser.ProtoMessage pr = proposed.messages.get(0);
            ProtoParser.ProtoMessage pw = prior.messages.get(0);
            if (level.requiresBackward()) {
                compareMessage(pr, pw, "backward", "", messages);
            }
            if (level.requiresForward()) {
                compareMessage(pw, pr, "forward", "", messages);
            }
        }
        return messages.isEmpty()
                ? CompatibilityResult.compatible()
                : CompatibilityResult.incompatible(messages);
    }

    private static ProtoParser.ProtoFile parse(String text) {
        return new ProtoParser(text).parseFile();
    }

    private static void compareMessage(
            ProtoParser.ProtoMessage reader,
            ProtoParser.ProtoMessage writer,
            String direction,
            String path,
            List<String> out) {
        Map<Integer, ProtoParser.ProtoField> rFields = byTag(allFields(reader));
        Map<Integer, ProtoParser.ProtoField> wFields = byTag(allFields(writer));

        Set<Integer> allTags = new HashSet<>();
        allTags.addAll(rFields.keySet());
        allTags.addAll(wFields.keySet());

        for (int tag : allTags) {
            ProtoParser.ProtoField r = rFields.get(tag);
            ProtoParser.ProtoField w = wFields.get(tag);
            String where = (path.isEmpty() ? reader.name : path) + ".#" + tag;
            if (r != null && w != null) {
                if (!wireCompatible(r, w)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + where
                                    + ": tag "
                                    + tag
                                    + " changed wire type (reader='"
                                    + r.type
                                    + (r.modifier == null ? "" : "', modifier='" + r.modifier)
                                    + "', writer='"
                                    + w.type
                                    + (w.modifier == null ? "" : "', modifier='" + w.modifier)
                                    + "')");
                }
            } else if (r != null) {
                // Reader knows tag, writer doesn't emit it.
                if ("required".equals(r.modifier) || "repeated".equals(r.modifier)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + where
                                    + ": reader requires tag "
                                    + tag
                                    + " but writer never emits it");
                }
            } else {
                // Writer emits a tag reader doesn't know. In proto3 that's fine (reader skips
                // unknown fields). Required writers in proto2, though: skippable only if reader
                // is lax.
                if ("required".equals(w.modifier)) {
                    out.add(
                            "["
                                    + direction
                                    + "] "
                                    + where
                                    + ": writer emits required tag "
                                    + tag
                                    + " that reader does not know");
                }
            }
        }
    }

    private static List<ProtoParser.ProtoField> allFields(ProtoParser.ProtoMessage m) {
        List<ProtoParser.ProtoField> out = new ArrayList<>(m.fields);
        for (ProtoParser.ProtoOneof o : m.oneofs) {
            out.addAll(o.fields);
        }
        return out;
    }

    private static Map<Integer, ProtoParser.ProtoField> byTag(List<ProtoParser.ProtoField> fields) {
        Map<Integer, ProtoParser.ProtoField> out = new HashMap<>();
        for (ProtoParser.ProtoField f : fields) {
            out.put(f.tag, f);
        }
        return out;
    }

    private static boolean wireCompatible(ProtoParser.ProtoField a, ProtoParser.ProtoField b) {
        if (!a.type.equals(b.type)) {
            return false;
        }
        // Allow proto3 optional (presence tracking) to be added/removed — not a wire-type change.
        String ma = a.modifier == null ? "" : a.modifier;
        String mb = b.modifier == null ? "" : b.modifier;
        if (ma.equals(mb)) {
            return true;
        }
        // repeated on one side, non-repeated on the other is a wire-type change.
        if ("repeated".equals(ma) || "repeated".equals(mb)) {
            return false;
        }
        // required vs optional: changing presence affects what prior readers accept —
        // conservative: treat as incompatible. Confluent does the same for strict BACKWARD.
        if ("required".equals(ma) || "required".equals(mb)) {
            return false;
        }
        // proto3 optional vs no modifier: both wire-compatible (same tag, same type, presence
        // tracking is additive).
        return true;
    }
}
