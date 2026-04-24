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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ProtobufCompatibilityChecker}. */
class ProtobufCompatibilityCheckerTest {

    private final ProtobufCompatibilityChecker checker = new ProtobufCompatibilityChecker();

    private static final String BASE =
            "syntax = \"proto3\";\n"
                    + "message M {\n"
                    + "  int32 id = 1;\n"
                    + "  string name = 2;\n"
                    + "}\n";

    private static final String BASE_ADD_FIELD =
            "syntax = \"proto3\";\n"
                    + "message M {\n"
                    + "  int32 id = 1;\n"
                    + "  string name = 2;\n"
                    + "  string nick = 3;\n"
                    + "}\n";

    private static final String BASE_TYPE_CHANGED =
            "syntax = \"proto3\";\n"
                    + "message M {\n"
                    + "  int32 id = 1;\n"
                    + "  bytes name = 2;\n"
                    + "}\n";

    private static final String BASE_DROP_FIELD =
            "syntax = \"proto3\";\n" + "message M {\n" + "  int32 id = 1;\n" + "}\n";

    private static final String BASE_TAG_REUSED =
            "syntax = \"proto3\";\n"
                    + "message M {\n"
                    + "  int32 id = 1;\n"
                    + "  bool name = 2;\n"
                    + "}\n";

    @Test
    void noneAlwaysPasses() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.NONE)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void firstRegistrationPassesAtEveryLevel() {
        for (CompatLevel level : CompatLevel.values()) {
            assertThat(checker.check(BASE, Collections.emptyList(), level).isCompatible())
                    .as("empty prior list at " + level)
                    .isTrue();
        }
    }

    // --- BACKWARD ----------------------------------------------------------

    @Test
    void backwardPass_addField() {
        // New (reader) adds a field; writer (prior) never emits it — reader doesn't care.
        assertThat(
                        checker.check(BASE_ADD_FIELD, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void backwardPass_dropField() {
        // New (reader) drops a field; writer (prior) still emits it — reader skips unknown.
        assertThat(
                        checker.check(BASE_DROP_FIELD, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void backwardFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void backwardFail_tagReusedWithDifferentType() {
        // Reused tag 2 with bool instead of string; wire type is different.
        assertThat(
                        checker.check(BASE_TAG_REUSED, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    // --- FORWARD -----------------------------------------------------------

    @Test
    void forwardPass_dropField() {
        // New (writer) drops a field; prior (reader) just won't receive it.
        assertThat(
                        checker.check(BASE_DROP_FIELD, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void forwardPass_addField() {
        // New (writer) adds a field; prior (reader) ignores unknown fields in proto3.
        assertThat(
                        checker.check(BASE_ADD_FIELD, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void forwardFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void forwardFail_tagReusedWithDifferentType() {
        assertThat(
                        checker.check(BASE_TAG_REUSED, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isFalse();
    }

    // --- FULL --------------------------------------------------------------

    @Test
    void fullPass_identical() {
        assertThat(checker.check(BASE, priorList(BASE), CompatLevel.FULL).isCompatible()).isTrue();
    }

    @Test
    void fullFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.FULL)
                                .isCompatible())
                .isFalse();
    }

    // --- Transitive --------------------------------------------------------

    @Test
    void backwardTransitive_failsAgainstAnyPrior() {
        List<String> priors = Arrays.asList(BASE_TYPE_CHANGED, BASE);
        assertThat(checker.check(BASE, priors, CompatLevel.BACKWARD_TRANSITIVE).isCompatible())
                .isFalse();
        // Non-transitive only checks latest (matching) — passes.
        assertThat(checker.check(BASE, priors, CompatLevel.BACKWARD).isCompatible()).isTrue();
    }

    @Test
    void fullTransitivePass_selfPriors() {
        List<String> priors = Arrays.asList(BASE, BASE);
        assertThat(checker.check(BASE, priors, CompatLevel.FULL_TRANSITIVE).isCompatible())
                .isTrue();
    }

    // --- proto3 optional / repeated ---------------------------------------

    @Test
    void backwardPass_addOptional() {
        String prior = "syntax = \"proto3\";\n" + "message M {\n" + "  int32 id = 1;\n" + "}\n";
        String proposed =
                "syntax = \"proto3\";\n"
                        + "message M {\n"
                        + "  int32 id = 1;\n"
                        + "  optional string nick = 2;\n"
                        + "}\n";
        assertThat(checker.check(proposed, priorList(prior), CompatLevel.BACKWARD).isCompatible())
                .isTrue();
    }

    @Test
    void backwardFail_singleToRepeated() {
        String prior = "syntax = \"proto3\";\n" + "message M {\n" + "  string tag = 1;\n" + "}\n";
        String proposed =
                "syntax = \"proto3\";\n" + "message M {\n" + "  repeated string tag = 1;\n" + "}\n";
        // Tag 1 changed wire type from single-string to repeated-string packing.
        assertThat(checker.check(proposed, priorList(prior), CompatLevel.BACKWARD).isCompatible())
                .isFalse();
    }

    // --- Malformed input ---------------------------------------------------

    @Test
    void malformedProposedRejected() {
        assertThat(
                        checker.check("not a proto", priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void malformedPriorReported() {
        List<String> priors = Collections.singletonList("not a proto");
        assertThat(checker.check(BASE, priors, CompatLevel.BACKWARD).isCompatible()).isFalse();
    }

    private static List<String> priorList(String s) {
        return Collections.singletonList(s);
    }
}
