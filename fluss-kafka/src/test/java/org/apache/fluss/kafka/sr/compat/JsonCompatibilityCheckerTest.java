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

/** Unit tests for {@link JsonCompatibilityChecker}. */
class JsonCompatibilityCheckerTest {

    private final JsonCompatibilityChecker checker = new JsonCompatibilityChecker();

    // --- base / canned schemas --------------------------------------------

    private static final String BASE =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"integer\"},"
                    + "\"name\":{\"type\":\"string\"}"
                    + "},\"required\":[\"id\",\"name\"]}";

    private static final String BASE_WITH_OPTIONAL =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"integer\"},"
                    + "\"name\":{\"type\":\"string\"},"
                    + "\"nick\":{\"type\":\"string\"}"
                    + "},\"required\":[\"id\",\"name\"]}";

    private static final String BASE_WITH_REQUIRED =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"integer\"},"
                    + "\"name\":{\"type\":\"string\"},"
                    + "\"nick\":{\"type\":\"string\"}"
                    + "},\"required\":[\"id\",\"name\",\"nick\"]}";

    private static final String BASE_NAME_DROPPED =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"integer\"}"
                    + "},\"required\":[\"id\"]}";

    private static final String BASE_TYPE_CHANGED =
            "{\"type\":\"object\",\"properties\":{"
                    + "\"id\":{\"type\":\"string\"},"
                    + "\"name\":{\"type\":\"string\"}"
                    + "},\"required\":[\"id\",\"name\"]}";

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

    // --- BACKWARD -----------------------------------------------------------

    @Test
    void backwardPass_addOptional() {
        assertThat(
                        checker.check(BASE_WITH_OPTIONAL, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void backwardPass_removeRequiredButKeepProperty() {
        // Prior required `name`, new doesn't. Reader (new) accepts data with or without the
        // field, so data previously written with the field is still readable.
        String proposed =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"id\":{\"type\":\"integer\"},"
                        + "\"name\":{\"type\":\"string\"}"
                        + "},\"required\":[\"id\"]}";
        assertThat(checker.check(proposed, priorList(BASE), CompatLevel.BACKWARD).isCompatible())
                .isTrue();
    }

    @Test
    void backwardFail_addRequired() {
        assertThat(
                        checker.check(BASE_WITH_REQUIRED, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void backwardFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void backwardFail_dropRequiredProperty() {
        // Prior emitted `name`; new doesn't have it at all, and new's schema still requires
        // fields that prior's data didn't carry? Here the reader (new) simply never expects
        // the property, so that's actually BACKWARD-ok. Use a different scenario: reader
        // requires a field that writer did not emit.
        String priorSchema =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"id\":{\"type\":\"integer\"}"
                        + "},\"required\":[\"id\"]}";
        String proposed =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"id\":{\"type\":\"integer\"},"
                        + "\"nick\":{\"type\":\"string\"}"
                        + "},\"required\":[\"id\",\"nick\"]}";
        assertThat(
                        checker.check(proposed, priorList(priorSchema), CompatLevel.BACKWARD)
                                .isCompatible())
                .isFalse();
    }

    // --- FORWARD -----------------------------------------------------------

    @Test
    void forwardPass_removeOptional() {
        // prior had optional nick, new drops it → prior reader accepts new writer data (new
        // writer just doesn't emit nick; prior reader never required it).
        String proposed = BASE;
        assertThat(
                        checker.check(proposed, priorList(BASE_WITH_OPTIONAL), CompatLevel.FORWARD)
                                .isCompatible())
                .isTrue();
    }

    @Test
    void forwardFail_addRequired() {
        // New adds required nick; prior reader never expected it and will break on strict
        // JSON schema validation. Forward-incompatible.
        assertThat(
                        checker.check(BASE_WITH_REQUIRED, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void forwardFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.FORWARD)
                                .isCompatible())
                .isFalse();
    }

    // --- FULL = BACKWARD ∧ FORWARD ----------------------------------------

    @Test
    void fullPass_identical() {
        assertThat(checker.check(BASE, priorList(BASE), CompatLevel.FULL).isCompatible()).isTrue();
    }

    @Test
    void fullFail_addRequired() {
        assertThat(
                        checker.check(BASE_WITH_REQUIRED, priorList(BASE), CompatLevel.FULL)
                                .isCompatible())
                .isFalse();
    }

    @Test
    void fullFail_typeChanged() {
        assertThat(
                        checker.check(BASE_TYPE_CHANGED, priorList(BASE), CompatLevel.FULL)
                                .isCompatible())
                .isFalse();
    }

    // --- transitive variants -----------------------------------------------

    @Test
    void backwardTransitive_failsAgainstAnyPrior() {
        // Two priors; latest matches, but an earlier one has a clashing type.
        List<String> priors = Arrays.asList(BASE_TYPE_CHANGED, BASE);
        assertThat(checker.check(BASE, priors, CompatLevel.BACKWARD_TRANSITIVE).isCompatible())
                .isFalse();
        // Non-transitive BACKWARD only checks the latest — which is the matching one.
        assertThat(checker.check(BASE, priors, CompatLevel.BACKWARD).isCompatible()).isTrue();
    }

    @Test
    void forwardTransitive_failsAgainstAnyPrior() {
        List<String> priors = Arrays.asList(BASE_WITH_REQUIRED, BASE);
        // New = BASE. Prior 0 required `nick` which new doesn't emit → forward break against
        // that prior.
        assertThat(checker.check(BASE, priors, CompatLevel.FORWARD_TRANSITIVE).isCompatible())
                .isFalse();
    }

    @Test
    void fullTransitivePass_selfAsPrior() {
        List<String> priors = Arrays.asList(BASE, BASE);
        assertThat(checker.check(BASE, priors, CompatLevel.FULL_TRANSITIVE).isCompatible())
                .isTrue();
    }

    // --- nullability rules -------------------------------------------------

    @Test
    void backwardFail_readerNotNullableWriterIs() {
        String prior =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"type\":[\"null\",\"string\"]}"
                        + "}}";
        String proposed =
                "{\"type\":\"object\",\"properties\":{" + "\"x\":{\"type\":\"string\"}" + "}}";
        // Reader strips nullability but writer emits null → reader can't accept writer output.
        assertThat(checker.check(proposed, priorList(prior), CompatLevel.BACKWARD).isCompatible())
                .isFalse();
    }

    @Test
    void backwardPass_broadenNullable() {
        String prior =
                "{\"type\":\"object\",\"properties\":{" + "\"x\":{\"type\":\"string\"}" + "}}";
        String proposed =
                "{\"type\":\"object\",\"properties\":{"
                        + "\"x\":{\"type\":[\"null\",\"string\"]}"
                        + "}}";
        // Reader (proposed) accepts nulls + strings; writer (prior) emits only strings. OK.
        assertThat(checker.check(proposed, priorList(prior), CompatLevel.BACKWARD).isCompatible())
                .isTrue();
    }

    // --- malformed input ---------------------------------------------------

    @Test
    void proposedMalformedRejected() {
        assertThat(checker.check("{not json", priorList(BASE), CompatLevel.BACKWARD).isCompatible())
                .isFalse();
    }

    private static List<String> priorList(String s) {
        return Collections.singletonList(s);
    }
}
