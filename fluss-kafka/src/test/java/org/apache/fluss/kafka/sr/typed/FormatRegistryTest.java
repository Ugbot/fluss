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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.kafka.sr.compat.AvroCompatibilityChecker;
import org.apache.fluss.kafka.sr.compat.CompatLevel;
import org.apache.fluss.kafka.sr.compat.CompatibilityChecker;
import org.apache.fluss.kafka.sr.compat.CompatibilityResult;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FormatRegistry}. */
class FormatRegistryTest {

    @Test
    void serviceLoaderDiscoversBundledAvro() {
        FormatRegistry registry = FormatRegistry.instance();
        assertThat(registry.translator("AVRO"))
                .isNotNull()
                .isInstanceOf(AvroFormatTranslator.class);
        assertThat(registry.checker("AVRO"))
                .isNotNull()
                .isInstanceOf(AvroCompatibilityChecker.class);
    }

    @Test
    void lookupIsCaseInsensitive() {
        FormatRegistry registry = FormatRegistry.instance();
        assertThat(registry.translator("avro")).isSameAs(registry.translator("AVRO"));
        assertThat(registry.translator(" Avro ")).isSameAs(registry.translator("AVRO"));
        assertThat(registry.checker("avro")).isSameAs(registry.checker("AVRO"));
    }

    @Test
    void unknownFormatReturnsNull() {
        FormatRegistry registry = FormatRegistry.instance();
        assertThat(registry.translator("DOES_NOT_EXIST")).isNull();
        assertThat(registry.checker("DOES_NOT_EXIST")).isNull();
        assertThat(registry.translator(null)).isNull();
        assertThat(registry.checker(null)).isNull();
    }

    @Test
    void registerRejectsNull() {
        FormatRegistry fresh = newEmptyRegistry();
        assertThatThrownBy(() -> fresh.register((FormatTranslator) null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fresh.register((CompatibilityChecker) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void programmaticRegistrationOverrides() {
        FormatRegistry fresh = newEmptyRegistry();
        FormatTranslator stubTranslator = new StubTranslator("AVRO");
        CompatibilityChecker stubChecker = new StubChecker("AVRO");
        fresh.register(stubTranslator);
        fresh.register(stubChecker);
        assertThat(fresh.translator("AVRO")).isSameAs(stubTranslator);
        assertThat(fresh.checker("AVRO")).isSameAs(stubChecker);
    }

    @Test
    void formatIdsListReflectsRegistrations() {
        FormatRegistry fresh = newEmptyRegistry();
        fresh.register(new StubTranslator("AVRO"));
        fresh.register(new StubTranslator("JSON"));
        fresh.register(new StubChecker("PROTOBUF"));
        assertThat(fresh.formatIds()).containsExactlyInAnyOrder("AVRO", "JSON", "PROTOBUF");
    }

    private static FormatRegistry newEmptyRegistry() {
        // Use the singleton's API to produce a disposable instance without touching the global one.
        // We rely on reflection / package access to construct; the simplest path is to use the
        // global and clear it, then restore at end. For brevity in unit tests, construct via the
        // public API: bootstrap then clear.
        FormatRegistry instance = FormatRegistry.instance();
        // We don't want to clobber the shared global, so produce a standalone registry through a
        // package-private helper. Since FormatRegistry has a private constructor, test reflection
        // avoids needing any extra API surface.
        try {
            java.lang.reflect.Constructor<FormatRegistry> ctor =
                    FormatRegistry.class.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not build an isolated FormatRegistry", e);
        }
    }

    private static class StubTranslator implements FormatTranslator {
        private final String id;

        StubTranslator(String id) {
            this.id = id;
        }

        @Override
        public RowType translateTo(String schemaText) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String translateFrom(RowType rowType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatId() {
            return id;
        }
    }

    private static class StubChecker implements CompatibilityChecker {
        private final String id;

        StubChecker(String id) {
            this.id = id;
        }

        @Override
        public CompatibilityResult check(
                String proposedText,
                List<String> priorTexts,
                CompatLevel level,
                org.apache.fluss.kafka.sr.references.ReferenceResolver resolver) {
            return CompatibilityResult.compatible();
        }

        @Override
        public String formatId() {
            return id;
        }
    }
}
