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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.sr.compat.CompatibilityChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Central registry mapping a case-insensitive format id (e.g. {@code "AVRO"}, {@code "JSON"},
 * {@code "PROTOBUF"}) to a {@link FormatTranslator} and a {@link CompatibilityChecker}.
 *
 * <p>Discovery happens via {@link ServiceLoader} on first use: every {@code META-INF/services/
 * org.apache.fluss.kafka.sr.typed.FormatTranslator} line and every {@code META-INF/services/
 * org.apache.fluss.kafka.sr.compat.CompatibilityChecker} line contributes a provider.
 * Implementations bundled with the bolt-on are listed in the resources under {@code
 * fluss-kafka/src/main/resources/META-INF/services/}; tests may register additional providers
 * programmatically via {@link #register(FormatTranslator)} / {@link
 * #register(CompatibilityChecker)}.
 *
 * <p>Plan §27.3.
 */
@Internal
public final class FormatRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(FormatRegistry.class);

    private static volatile FormatRegistry instance;

    private final ConcurrentMap<String, FormatTranslator> translators = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompatibilityChecker> checkers = new ConcurrentHashMap<>();
    private final CompiledCodecCache codecCache = new CompiledCodecCache();

    private FormatRegistry() {}

    /**
     * The shared codec cache used by the typed Produce/Fetch hot path (design 0014). One instance
     * per registry; the registry is a process-singleton so this is effectively the cluster-wide
     * cache for compiled {@link RecordCodec}s.
     */
    public CompiledCodecCache codecCache() {
        return codecCache;
    }

    /** Global registry, seeded from {@link ServiceLoader} on first access. */
    public static FormatRegistry instance() {
        FormatRegistry local = instance;
        if (local != null) {
            return local;
        }
        synchronized (FormatRegistry.class) {
            if (instance == null) {
                FormatRegistry fresh = new FormatRegistry();
                fresh.loadFromServiceLoader();
                instance = fresh;
            }
            return instance;
        }
    }

    /** Replace the global registry (testing only). */
    @Internal
    public static synchronized void replaceInstance(@Nullable FormatRegistry replacement) {
        instance = replacement;
    }

    /** Register a translator. Idempotent: duplicate ids overwrite with a warn log. */
    public FormatRegistry register(FormatTranslator translator) {
        if (translator == null || translator.formatId() == null) {
            throw new IllegalArgumentException("translator and its formatId must be non-null");
        }
        String key = canonical(translator.formatId());
        FormatTranslator prior = translators.put(key, translator);
        if (prior != null && prior.getClass() != translator.getClass()) {
            LOG.warn(
                    "FormatTranslator for '{}' replaced: {} -> {}",
                    key,
                    prior.getClass().getName(),
                    translator.getClass().getName());
        }
        return this;
    }

    /** Register a compatibility checker. Idempotent: duplicate ids overwrite with a warn log. */
    public FormatRegistry register(CompatibilityChecker checker) {
        if (checker == null || checker.formatId() == null) {
            throw new IllegalArgumentException("checker and its formatId must be non-null");
        }
        String key = canonical(checker.formatId());
        CompatibilityChecker prior = checkers.put(key, checker);
        if (prior != null && prior.getClass() != checker.getClass()) {
            LOG.warn(
                    "CompatibilityChecker for '{}' replaced: {} -> {}",
                    key,
                    prior.getClass().getName(),
                    checker.getClass().getName());
        }
        return this;
    }

    /** Look up a translator by format id (case-insensitive). */
    @Nullable
    public FormatTranslator translator(String formatId) {
        if (formatId == null) {
            return null;
        }
        return translators.get(canonical(formatId));
    }

    /** Look up a compatibility checker by format id (case-insensitive). */
    @Nullable
    public CompatibilityChecker checker(String formatId) {
        if (formatId == null) {
            return null;
        }
        return checkers.get(canonical(formatId));
    }

    /** List registered format ids in a stable order (insertion-ordered, canonical case). */
    public List<String> formatIds() {
        // Both maps have the same key set by construction; copy from translators.
        Map<String, Boolean> merged = new LinkedHashMap<>();
        for (String k : translators.keySet()) {
            merged.put(k, true);
        }
        for (String k : checkers.keySet()) {
            merged.put(k, true);
        }
        return Collections.unmodifiableList(new java.util.ArrayList<>(merged.keySet()));
    }

    /** Clear all registrations (testing only). */
    @Internal
    public void clear() {
        translators.clear();
        checkers.clear();
    }

    private void loadFromServiceLoader() {
        int translatorCount = 0;
        for (FormatTranslator t : ServiceLoader.load(FormatTranslator.class)) {
            register(t);
            translatorCount++;
        }
        int checkerCount = 0;
        for (CompatibilityChecker c : ServiceLoader.load(CompatibilityChecker.class)) {
            register(c);
            checkerCount++;
        }
        LOG.info(
                "FormatRegistry loaded {} translator(s) + {} checker(s) from ServiceLoader: {}",
                translatorCount,
                checkerCount,
                formatIds());
    }

    private static String canonical(String formatId) {
        return formatId.trim().toUpperCase(Locale.ROOT);
    }
}
