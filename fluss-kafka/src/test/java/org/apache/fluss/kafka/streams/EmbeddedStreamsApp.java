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

package org.apache.fluss.kafka.streams;

import org.apache.fluss.utils.FileUtils;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thin in-JVM wrapper around {@link KafkaStreams} used by the {@code fluss-kafka} test suite (see
 * {@code dev-docs/design/0017-kafka-connect-and-streams-test-harness.md}).
 *
 * <p>Enforces a per-test {@code state.dir} so parallel Surefire forks cannot collide on the RocksDB
 * lock; forces {@code auto.offset.reset=earliest} and a 500 ms commit interval so tests don't
 * silently hang or wait 30 s for offsets to land. The uncaught-exception handler is wired to a
 * {@link AtomicReference} so tests can assert the app did not die mid-poll.
 */
public final class EmbeddedStreamsApp implements AutoCloseable {

    private final KafkaStreams streams;
    private final Path stateDir;
    private final List<KafkaStreams.State> stateHistory = new ArrayList<>();
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    /**
     * Builds an app that will run the supplied topology against the supplied props. Callers must
     * set {@code application.id} and {@code bootstrap.servers}; the constructor forces a per-test
     * state dir under {@code target/streams-<uuid>/} and fails fast if the caller supplied a
     * conflicting {@code state.dir}.
     */
    public EmbeddedStreamsApp(Topology topology, Properties props) {
        if (topology == null) {
            throw new IllegalArgumentException("topology must not be null");
        }
        if (props == null) {
            throw new IllegalArgumentException("props must not be null");
        }
        if (!props.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            throw new IllegalArgumentException(
                    "Caller must set " + StreamsConfig.APPLICATION_ID_CONFIG);
        }
        if (!props.containsKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(
                    "Caller must set " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        }

        this.stateDir = Path.of("target", "streams-" + UUID.randomUUID());
        try {
            java.nio.file.Files.createDirectories(stateDir);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to create streams state dir " + stateDir, e);
        }

        Properties effective = new Properties();
        effective.putAll(props);
        // Force-overridable defaults. The spec (design 0017 §4.2) allows us to trample these.
        effective.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
        effective.put(
                StreamsConfig.consumerPrefix(
                        org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest");
        effective.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        effective.putIfAbsent(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndFailExceptionHandler.class.getName());
        // Single stream thread keeps the smoke ITs deterministic.
        effective.putIfAbsent(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        this.streams = new KafkaStreams(topology, effective);
        this.streams.setStateListener(
                (newState, oldState) -> {
                    synchronized (stateHistory) {
                        stateHistory.add(newState);
                    }
                });
        this.streams.setUncaughtExceptionHandler(
                (Thread.UncaughtExceptionHandler)
                        (thread, throwable) -> uncaught.compareAndSet(null, throwable));
    }

    /** Starts the underlying {@link KafkaStreams} instance. */
    public void start() {
        streams.start();
    }

    /**
     * Polls the app state every 100 ms until it reaches {@link KafkaStreams.State#RUNNING} or the
     * deadline expires; on timeout throws an {@link AssertionError} including the full state
     * transition history — critical for debugging flaky starts.
     */
    public void waitForRunning(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            KafkaStreams.State s = streams.state();
            if (s == KafkaStreams.State.RUNNING) {
                return;
            }
            if (s == KafkaStreams.State.ERROR
                    || s == KafkaStreams.State.PENDING_ERROR
                    || s == KafkaStreams.State.NOT_RUNNING
                    || s == KafkaStreams.State.PENDING_SHUTDOWN) {
                throw new AssertionError(
                        "Streams app entered terminal state "
                                + s
                                + " before RUNNING; history="
                                + stateHistorySnapshot()
                                + ", uncaught="
                                + uncaught.get());
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "Streams app did not reach RUNNING within "
                                + timeout.toMillis()
                                + "ms; current="
                                + s
                                + ", history="
                                + stateHistorySnapshot()
                                + ", uncaught="
                                + uncaught.get());
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for RUNNING", ie);
            }
        }
    }

    /** Current {@link KafkaStreams} state. */
    public KafkaStreams.State state() {
        return streams.state();
    }

    /** Exception captured by the uncaught-exception handler, if any. */
    public Throwable uncaughtException() {
        return uncaught.get();
    }

    /** Snapshot of the state transition history (for diagnostic asserts). */
    public List<KafkaStreams.State> stateHistorySnapshot() {
        synchronized (stateHistory) {
            return new ArrayList<>(stateHistory);
        }
    }

    /** Per-test {@code state.dir}. Deleted on {@link #close()}. */
    public Path stateDir() {
        return stateDir;
    }

    /** Raw handle to the underlying {@link KafkaStreams} for advanced assertions. */
    public KafkaStreams streams() {
        return streams;
    }

    @Override
    public void close() {
        try {
            streams.close(Duration.ofSeconds(10));
        } finally {
            FileUtils.deleteDirectoryQuietly(stateDir.toFile());
        }
    }
}
