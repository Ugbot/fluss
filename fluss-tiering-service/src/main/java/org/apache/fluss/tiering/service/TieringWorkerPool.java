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

package org.apache.fluss.tiering.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A bounded executor that tiers one table at a time per worker.
 *
 * <p>The pool is sized by {@code tiering.max-concurrent-tables} (default 1, preserving the
 * enumerator's single-table-at-a-time property). The daemon polls {@link #freeWorkers()} to decide
 * whether it may request a new table from the coordinator this round, then hands the assigned table
 * to {@link #submit(Runnable)} which runs it on a worker thread. A worker is considered busy from
 * the moment {@code submit} accepts the unit until that unit completes (normally or exceptionally).
 *
 * <p>This is the engine-agnostic replacement for the way the Flink {@code TieringSourceEnumerator}
 * serialized table requests; here concurrency is bounded by the executor and the free-worker
 * accounting rather than by the Flink source-split protocol.
 */
@ThreadSafe
public class TieringWorkerPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TieringWorkerPool.class);

    private final int maxConcurrentTables;
    private final ExecutorService executor;

    /** The number of workers currently running (or queued to run) a tiering unit. */
    private final AtomicInteger busyWorkers = new AtomicInteger(0);

    public TieringWorkerPool(int maxConcurrentTables) {
        checkArgument(
                maxConcurrentTables > 0,
                "tiering.max-concurrent-tables must be positive, but was %s.",
                maxConcurrentTables);
        this.maxConcurrentTables = maxConcurrentTables;
        this.executor =
                Executors.newFixedThreadPool(maxConcurrentTables, new TieringWorkerThreadFactory());
    }

    /** Returns the number of workers free to accept a new table this round. */
    public int freeWorkers() {
        return Math.max(0, maxConcurrentTables - busyWorkers.get());
    }

    /** Returns {@code true} if at least one worker is free to tier a table. */
    public boolean hasFreeWorker() {
        return freeWorkers() > 0;
    }

    /** Returns the configured worker parallelism (the maximum number of concurrent tables). */
    public int maxConcurrentTables() {
        return maxConcurrentTables;
    }

    /**
     * Submits a single table-tiering unit to run on a worker thread. The unit owns its full
     * lifecycle (generate splits, tier, commit, report finished/failed); this pool only bounds how
     * many run at once and tracks worker occupancy.
     *
     * <p>The {@code busyWorkers} counter is incremented before the task is enqueued and decremented
     * when the task finishes, so {@link #freeWorkers()} is accurate even while the task is queued.
     *
     * @param tieringUnit the work to run; must not throw checked exceptions out of {@code run()} —
     *     it is responsible for handling its own failures (e.g. reporting them to the coordinator)
     */
    public void submit(Runnable tieringUnit) {
        checkNotNull(tieringUnit, "tieringUnit must not be null.");
        busyWorkers.incrementAndGet();
        try {
            executor.execute(
                    () -> {
                        try {
                            tieringUnit.run();
                        } catch (Throwable t) {
                            // a tiering unit must handle its own failures; this is a last-resort
                            // guard so a buggy unit can never leak a worker as permanently busy.
                            LOG.error("Uncaught error in tiering worker; releasing worker.", t);
                        } finally {
                            busyWorkers.decrementAndGet();
                        }
                    });
        } catch (RuntimeException e) {
            // execution was rejected (e.g. during shutdown); release the reserved worker slot.
            busyWorkers.decrementAndGet();
            throw e;
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn(
                        "Tiering worker pool did not terminate within 60s; forcing shutdown of "
                                + "in-flight workers.");
                executor.shutdownNow();
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOG.warn("Tiering worker pool did not terminate after forced shutdown.");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class TieringWorkerThreadFactory implements ThreadFactory {

        private final AtomicInteger threadId = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "fluss-tiering-worker-" + threadId.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
