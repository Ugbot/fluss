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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Containing resources needed to do kv snapshot. It contains:
 *
 * <ul>
 *   <li>A scheduler to schedule snapshot for kv periodically
 *   <li>A thread pool for the async part of kv snapshot
 *   <li>A uploader to upload snapshot data in the async part of kv snapshot
 * </ul>
 */
public class KvSnapshotResource {

    /** A scheduler to schedule kv snapshot. */
    private final ScheduledExecutorService kvSnapshotScheduler;

    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;

    /** A uploader to upload snapshot data in the async phase of kv snapshot. */
    private final KvSnapshotDataUploader kvSnapshotDataUploader;

    /** A downloader to download snapshot data. */
    private final KvSnapshotDataDownloader kvSnapshotDataDownloader;

    private KvSnapshotResource(
            ScheduledExecutorService kvSnapshotScheduler,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            KvSnapshotDataDownloader kvSnapshotDataDownloader,
            ExecutorService asyncOperationsThreadPool) {
        this.kvSnapshotScheduler = kvSnapshotScheduler;
        this.kvSnapshotDataUploader = kvSnapshotDataUploader;
        this.kvSnapshotDataDownloader = kvSnapshotDataDownloader;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
    }

    public ScheduledExecutorService getKvSnapshotScheduler() {
        return kvSnapshotScheduler;
    }

    public ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    public KvSnapshotDataUploader getKvSnapshotDataUploader() {
        return kvSnapshotDataUploader;
    }

    public KvSnapshotDataDownloader getKvSnapshotDataDownloader() {
        return kvSnapshotDataDownloader;
    }

    public static KvSnapshotResource create(
            int serverId, Configuration conf, ExecutorService ioExecutor) {
        KvSnapshotDataUploader kvSnapshotDataUploader = new KvSnapshotDataUploader(ioExecutor);

        KvSnapshotDataDownloader kvSnapshotDataDownloader =
                new KvSnapshotDataDownloader(ioExecutor);

        ScheduledExecutorService kvSnapshotScheduler =
                Executors.newScheduledThreadPool(
                        conf.getInt(ConfigOptions.KV_SNAPSHOT_SCHEDULER_THREAD_NUM),
                        new ExecutorThreadFactory("periodic-snapshot-scheduler-" + serverId));

        // Thread pool for the async part of kv snapshot.
        //
        // Backing executor: a virtual-thread-per-task executor. The blocking snapshot phase
        // (remote upload of checkpoint files) parks on I/O; virtual threads unmount from their
        // carrier thread while parked, so blocking I/O no longer pins a platform thread.
        //
        // Concurrency bound: a Semaphore caps how many async (upload) snapshot operations may be
        // in-flight at once. The previous platform-thread pool used a maxPoolSize of 3 to bound the
        // number of concurrent in-flight async uploads (remote I/O, memory, connections); switching
        // to unbounded virtual threads would have removed that cap (which is why the earlier
        // unbounded version was reverted). Each submitted task acquires a permit before doing any
        // work and releases it in a finally block, so peak in-flight uploads stays bounded
        // regardless of how cheap virtual threads are to spawn. (The local RocksDB checkpoint
        // directories are created earlier, synchronously under kvLock in initSnapshot, and are
        // bounded per-bucket by the snapshot scheduler — not by this Semaphore.)
        //
        // The submit path must never block and must never run work inline on the caller: the
        // producer (PeriodicSnapshotManager.triggerSnapshot) submits while holding the KV tablet
        // write lock (kvLock). Submission here only spawns a virtual thread (cheap, non-blocking);
        // the Semaphore is acquired inside that virtual thread, never on the submitting thread, so
        // back-pressure never stalls writes to the tablet.
        int maxConcurrency = conf.getInt(ConfigOptions.KV_SNAPSHOT_ASYNC_OPERATION_MAX_CONCURRENCY);
        ExecutorService asyncOperationsThreadPool = newBoundedVirtualThreadExecutor(maxConcurrency);
        return new KvSnapshotResource(
                kvSnapshotScheduler,
                kvSnapshotDataUploader,
                kvSnapshotDataDownloader,
                asyncOperationsThreadPool);
    }

    /**
     * Creates the executor used for the async (blocking-upload) phase of kv snapshots.
     *
     * <p>The executor spawns one virtual thread per submitted task so that the carrier thread is
     * released while the task is blocked on remote snapshot I/O. Concurrency is bounded by a {@link
     * Semaphore} of {@code maxConcurrency} permits: each task acquires a permit before running and
     * releases it when done, so at most {@code maxConcurrency} async upload operations are in flight
     * at any time. (Local RocksDB checkpoint directories are created earlier under kvLock in
     * initSnapshot and bounded per-bucket by the scheduler, not by this Semaphore.)
     *
     * @param maxConcurrency the maximum number of concurrently running snapshot operations; must be
     *     positive
     */
    static ExecutorService newBoundedVirtualThreadExecutor(int maxConcurrency) {
        checkArgument(
                maxConcurrency > 0,
                "kv snapshot async-operation max-concurrency must be positive, but was %s",
                maxConcurrency);
        return new SemaphoreBoundedExecutorService(maxConcurrency);
    }

    public void close() {
        // shutdown asyncOperationsThreadPool now
        asyncOperationsThreadPool.shutdownNow();
        // close kvSnapshotScheduler, also stop any actively executing task immediately
        // otherwise, a snapshot will still be take although it's closed, which will cause exception
        kvSnapshotScheduler.shutdownNow();
    }

    /**
     * An {@link ExecutorService} that runs every submitted task on its own virtual thread while
     * bounding the number of concurrently running tasks with a {@link Semaphore}.
     *
     * <p>All lifecycle management (shutdown, awaitTermination, etc.) is delegated to an inner
     * virtual-thread-per-task executor obtained from {@link Executors#newThreadPerTaskExecutor}.
     * Only {@link #execute(Runnable)} is intercepted: each task is wrapped so that it acquires a
     * permit before running its body and releases it in a {@code finally} block. The permit is
     * acquired on the spawned virtual thread, never on the submitting thread, so submission stays
     * non-blocking (important because snapshots are submitted while holding the KV tablet write
     * lock).
     */
    private static final class SemaphoreBoundedExecutorService extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final Semaphore permits;

        private SemaphoreBoundedExecutorService(int maxConcurrency) {
            this.permits = new Semaphore(maxConcurrency);
            this.delegate = Executors.newThreadPerTaskExecutor(newVirtualThreadFactory());
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(
                    () -> {
                        try {
                            permits.acquire();
                        } catch (InterruptedException e) {
                            // The executor is shutting down (shutdownNow interrupts workers) or the
                            // task was cancelled before it could start. Restore the interrupt flag
                            // and drop the task without running its (potentially heavy) body.
                            Thread.currentThread().interrupt();
                            return;
                        }
                        try {
                            command.run();
                        } finally {
                            permits.release();
                        }
                    });
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }

    private static ThreadFactory newVirtualThreadFactory() {
        // name(prefix, start) yields auto-incrementing virtual-thread names for diagnostics.
        return Thread.ofVirtual().name("fluss-kv-snapshot-async-operations-", 0).factory();
    }
}
