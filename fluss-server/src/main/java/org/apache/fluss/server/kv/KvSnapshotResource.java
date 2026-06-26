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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

        // Thread pool for the async part of kv snapshot. The work queue is intentionally UNBOUNDED:
        // the producer (PeriodicSnapshotManager.triggerSnapshot) submits while holding the KV
        // tablet write lock (kvLock), so the submit path must never block and must never run work
        // inline on the caller. A bounded queue with a blocking or CallerRunsPolicy handler would
        // run a heavy remote snapshot upload inline under kvLock, stalling all writes to the
        // tablet.
        // Memory growth here is bounded in practice by the snapshot scheduling interval; if a
        // backlog is a concern, observe pool/queue size via metrics rather than bounding this
        // queue.
        ExecutorService asyncOperationsThreadPool =
                new ThreadPoolExecutor(
                        0,
                        3,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory("fluss-kv-snapshot-async-operations"));
        return new KvSnapshotResource(
                kvSnapshotScheduler,
                kvSnapshotDataUploader,
                kvSnapshotDataDownloader,
                asyncOperationsThreadPool);
    }

    public void close() {
        // shutdown asyncOperationsThreadPool now
        asyncOperationsThreadPool.shutdownNow();
        // close kvSnapshotScheduler, also stop any actively executing task immediately
        // otherwise, a snapshot will still be take although it's closed, which will cause exception
        kvSnapshotScheduler.shutdownNow();
    }
}
