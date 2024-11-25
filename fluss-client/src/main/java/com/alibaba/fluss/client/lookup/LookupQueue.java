/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A queue that buffers the pending lookup operations and provides a list of {@link Lookup} when
 * call method {@link #drain()}.
 */
@ThreadSafe
@Internal
class LookupQueue {

    private volatile boolean closed;
    private final ArrayBlockingQueue<Lookup> lookupQueue;
    private final int maxBatchSize;

    LookupQueue(Configuration conf) {
        this.lookupQueue =
                new ArrayBlockingQueue<>(conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE));
        this.maxBatchSize = conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE);
        this.closed = false;
    }

    void appendLookup(Lookup lookup) {
        if (closed) {
            throw new IllegalStateException(
                    "Can not append lookup operation since the LookupQueue is closed.");
        }
        try {
            lookupQueue.put(lookup);
        } catch (InterruptedException e) {
            lookup.future().completeExceptionally(e);
        }
    }

    boolean hasUnDrained() {
        return !lookupQueue.isEmpty();
    }

    /** Drain a batch of {@link Lookup}s from the lookup queue. */
    List<Lookup> drain() throws Exception {
        List<Lookup> lookups = new ArrayList<>(maxBatchSize);
        Lookup firstLookup = lookupQueue.poll(300, TimeUnit.MILLISECONDS);
        if (firstLookup != null) {
            lookups.add(firstLookup);
            lookupQueue.drainTo(lookups, maxBatchSize - 1);
        }
        return lookups;
    }

    /** Drain all the {@link Lookup}s from the lookup queue. */
    List<Lookup> drainAll() {
        List<Lookup> lookups = new ArrayList<>(lookupQueue.size());
        lookupQueue.drainTo(lookups);
        return lookups;
    }

    public void close() {
        closed = true;
    }
}
