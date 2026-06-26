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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the bounded virtual-thread executor used by {@link KvSnapshotResource}. */
class KvSnapshotResourceTest {

    @Test
    void testInvalidConcurrencyRejected() {
        assertThatThrownBy(() -> KvSnapshotResource.newBoundedVirtualThreadExecutor(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-concurrency must be positive");
        assertThatThrownBy(() -> KvSnapshotResource.newBoundedVirtualThreadExecutor(-3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testConcurrencyIsBoundedBySemaphore() throws Exception {
        int maxConcurrency = 3;
        int totalTasks = 32;
        ExecutorService executor =
                KvSnapshotResource.newBoundedVirtualThreadExecutor(maxConcurrency);
        try {
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger observedPeak = new AtomicInteger();
            AtomicInteger completed = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(totalTasks);

            for (int i = 0; i < totalTasks; i++) {
                executor.execute(
                        () -> {
                            int current = inFlight.incrementAndGet();
                            // record the maximum number of simultaneously running tasks
                            observedPeak.accumulateAndGet(current, Math::max);
                            try {
                                // hold the permit long enough that, without the semaphore, more
                                // than maxConcurrency tasks would overlap here
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                inFlight.decrementAndGet();
                                completed.incrementAndGet();
                                done.countDown();
                            }
                        });
            }

            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(completed.get()).isEqualTo(totalTasks);
            // The core guarantee: peak in-flight tasks never exceeds the configured bound.
            assertThat(observedPeak.get()).isLessThanOrEqualTo(maxConcurrency);
            // Sanity check that the executor actually ran tasks concurrently (otherwise the bound
            // assertion would be trivially satisfied at peak 1).
            assertThat(observedPeak.get()).isGreaterThan(1);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testPermitReleasedWhenTaskThrows() throws Exception {
        ExecutorService executor = KvSnapshotResource.newBoundedVirtualThreadExecutor(1);
        try {
            // a task that throws must still release its permit, otherwise subsequent tasks deadlock
            for (int i = 0; i < 4; i++) {
                executor.execute(
                        () -> {
                            throw new RuntimeException("boom");
                        });
            }

            CountDownLatch ran = new CountDownLatch(1);
            executor.execute(ran::countDown);

            assertThat(ran.await(30, TimeUnit.SECONDS))
                    .as("a later task must run, proving permits were released by failed tasks")
                    .isTrue();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }
}
