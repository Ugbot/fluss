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

package com.alibaba.fluss.utils.concurrent;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory}. . */
public class ExecutorThreadFactoryTest {

    @Test
    void testThreadWithWithCustomExceptionHandler() {
        AtomicBoolean hasHandledUncaughtException = new AtomicBoolean(false);
        ExecutorThreadFactory executorThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("test-executor-thread-factory-pool-custom")
                        .setThreadPriority(1)
                        .setExceptionHandler(new TestExceptionHandler(hasHandledUncaughtException))
                        .build();
        Thread thread =
                executorThreadFactory.newThread(
                        () -> {
                            throw new RuntimeException("throw exception");
                        });
        thread.start();
        // TestExceptionHandler should set hasHandledUncaughtException to true
        while (!hasHandledUncaughtException.get()) {
            System.out.println("wait");
        }
    }

    @Test
    void testThreadWithWithCustomClassloader() {
        ClassLoader customClassloader = new URLClassLoader(new URL[0], null);
        ExecutorThreadFactory executorThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("test-executor-thread-factory-pool-custom")
                        .setThreadPriority(1)
                        .setThreadContextClassloader(customClassloader)
                        .build();
        Thread thread =
                executorThreadFactory.newThread(
                        () -> {
                            // do nothing
                        });
        assertThat(thread.getContextClassLoader()).isEqualTo(customClassloader);
    }

    private static class TestExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final AtomicBoolean hasHandledUncaughtException;

        public TestExceptionHandler(AtomicBoolean hasHandledUncaughtException) {
            this.hasHandledUncaughtException = hasHandledUncaughtException;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            hasHandledUncaughtException.set(true);
        }
    }
}
