/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class QOSThreadPoolExecutorTests extends ESTestCase {
    public void testPause() throws Exception {
        final int size = between(1, 10);
        final int maxSize = between(size, 20);

        ThreadContext context = new ThreadContext(Settings.EMPTY);
        QOSThreadPoolExecutor executor = EsExecutors.newFixedQOS("test", size, maxSize, 1000, EsExecutors.daemonThreadFactory("qostest"),
            context);
        try {
            final int rounds = between(1, 5);
            for (int r = 0; r < rounds; ++r) {
                final int result = randomInt();
                CompletableFuture<Integer> paused = new CompletableFuture<>();
                int expectedPaused = maxSize - size;
                for (int i = 0; i < expectedPaused; ++i) {
                    executor.submit(() -> {
                        try {
                            assertThat(QOSThreadPoolExecutor.pauseAndGet(paused), Matchers.equalTo(result));
                        } catch (ExecutionException | InterruptedException e) {
                            fail("no exception expected");
                        }
                    });
                }

                assertBusy(() -> assertThat(executor.numPaused(), Matchers.equalTo(expectedPaused)));
                assertThat(executor.numRunning(), Matchers.equalTo(expectedPaused));

                int busyThreads = size;
                CyclicBarrier busyThreadsBarrier = new CyclicBarrier(busyThreads + 1);
                for (int i = 0; i < busyThreads; ++i) {
                    executor.submit(() -> {
                        try {
                            busyThreadsBarrier.await();
                            busyThreadsBarrier.await();
                        } catch (BrokenBarrierException | InterruptedException e) {
                            fail("no exception expected");
                        }
                    });
                }

                // verify that the right number of threads are "busy".
                busyThreadsBarrier.await();

                CyclicBarrier queuedBarrier = new CyclicBarrier(2);
                executor.submit(() -> {
                    try {
                        queuedBarrier.await();
                    } catch (BrokenBarrierException | InterruptedException e) {
                        fail("no exception expected");
                    }
                });

                assertThat(executor.getQueue().size(), Matchers.equalTo(1));
                assertThat(executor.numRunning(), Matchers.equalTo(maxSize));
                assertThat(queuedBarrier.getNumberWaiting(), Matchers.equalTo(0));

                // release extra threads
                busyThreadsBarrier.await();
                // ensure the queue task executes.
                queuedBarrier.await();

                paused.complete(result);

                assertBusy(() -> assertThat(executor.numRunning(), Matchers.equalTo(0)));
                assertThat(executor.numPaused(), Matchers.equalTo(0));
                logger.info("Completed round {}", r);
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
