/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This test tries to provoke a bug in ThreadPoolExecutor. It ended up proofing that the believed bug is not present.
 */
public class ThreadPoolExecutorTest extends ESTestCase {

    public void testMonkey() throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(between(1, 100), 100, 1000, TimeUnit.SECONDS, new LinkedTransferQueue<>());
        AtomicBoolean stopped = new AtomicBoolean();
        Thread resizer = new Thread(() -> {
            while (stopped.get() == false) {
                executor.setCorePoolSize(between(1, 100));
            }
        });

        resizer.start();


        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 100000; ++i) {
            final int fi = i;
            executor.execute(() -> {
                assertFalse(Thread.currentThread().isInterrupted());
                count.incrementAndGet();
                if (fi % 4 == 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        fail();
                    }
                }
            });
        }

        assertBusy(() -> assertThat(count.get(), Matchers.equalTo(100000)));
        stopped.set(true);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        resizer.join();
    }

    private static class TestQueue extends LinkedTransferQueue<Runnable> {
//        private final CyclicBarrier barrier = new CyclicBarrier(3);
        private final CountDownLatch latch;
        private final AtomicBoolean go = new AtomicBoolean();
        private TestQueue(int count) {
            this.latch = new CountDownLatch(count);
        }
        @Override
        public Runnable take() throws InterruptedException {
            Runnable taken = super.take();
            // careful not to mess with interrupts
            Thread thread = Thread.currentThread();
            latch.countDown();
            while (go.get() == false) {
//                Thread.yield();
//                System.out.println(thread.isInterrupted());
            }

//            System.out.println("x " + thread.isInterrupted());
//            System.out.println("x " + thread.isInterrupted());
            return taken;
        }
    }
    public void testDeterministic() throws Exception {
        // this turned out to behave well, `runWorker` calls Thread.interrupted in an if-statement, sigh.
        TestQueue queue = new TestQueue(2);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 100, 1000, TimeUnit.SECONDS, queue);
        executor.prestartCoreThread();
        executor.prestartCoreThread();
        CountDownLatch latch = new CountDownLatch(2);
        Runnable job = () -> {
            System.out.println("   " + Thread.currentThread().isInterrupted());
            assertFalse(Thread.currentThread().isInterrupted());
            latch.countDown();
        };

        executor.execute(job);
        executor.execute(job);

        // ensure 2 threads have a task in their hand.
        queue.latch.await();

        executor.setCorePoolSize(1);
        Thread.sleep(10);
        // execute tasks.
        queue.go.set(true);

        latch.await();


        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
