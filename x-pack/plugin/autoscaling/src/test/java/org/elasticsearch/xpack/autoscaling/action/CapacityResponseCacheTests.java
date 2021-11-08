/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class CapacityResponseCacheTests extends AutoscalingTestCase {
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
        threadPool = null;
    }

    public void testSingleResponse() throws ExecutionException, InterruptedException {
        Queue<Long> responses = new ArrayDeque<>();
        CapacityResponseCache<Long> cache = new CapacityResponseCache<>(threadPool, cancelled -> responses.remove());
        long response = randomLong();
        responses.add(response);
        PlainActionFuture<Long> future = new PlainActionFuture<>();

        cache.get(() -> false, future);

        assertThat(future.get(), equalTo(response));
    }

    public void testSingleCancel() {
        CyclicBarrier barrier = new CyclicBarrier(2);
        CapacityResponseCache<Long> cache = new CapacityResponseCache<>(threadPool, cancelled -> {
            await(barrier);
            await(barrier);
            cancelled.run();
            return 0L;
        });

        AtomicBoolean cancelled = new AtomicBoolean();
        PlainActionFuture<Long> future = new PlainActionFuture<>();
        cache.get(cancelled::get, future);

        await(barrier);
        cancelled.set(true);
        await(barrier);

        expectThrows(TaskCancelledException.class, future::actionGet);
    }

    public void testMultipleRequests() {
        Queue<Supplier<Integer>> responses = new ArrayDeque<>();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(threadPool, cancellation -> responses.remove().get());

        CyclicBarrier barrier = new CyclicBarrier(2);
        responses.add(() -> {
            await(barrier);
            await(barrier);
            return 0;
        });

        PlainActionFuture<Integer> blockingFuture = new PlainActionFuture<>();
        cache.get(() -> false, blockingFuture);
        await(barrier);

        int count = between(1, 10);
        List<PlainActionFuture<Integer>> supersededFutures = new ArrayList<>(count + 1);
        for (int i = 0; i < count; ++i) {
            responses.add(() -> {
                fail();
                return 0;
            });
            PlainActionFuture<Integer> supersededFuture = new PlainActionFuture<>();
            supersededFutures.add(supersededFuture);
            cache.get(() -> false, supersededFuture);
        }

        int response = randomInt();
        responses.add(() -> response);
        PlainActionFuture<Integer> responseFuture = new PlainActionFuture<>();
        cache.get(() -> false, responseFuture);

        assertThat(blockingFuture.isDone(), is(false));
        for (PlainActionFuture<Integer> future : supersededFutures) {
            assertThat(future.isDone(), is(false));
        }
        assertThat(responseFuture.isDone(), is(false));

        // release the first blocking thread.
        await(barrier);

        assertThat(blockingFuture.actionGet(), equalTo(0));
        supersededFutures.forEach(future -> assertThat(future.actionGet(), equalTo(response)));
        assertThat(responseFuture.actionGet(), equalTo(response));
    }

    public void testMultipleRequestsCancelled() {
        Queue<Function<Runnable, Integer>> responses = new ArrayDeque<>();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(threadPool, cancellation -> responses.remove().apply(cancellation));

        CyclicBarrier barrier = new CyclicBarrier(2);
        responses.add(cancellation -> {
            await(barrier);
            await(barrier);

            return 0;
        });

        PlainActionFuture<Integer> blockingFuture = new PlainActionFuture<>();
        cache.get(() -> false, blockingFuture);
        await(barrier);

        AtomicBoolean cancelled = new AtomicBoolean();
        int count = between(1, 10);
        List<PlainActionFuture<Integer>> supersededFutures = new ArrayList<>(count + 1);
        for (int i = 0; i < count; ++i) {
            responses.add(cancellation -> {
                fail();
                return 0;
            });
            PlainActionFuture<Integer> supersededFuture = new PlainActionFuture<>();
            supersededFutures.add(supersededFuture);
            cache.get(cancelled::get, supersededFuture);
        }

        CyclicBarrier responseBarrier = new CyclicBarrier(2);
        responses.add(cancellation -> {
            await(responseBarrier);
            await(responseBarrier);
            cancellation.run();
            fail();
            return 0;
        });
        PlainActionFuture<Integer> responseFuture = new PlainActionFuture<>();
        cache.get(cancelled::get, responseFuture);

        assertThat(blockingFuture.isDone(), is(false));
        for (PlainActionFuture<Integer> future : supersededFutures) {
            assertThat(future.isDone(), is(false));
        }
        assertThat(responseFuture.isDone(), is(false));

        // release the first blocking thread.
        await(barrier);
        assertThat(blockingFuture.actionGet(), equalTo(0));

        await(responseBarrier);
        cancelled.set(true);
        await(responseBarrier);
        supersededFutures.forEach(future -> expectThrows(TaskCancelledException.class, future::actionGet));
        expectThrows(TaskCancelledException.class, responseFuture::actionGet);
    }

    public void testConcurrentRequests() throws Exception {
        Queue<Function<Runnable, Integer>> responses = new ArrayDeque<>();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(threadPool, cancellation -> responses.remove().apply(cancellation));
        int threadCount = between(2, 10);
        int iterations = between(2, 10);
        for (int i = 0; i < threadCount * iterations; ++i) {
            final int finali = i;
            responses.add(cancellation -> finali);
        }

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; ++t) {
            Thread thread = new Thread(() -> {
                await(barrier);
                int lastResult = -1;
                for (int i = 0; i < iterations; ++i) {
                    PlainActionFuture<Integer> future = new PlainActionFuture<>();
                    cache.get(() -> false, future);
                    int result = future.actionGet();
                    assertThat(result, greaterThan(lastResult));
                    lastResult = result;
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }
    private void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            fail();
        }
    }
}
