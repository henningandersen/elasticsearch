/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * A response cache for capacity API that ensures that concurrent requests may be served by a single calculation and that only one thread
 * is active at any time calculating the capacity API response. Work is delegated to the MANAGEMENT thread pool.
 * This protects the master from overload due to capacity API requests, which are expensive in nature.
 *
 * The generic arg mainly helps ease testing (and we may want to generalize this in the future)
 */
class CapacityResponseCache<Response> extends CancellableSingleObjectCache<Long, Long, Response> {
    private final Queue<Job> jobQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger jobQueueSize = new AtomicInteger();
    private final AtomicLong logicalTime = new AtomicLong();
    private final ThreadPool threadPool;
    private final Function<Runnable, Response> refresher;
    public CapacityResponseCache(ThreadPool threadPool, Function<Runnable, Response> refresher) {
        this.threadPool = threadPool;
        this.refresher = refresher;
    }

    public void get(BooleanSupplier isCancelled, ActionListener<Response> listener) {
        super.get(logicalTime.incrementAndGet(), isCancelled, listener);
    }

    @Override
    protected void refresh(Long input, Runnable ensureNotCancelled, BooleanSupplier supersedeIfStale,
                           ActionListener<Response> listener) {

        jobQueue.add(new Job(ensureNotCancelled, supersedeIfStale, listener));
        assert jobQueueSize.get() >= 0;
        if (jobQueueSize.getAndIncrement() == 0) {
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(this::singleThreadRefresh);
        }
    }

    @Override
    protected Long getKey(Long input) {
        return input;
    }

    @Override
    protected boolean isFresh(Long currentKey, Long newKey) {
        return newKey <= currentKey;
    }

    private void singleThreadRefresh() {
        assert jobQueueSize.get() > 0 : "poor man's single thread check";
        do {
            Job job = jobQueue.poll();
            assert job != null : jobQueueSize.get() + " queue size is out of sync";
//             increment logical time before reading any state that the calculation depends on.
//            logicalTime.incrementAndGet();
            job.execute();
        } while (jobQueueSize.decrementAndGet() > 0);

        // clear the cache
        clearIfNotFresh(logicalTime.get());
    }

    // for tests
    int jobQueueSize() {
        return jobQueueSize.get();
    }

    private class Job {
        private final Runnable ensureNotCancelled;
        private final BooleanSupplier supersedeIfStale;
        private final ActionListener<Response> listener;

        private Job(Runnable ensureNotCancelled, BooleanSupplier supersedeIfStale, ActionListener<Response> listener) {
            this.ensureNotCancelled = ensureNotCancelled;
            this.supersedeIfStale = supersedeIfStale;
            this.listener = listener;
        }

        public void execute() {
            if (supersedeIfStale.getAsBoolean() == false) {
                // disregard input, in case we sat in the queue, it is better to use newest state.
                ActionListener.completeWith(listener, () -> refresher.apply(ensureNotCancelled));
            }
        }
    }
}
