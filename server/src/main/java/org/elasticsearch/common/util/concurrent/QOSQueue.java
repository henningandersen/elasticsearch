/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.CheckedFunction;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QOSQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {
    private final BlockingQueue<Runnable> delegate;

    private final AtomicInteger paused = new AtomicInteger();

    public int numPaused() {
        return paused.get();
    }

    private static class ReduceSemaphore extends Semaphore {
        private ReduceSemaphore(int permits) {
            super(permits);
        }

        @Override
        public void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }

        public void reacquire() throws InterruptedException {
            if (availablePermits() < 0) {
                release();
                acquire();
            }
        }

        public Runnable handlePoll(CheckedFunction<Semaphore, Boolean, InterruptedException> acquirer,
                                   CheckedSupplier<Runnable, InterruptedException> poller) throws InterruptedException {
            Runnable runnable = null;
            if (acquirer.apply(this)) {
                try {
                    runnable = poller.get();
                    if (runnable != null) {
                        reacquire();
                    }
                    return runnable;
                } finally {
                    if (runnable == null) {
                        // exception or poll returned null
                        release();
                    }
                }
            } else {
                return null;
            }
        }
    }
    private final Semaphore runPermit;
    private final ReduceSemaphore pollPermit;


    public QOSQueue(int size, BlockingQueue<Runnable> delegate) {
        this.runPermit = new Semaphore(size);
        this.pollPermit = new ReduceSemaphore(size);
        this.delegate = delegate;
    }

    @Override
    public Iterator<Runnable> iterator() {
        return delegate.iterator();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean offer(Runnable e) {
        return delegate.offer(new WrappedRunnable(e));
    }

    @Override
    public void put(Runnable run) throws InterruptedException {
        delegate.put(new WrappedRunnable(run));
    }

    @Override
    public boolean offer(Runnable run, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.offer(new WrappedRunnable(run), timeout, unit);
    }

    @Override
    public Runnable take() throws InterruptedException {
        //noinspection ConstantConditions
        return pollPermit.handlePoll(s -> {
            s.acquire();
            return true;
        }, delegate::take);
    }

    @Override
    public Runnable poll() {
        try {
            return pollPermit.handlePoll(Semaphore::tryAcquire, delegate::poll);
        } catch (InterruptedException e) {
            assert false : "no interrupt exception expected";
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
        return pollPermit.handlePoll(s -> s.tryAcquire(timeout, unit), () -> delegate.poll(timeout, unit));
    }

    @Override
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    @Override
    public int drainTo(Collection<? super Runnable> c) {
        return delegate.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        return delegate.drainTo(c, maxElements);
    }

    @Override
    public Runnable peek() {
        throw new UnsupportedOperationException("not used by thread pools");
    }

    <T> T pauseAndGet(Future<T> until, QOSThreadPoolExecutor executor) throws ExecutionException, InterruptedException {
        pollPermit.release();
        runPermit.release();
        paused.incrementAndGet();
        try {
            executor.adjustPoolSize(paused::get);
            return until.get();
        } finally {
            pollPermit.reducePermits(1); // hard take it.
            paused.decrementAndGet();
            executor.adjustPoolSize(paused::get);
            runPermit.acquire(); // but do not run until a slot is available.
        }
    }

    private class WrappedRunnable implements Runnable {
        private final Runnable delegate;

        private WrappedRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            try {
                runPermit.acquire();
            } catch (InterruptedException e) {
                pollPermit.release();
                // todo: better handling.
                throw new RuntimeException(e);
            }
            try {
                delegate.run();
            } finally {
                runPermit.release();
                pollPermit.release();
            }
        }
    }

}
