/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/** A thread pool executor allowing to volunteer to give up execution when waiting for IO or otherwise. QOS may not be its final name */
// todo: determine frozen vs not frozen shard and utilize for QOS.
public class QOSThreadPoolExecutor extends EsThreadPoolExecutor {
    private final AtomicInteger active = new AtomicInteger();
    private final int maxExtraThreads;
    private static final ThreadLocal<QOSThreadPoolExecutor> self = new ThreadLocal<>();
    private final Object lock = new Object();
    private final QOSQueue qosQueue;
    private final int size;
    private final ReduceSemaphore runPermit;

    public static QOSThreadPoolExecutor create(String name, int size, int maximumPoolSize, int keepAliveTime, TimeUnit unit,
                                               BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                                               RejectedExecutionHandler handler, ThreadContext contextHolder) {
        return new QOSThreadPoolExecutor(name, size, maximumPoolSize, keepAliveTime, unit, new QOSQueue(size, workQueue), threadFactory,
            handler, contextHolder);
    }

    private QOSThreadPoolExecutor(String name, int size, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                            QOSQueue workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler,
                                  ThreadContext contextHolder) {
        super(name, maximumPoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);
//        super(name, size, Integer.MAX_VALUE, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);
        this.runPermit = new ReduceSemaphore(size);
        this.maxExtraThreads = maximumPoolSize - size;
        assert this.maxExtraThreads >= 0;
        this.size = size;
        this.qosQueue = workQueue;
    }

    @Override
    public void execute(Runnable command) {
        super.execute(wrap(command));
    }

    private Runnable wrap(Runnable command) {
        return new WrappedRunnable(command);
    }

    public int numPaused() {
        return qosQueue.numPaused();
    }

    public int numRunning() {
        return active.get();
    }

    private class WrappedRunnable implements Runnable {
        private final Runnable delegate;

        private WrappedRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {

            active.incrementAndGet();
            self.set(QOSThreadPoolExecutor.this);
            try {
                try {
                    runPermit.acquire();
                } catch (InterruptedException e) {
                    // todo: better handling.
                    throw new RuntimeException(e);
                }
                try {
                    delegate.run();
                } finally {
                    runPermit.release();
                }
            } finally {
                self.set(null);
                active.decrementAndGet();
            }
        }
    }

    public static <T> T pauseAndGet(Future<T> until) throws ExecutionException, InterruptedException {
        QOSThreadPoolExecutor executor = self.get();
        // todo: in principle this assertion would be nice, but we can probably cover that it actually works with tests instead.
        // frozen does read from cache during recovery etc. on non-search threads.
        //assert executor != null : "can only be called from a thread that is in a QOS thread pool";
        if (until.isDone() || executor == null) {
            return until.get();
        }
        executor.runPermit.release();
        try {
            return executor.qosQueue.pauseAndGet(until, executor);
        } finally {
            // but do not run until a slot is available.
            if (executor.runPermit.tryAcquire(100, TimeUnit.MILLISECONDS) == false) {
                // hard take it, allowing this thread to continue despite competing with too many others.
                // this resolves deadlocks from things like LazySoftDeletesDirectoryReaderWrapper.get/init.
                executor.runPermit.reducePermits(1);
            }
        }
//        return until.get();
    }

    /**
     * Adjust pool size. Notice that this is not immediate for scale down.
     * @param extra a supplier of extra threads (i.e., paused threads) to allow
     */
    public void adjustPoolSize(IntSupplier extra) {
        // disabling this, since it looks like adjusting the queue size down sometimes skips a task. Unable to provoke this though, so
        // could be something else.
        //        synchronized (lock) {
//            setCorePoolSize(size + Math.min(extra.getAsInt(), maxExtraThreads));
//        }
    }
}
