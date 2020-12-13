/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

/**
 * A rate limiter that allows prioritization by stacking the rate limiters. The inner most rate limiter has highest priority,
 * the outer level rate limiters generally wait for the inner rate limiters to have spare capacity before using it.
 *
 * It otherwise works similarly to SimpleRateLimiter.
 */
public class PrioritizedRateLimiter3 extends RateLimiter {

    // todo: make a version where the locks are in an array and we just lock all from priority-no and down.
    private final static int MIN_PAUSE_CHECK_MSEC = 5;

    private long lastNS;
    private volatile double mbPerSec;
    private volatile long minPauseCheckBytes;
    private LongSupplier nowSupplier;

    private final ReleasableLock[] locks;
    private final Condition[] conditions;
    private AtomicInteger[] waiters;
    private AtomicLong[] reserved;
    private AtomicLong reservedTotal;

    /** mbPerSec is the MB/sec max IO rate */
    public PrioritizedRateLimiter3(double mbPerSec, LongSupplier nowSupplier, int priorities) {
        locks = IntStream.range(0, priorities).mapToObj(i -> new ReleasableLock(new ReentrantLock())).toArray(ReleasableLock[]::new);
        setMBPerSec(mbPerSec);
    }

    /**
     * Sets an updated mb per second rate limit.
     */
    @Override
    public void setMBPerSec(double mbPerSec) {
        this.mbPerSec = mbPerSec;
        minPauseCheckBytes = (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024);
    }

    @Override
    protected long getMinPauseCheckBytes() {
        return minPauseCheckBytes;
    }

    @Override
    protected long now() {
        return nowSupplier.getAsLong();
    }

    /**
     * The current mb per second rate limit.
     */
    @Override
    public double getMBPerSec() {
        return mbPerSec;
    }

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target.  Be sure to only call
     *  this method when bytes &gt; {@link #getMinPauseCheckBytes},
     *  otherwise it will pause way too long!
     *
     *  @return the pause time in nano seconds
     */
    public long pause(long bytes) {
        return pause(bytes, 0);
    }

    private long pause(long bytes, int priority) {
        assert priority < locks.length;
        int no = waiters[priority].incrementAndGet();

        long myReservation = reserved[priority].addAndGet(bytes);
        long totalReserved = reservedTotal.addAndGet(bytes);
        long now = now();
        if (no != 1 || waiters[0].get() != 0 || tryPause(bytes, now) != 0) {
            // acquire a lock for fairness, since we are rate limiting anyway
            try (Releasable dummy = locks[priority].acquire()) {
                while (true) {
                    boolean higherPriorityWaiters = true;
                    while (higherPriorityWaiters) {
                        higherPriorityWaiters = false;
                        for (int i = priority - 1; i >= 0; ++i) {
                            try (Releasable dummy2 = locks[i].acquire()) {
                                if (waiters[i].get() > 0) {
                                    higherPriorityWaiters = true;
                                    try {
                                        conditions[i].await();
                                    } catch (InterruptedException e) {
                                        throw new ThreadInterruptedException(e);
                                    }
                                }
                            }
                        }
                    }
                    long current = System.nanoTime();
                    if (tryPause(bytes, current) == 0) {
                        assert current - now > 0;
                        return current - now;
                    }
                }
            }
        }

        return 0;

    }

    public RateLimiter priority(int priority) {
        return new RateLimiter() {
            @Override
            public void setMBPerSec(double mbPerSec) {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getMBPerSec() {
                return PrioritizedRateLimiter3.this.getMBPerSec();
            }

            @Override
            public long pause(long bytes) {
                return PrioritizedRateLimiter3.this.pause(bytes, priority);
            }

            @Override
            public long getMinPauseCheckBytes() {
                return PrioritizedRateLimiter3.this.getMinPauseCheckBytes();
            }
        };
    }

    private long tryPause(long bytes, long now) {
        long startNS = now;

        double secondsToPause = (bytes /1024./1024.) / mbPerSec;

        long targetNS;
        long delta = (long) (1000000000 * secondsToPause);

        // Sync'd to read + write lastNS:
        synchronized (this) {

            // Time we should sleep until; this is purely instantaneous
            // rate (just adds seconds onto the last time we had paused to);
            // maybe we should also offer decayed recent history one?
            targetNS = lastNS + delta;
            if (startNS >= targetNS) {
                // OK, current time is already beyond the target sleep time,
                // no pausing to do.

                // use waiters instead?
                if (reservedTotal.get() > bytes) {
                    // Set to startNS, not targetNS, to enforce the instant rate, not
                    // the "averaaged over all history" rate:
                    lastNS = startNS;
                } else {
                    lastNS = targetNS;
                }
                return 0;
            }
        }

        long curNS = startNS;

        // While loop because Thread.sleep doesn't always sleep
        // enough:
        while (true) {
            final long pauseNS = targetNS - curNS;
            if (pauseNS > 0) {
                try {
                    // NOTE: except maybe on real-time JVMs, minimum realistic sleep time
                    // is 1 msec; if you pass just 1 nsec the default impl rounds
                    // this up to 1 msec:
                    int sleepNS;
                    int sleepMS;
                    if (pauseNS > 100000L * Integer.MAX_VALUE) {
                        // Not really practical (sleeping for 25 days) but we shouldn't overflow int:
                        sleepMS = Integer.MAX_VALUE;
                        sleepNS = 0;
                    } else {
                        sleepMS = (int) (pauseNS/1000000);
                        sleepNS = (int) (pauseNS % 1000000);
                    }
                    Thread.sleep(sleepMS, sleepNS);
                } catch (InterruptedException ie) {
                    throw new ThreadInterruptedException(ie);
                }
                curNS = System.nanoTime();
                continue;
            }
            break;
        }

        return curNS - startNS;
    }
}
