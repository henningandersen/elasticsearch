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

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * A rate limiter that allows prioritization by stacking the rate limiters. The inner most rate limiter has highest priority,
 * the outer level rate limiters generally wait for the inner rate limiters to have spare capacity before using it.
 *
 * It otherwise works similarly to SimpleRateLimiter.
 */
public class PrioritizedRateLimiter extends BasePrioritizedRateLimiter {

    private final static int MIN_PAUSE_CHECK_MSEC = 5;

    private final BasePrioritizedRateLimiter delegate;
    private final ReleasableLock lock = new ReleasableLock(new ReentrantLock(true));

    public PrioritizedRateLimiter(PrioritizedRateLimiter delegate) {
        this((BasePrioritizedRateLimiter) delegate);
    }

    /** mbPerSec is the MB/sec max IO rate */
    public PrioritizedRateLimiter(double mbPerSec, LongSupplier nowSupplier) {
        this(new PauseTracker(nowSupplier));
        setMBPerSec(mbPerSec);
    }

    private PrioritizedRateLimiter(BasePrioritizedRateLimiter delegate) {
        this.delegate = delegate;
    }

    /**
     * Sets an updated mb per second rate limit.
     */
    public void setMBPerSec(double mbPerSec) {
        delegate.setMBPerSec(mbPerSec);
    }

    public long getMinPauseCheckBytes() {
        return delegate.getMinPauseCheckBytes();
    }

    /**
     * The current mb per second rate limit.
     */
    public double getMBPerSec() {
        return delegate.getMBPerSec();
    }

    @Override
    protected long now() {
        return delegate.now();
    }

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target.  Be sure to only call
     *  this method when bytes &gt; {@link #getMinPauseCheckBytes},
     *  otherwise it will pause way too long!
     *
     *  @return the pause time in nano seconds
     */
    public long pause(long bytes) {
        long now = delegate.now();
        if (delegate.tryPause(bytes, now) != 0) {
            // acquire a lock for fairness, since we are rate limiting anyway
            try (Releasable dummy = lock.acquire()) {
                while (true) {
                    long current = System.nanoTime();
                    if (delegate.tryPause(bytes, current) == 0) {
                        assert current - now > 0;
                        return current - now;
                    }
                }
            }
        }

        return 0;
    }

    @Override
    long tryPause(long bytes, long now) {
        return delegate.tryPause(bytes, now);
    }

    public static class PauseTracker extends BasePrioritizedRateLimiter {
        private long lastNS;
        private volatile double mbPerSec;
        private volatile long minPauseCheckBytes;
        private LongSupplier nowSupplier;

        public PauseTracker(LongSupplier nowSupplier) {
            this.nowSupplier = nowSupplier;
            this.lastNS = nowSupplier.getAsLong();
        }

        public long tryPause(long bytes, long now) {
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

                    // Set to startNS, not targetNS, to enforce the instant rate, not
                    // the "averaaged over all history" rate:
                    lastNS = startNS;
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

        @Override
        protected void setMBPerSec(double mbPerSec) {
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

        @Override
        public double getMBPerSec() {
            return mbPerSec;
        }
    }

}
