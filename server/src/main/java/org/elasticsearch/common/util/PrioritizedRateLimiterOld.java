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

/**
 * A rate limiter that allows foreground and background tasks to run.
 *
 * Background tasks will not on their own impose foreground tasks to wait.
 *
 * It otherwise works similarly to SimpleRateLimiter.
 */
public class PrioritizedRateLimiterOld {

    private final static int MIN_PAUSE_CHECK_MSEC = 5;

    private volatile double mbPerSec;
    private volatile long minPauseCheckBytes;
    private long lastNS;
    private volatile long accumulatedDelta;

    /** mbPerSec is the MB/sec max IO rate */
    public PrioritizedRateLimiterOld(double mbPerSec) {
        setMBPerSec(mbPerSec);
        lastNS = System.nanoTime();
    }

    /**
     * Sets an updated mb per second rate limit.
     */
    public void setMBPerSec(double mbPerSec) {
        this.mbPerSec = mbPerSec;
        minPauseCheckBytes = (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024);
    }

    public long getMinPauseCheckBytes() {
        return minPauseCheckBytes;
    }

    /**
     * The current mb per second rate limit.
     */
    public double getMBPerSec() {
        return this.mbPerSec;
    }

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target.  Be sure to only call
     *  this method when bytes &gt; {@link #getMinPauseCheckBytes},
     *  otherwise it will pause way too long!
     *
     *  @return the pause time in nano seconds */
    public long pause(long bytes, boolean background) {

        long startNS = System.nanoTime();

        double secondsToPause = (bytes/1024./1024.) / mbPerSec;

        long targetNS;
        long startDelta;
        long delta = (long) (1000000000 * secondsToPause);

        // Sync'd to read + write lastNS:
        synchronized (this) {

            // Time we should sleep until; this is purely instantaneous
            // rate (just adds seconds onto the last time we had paused to);
            // maybe we should also offer decayed recent history one?
            targetNS = lastNS + delta;

            startDelta = accumulatedDelta += delta;

            if (startNS >= targetNS) {
                // OK, current time is already beyond the target sleep time,
                // no pausing to do.

                // Set to startNS, not targetNS, to enforce the instant rate, not
                // the "averaaged over all history" rate:
                lastNS = startNS;
                return 0;
            }

            if (background == false) {
                lastNS = targetNS;
            }
        }

        long curNS = startNS;

        // While loop because Thread.sleep doesn't always sleep
        // enough:
        while (true) {
            final long pauseNS = targetNS - curNS + (background ? accumulatedDelta - startDelta : 0);
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

        if (background) {
            synchronized (this) {
                lastNS = Math.max(lastNS, targetNS + accumulatedDelta - startDelta);
            }
        }
        return curNS - startNS;
    }
}
