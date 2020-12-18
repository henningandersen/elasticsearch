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

package org.elasticsearch.common.util.ratelimiter;

import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

/**
 * A rate limiter that allows prioritization of rate limiters by calling the pause method with a priority.
 *
 * It otherwise works similarly to SimpleRateLimiter.
 */
public class PrioritizedRateLimiter extends RateLimiter {

    private final static int MIN_PAUSE_CHECK_MSEC = 5;

    private long lastNS;
    private volatile double mbPerSec;
    private volatile long minPauseCheckBytes;
    private final LongSupplier nowSupplier;
    private final ReleasableLock lock;
    private final TicketQueue[] ticketQueues;

    /**
     * @param mbPerSec is the MB/sec max IO rate
     * @param nowSupplier supplies current nano time
     * @param priorities number of priorities, must be > 0
     */
    public PrioritizedRateLimiter(double mbPerSec, LongSupplier nowSupplier, int priorities) {
        assert priorities > 0;
        lock = new ReleasableLock(new ReentrantLock());
        ticketQueues = IntStream.range(0, priorities).mapToObj(i -> new TicketQueue()).toArray(TicketQueue[]::new);
        this.nowSupplier = nowSupplier;
        setMBPerSec(mbPerSec);
        this.lastNS = nowSupplier.getAsLong();
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
    public long getMinPauseCheckBytes() {
        return minPauseCheckBytes;
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

    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target.  Be sure to only call
     *  this method when bytes &gt; {@link #getMinPauseCheckBytes},
     *  otherwise it will pause way too long!
     *
     * @param bytes the bytes to account for
     * @param priority the priority of the caller, 0 is highest priority.
     * @return the pause time in nano seconds
     */
    public long pause(long bytes, int priority) {
        assert priority < ticketQueues.length;
        long start = now();
        long now = start;
        boolean assignedTicket = false;
        long ticket = -1;
        long deltaNS = calculateDeltaNS(bytes);
        TicketQueue ticketQueue = ticketQueues[priority];
        while (true) {
            long sleepTime;
            try (Releasable dummy = lock.acquire()) {
                if (assignedTicket == false) {
                    ticket = ticketQueue.ticket(deltaNS);
                    assignedTicket = true;
                }
                long lowerPriorityReservations = calculateReserved(0, priority);
                long reservedBeforeTicket = ticketQueue.reservedBeforeTicket(ticket);
                if (reservedBeforeTicket <= 0) {
                    return now - start;
                }
                long targetNS = lastNS + reservedBeforeTicket + lowerPriorityReservations;
                sleepTime = targetNS - now;
                if (sleepTime <= 0) {
                    // regardless of priority, it is ok to release now, even if higher priority paused threads are not yet dealt with,
                    // since we accounted for all higher priority pause threads.
                    ticketQueue.release(ticket, reservedBeforeTicket);
                    if (lowerPriorityReservations == 0 && calculateReserved(priority, ticketQueues.length) == 0) {
                        // Set to startNS, not targetNS, to enforce the instant rate, not
                        // the "averaged over all history" rate:
                        lastNS = now;
                    } else {
                        lastNS = targetNS;
                    }
                    return now - start;
                }
            }
            trySleepNS(sleepTime);
            now = now();
        }
    }

    private long calculateReserved(int start, int end) {
        long sum = 0;
        for (int i = start; i < end; ++i) {
            sum += ticketQueues[i].reserved();
        }
        return sum;
    }

    /**
     * Try to sleep for the indicated time though waking up earlier is possible.
     */
    void trySleepNS(long pauseNS) {
        assert pauseNS > 0;
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
                sleepMS = (int) (pauseNS /1000000);
                sleepNS = (int) (pauseNS % 1000000);
            }
            Thread.sleep(sleepMS, sleepNS);
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
        }
    }

    private long calculateDeltaNS(long bytes) {
        assert bytes > 0;
        double secondsToPause = (bytes / 1024. / 1024.) / mbPerSec;
        assert secondsToPause > 0d;

        long delta = (long) (1000000000 * secondsToPause);
        assert delta >= 0 : delta;
        return delta == 0 ? 1 : delta;
    }

    private long now() {
        return nowSupplier.getAsLong();
    }

    private class TicketQueue {

        private long reservedNS;
        private long releasedNS;

        public long ticket(long reservationNS) {
            assert lock.isHeldByCurrentThread();
            this.reservedNS += reservationNS;
            return reservedNS;
        }

        public void release(long ticket, long reservationNS) {
            assert lock.isHeldByCurrentThread();
            assert releasedNS < ticket : releasedNS + ">=" + ticket;
            this.releasedNS += reservationNS;
        }

        public long reserved() {
            assert lock.isHeldByCurrentThread();
            return reservedNS - releasedNS;
        }

        public long reservedBeforeTicket(long ticket) {
            assert lock.isHeldByCurrentThread();
            return ticket - releasedNS;
        }
    }

    void assertAllReleased() {
        try (Releasable dummy = lock.acquire()) {
            for (TicketQueue ticketQueue : ticketQueues) {
                assert ticketQueue.reserved() == 0;
            }
        }
    }
}
