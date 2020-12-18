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
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public class PrioritizedRateLimiterTests extends ESTestCase {

    public void testRealtimeThroughput() {
        int priorities = randomIntBetween(1, 5);
        long totalAmount = randomLongBetween(1, Long.MAX_VALUE/2);
        // approximately 1/10th of a second run time, allowing ~20 sleeps.
        double mbPerSec = totalAmount / 1024. / 1024. * randomIntBetween(5, 15);
        PrioritizedRateLimiter rateLimiter = new PrioritizedRateLimiter(mbPerSec, System::nanoTime, priorities);

        long before = System.nanoTime();
        long remaining = totalAmount;
        while (remaining > 0) {
            long bytes;
            if (remaining < rateLimiter.getMinPauseCheckBytes()) {
                bytes = remaining;
            } else if (randomBoolean()) {
                bytes = randomLongBetween(rateLimiter.getMinPauseCheckBytes(), remaining);
            } else {
                bytes = randomLongBetween(rateLimiter.getMinPauseCheckBytes(), Math.min(remaining,
                    rateLimiter.getMinPauseCheckBytes() * 2));
            }

            rateLimiter.pause(remaining, between(0, priorities - 1));

            remaining -= bytes;
        }

        long after = System.nanoTime();
        long time = after - before;
        logger.info("Completed in [{}] ns, mbPerSec actual [{}], mbPerSec expected [{}]", time,
            ((double) totalAmount) / time * 1e9 / 1024 / 1024, mbPerSec);

        assertThat(time, Matchers.lessThan(10L * 1000_000_000));
        rateLimiter.assertAllReleased();
    }

    public void testSimulatedThroughput() throws Exception {
        int priorities = randomIntBetween(1, 5);
        long totalAmount = randomLongBetween(1, Long.MAX_VALUE/2);
        // approximately 1/10th of a second run time, allowing ~20 sleeps.
        double mbPerSec = totalAmount / 1024. / 1024. * randomIntBetween(5, 15);
        AtomicLong clock = new AtomicLong(randomLong());
        PrioritizedRateLimiter rateLimiter = new PrioritizedRateLimiter(mbPerSec, clock::get, priorities) {
            @Override
            void trySleepNS(long pauseNS) {
                long delta = randomLongBetween(0, pauseNS * 2);
                clock.addAndGet(delta);
            }
        };

        Thread t = new Thread() {
            @Override
            public void run() {
                super.run();
            }
        };



        t.join(10000);
        assertThat(t.isAlive(), is(false));
    }


    public void testPriority() throws Exception {
        int priorities = randomIntBetween(2, 5);
        double mbPerSec = randomDoubleBetween(1d / 1024 / 1024, 1d, true);
        AtomicLong clock = new AtomicLong(randomLong());
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean stopped = new AtomicBoolean();
        PrioritizedRateLimiter rateLimiter = new PrioritizedRateLimiter(mbPerSec, clock::get, priorities) {
            @Override
            void trySleepNS(long pauseNS) {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    fail();
                }
                long delta = stopped.get() ? pauseNS : randomLongBetween(0, pauseNS - 1);
                clock.addAndGet(delta);
            }
        };

        Thread t = new Thread(() -> {
            rateLimiter.pause(between(1, 100), priorities - 1);
            assertTrue(stopped.get());
        });

        t.start();

        for (int i = 0; i < 100; ++i) {
            rateLimiter.pause(between(1, 1000), between(0, priorities - 2));
        }

        assertThat(t.isAlive(), is(true));
        stopped.set(true);
        barrier.await(10, TimeUnit.SECONDS);

        t.join(10000);
        assertThat(t.isAlive(), is(false));
    }

    public void testPriority2() throws Exception {
        int priorities = randomIntBetween(2, 5);
        double mbPerSec = randomDoubleBetween(1d / 1024 / 1024, 1d, true);
        AtomicLong clock = new AtomicLong(randomLong());
        int threadCount = between(2, 5);
        CyclicBarrier barrier = new CyclicBarrier(1 + threadCount);
        AtomicBoolean stopped = new AtomicBoolean();
        PrioritizedRateLimiter rateLimiter = new PrioritizedRateLimiter(mbPerSec, clock::get, priorities) {
            @Override
            void trySleepNS(long pauseNS) {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    fail();
                }
                long delta = stopped.get() ? pauseNS : randomLongBetween(0, pauseNS - 1);
                clock.addAndGet(delta);
            }
        };

        CyclicBarrier threadBarrier = new CyclicBarrier(threadCount);
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(no -> new Thread(() -> {
            for (int i = 0; i < 100; ++i) {
                rateLimiter.pause(between(1, 1000), between(0, priorities - 2));
            }
            try {
                threadBarrier.await(10, TimeUnit.SECONDS);
                stopped.set(true);
                barrier.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                fail(e.getMessage());
            }
        })).collect(Collectors.toList());

        threads.forEach(Thread::start);

        rateLimiter.pause(between(1, 100), priorities - 1);
        assertTrue(stopped.get());

        for (Thread thread : threads) {
            thread.join(10000);
        }
        threads.forEach(t -> assertThat(t.isAlive(), is(false)));
    }

    public void testMinPauseCheckBytes() {
        double mbPerSec = randomDoubleBetween(0, 1_000_000, false);
        PrioritizedRateLimiter rateLimiter = new PrioritizedRateLimiter(mbPerSec, System::nanoTime, between(1, 10));
        RateLimiter.SimpleRateLimiter simpleRateLimiter = new RateLimiter.SimpleRateLimiter(mbPerSec);
        assertThat(rateLimiter.getMinPauseCheckBytes(), Matchers.equalTo(simpleRateLimiter.getMinPauseCheckBytes()));
    }
}
