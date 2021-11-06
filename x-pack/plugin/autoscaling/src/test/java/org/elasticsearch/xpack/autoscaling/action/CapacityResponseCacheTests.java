/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayDeque;
import java.util.Queue;

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

    public void testConcurrentCache() {
        Queue<Long> responses = new ArrayDeque<>();
        CapacityResponseCache<Long> cache = new CapacityResponseCache<>(threadPool, responses::remove);


    }

    public void test
}
