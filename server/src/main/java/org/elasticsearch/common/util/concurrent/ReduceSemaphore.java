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

import java.util.concurrent.Semaphore;

class ReduceSemaphore extends Semaphore {
    ReduceSemaphore(int permits) {
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
// this causes deadlocks too - and seems unnecessary. Whether we wait for the run permit or the poll permit should not matter.
                //                    if (runnable != null) {
//                        logger.info("reacquire {}", Thread.currentThread().getName());
//                        reacquire();
//                    }
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
