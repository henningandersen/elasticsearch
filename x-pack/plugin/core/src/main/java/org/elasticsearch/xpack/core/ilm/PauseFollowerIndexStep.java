/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;

final class PauseFollowerIndexStep extends AbstractUnfollowIndexStep {

    static final String NAME = "pause-follower-index";

    PauseFollowerIndexStep(StepKey key, StepKey nextStepKey, IndexLifecycleContext indexLifecycleContext) {
        super(key, nextStepKey, indexLifecycleContext);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        getIndexLifecycleContext().pauseFollow(followerIndex, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "pause follow response is not acknowledged";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }
}
