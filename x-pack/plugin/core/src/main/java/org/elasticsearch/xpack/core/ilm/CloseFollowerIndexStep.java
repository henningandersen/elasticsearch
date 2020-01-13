/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

final class CloseFollowerIndexStep extends AsyncRetryDuringSnapshotActionStep {

    static final String NAME = "close-follower-index";

    CloseFollowerIndexStep(StepKey key, StepKey nextStepKey, IndexLifecycleContext indexLifecyleContext) {
        super(key, nextStepKey, indexLifecyleContext);
    }

    @Override
    void performDuringNoSnapshot(IndexMetaData indexMetaData, ClusterState currentClusterState, Listener listener) {
        String followerIndex = indexMetaData.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetaData.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        if (indexMetaData.getState() == IndexMetaData.State.OPEN) {
            getIndexLifecycleContext().close(followerIndex, ActionListener.wrap(
                r -> {
                    assert r.isAcknowledged() : "close index response is not acknowledged";
                    listener.onResponse(true);
                },
                listener::onFailure)
            );
        } else {
            listener.onResponse(true);
        }
    }
}
