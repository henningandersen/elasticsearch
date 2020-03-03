/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

abstract class AbstractUnfollowIndexStep extends AsyncActionStep {

    AbstractUnfollowIndexStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public final void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState,
                                    ClusterStateObserver observer, Listener listener, Client client) {
        String followerIndex = indexMetaData.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetaData.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        innerPerformAction(followerIndex, listener, client);
    }

    abstract void innerPerformAction(String followerIndex, Listener listener, Client client);
}
