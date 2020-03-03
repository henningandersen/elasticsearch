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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

/**
 * Performs an action which must be performed asynchronously because it may take time to complete.
 */
public abstract class AsyncActionStep extends Step {

    public AsyncActionStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    public static TimeValue getMasterTimeout(ClusterState clusterState){
        Objects.requireNonNull(clusterState, "cannot determine master timeout when cluster state is null");
        return LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING.get(clusterState.metaData().settings());
    }

    public boolean indexSurvives() {
        return true;
    }

    public abstract void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState,
                                       ClusterStateObserver observer, Listener listener, Client client);

    public interface DryRunContext {
        ClusterState reroute(ClusterState state, String reason);

        IndexScopedSettings getIndexScopedSettings();

        IndicesService getIndicesService();

        ThreadPool getThreadPool();
    }
    // todo: should we have ActionListener<ClusterState> instead?
    public ClusterState performDryRun(IndexMetaData indexMetaData, ClusterState currentClusterState, DryRunContext context)
    {
        assert false;
        return currentClusterState;
    }

    public interface Listener {

        void onResponse(boolean complete);

        void onFailure(Exception e);
    }

}
