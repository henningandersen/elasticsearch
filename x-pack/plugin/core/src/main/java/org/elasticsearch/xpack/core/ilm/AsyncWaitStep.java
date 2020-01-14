/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * A step which will be called periodically, waiting for some condition to become true.
 * Called asynchronously, as the condition may take time to check.
 *
 * If checking something based on the current cluster state which does not take time to check, use {@link ClusterStateWaitStep}.
 */
public abstract class AsyncWaitStep extends Step {

    private IndexLifecycleContext indexLifecycleContext;

    public AsyncWaitStep(StepKey key, StepKey nextStepKey, IndexLifecycleContext indexLifecycleContext) {
        super(key, nextStepKey);
        this.indexLifecycleContext = indexLifecycleContext;
    }

    protected IndexLifecycleContext getIndexLifecycleContext() {
        return indexLifecycleContext;
    }

    public abstract void evaluateCondition(IndexMetaData indexMetaData, Listener listener);

    public interface Listener {

        void onResponse(boolean conditionMet, ToXContentObject infomationContext);

        void onFailure(Exception e);
    }
}
