/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleContext;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.ilm.history.ILMHistoryItem;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.util.function.Supplier;

/**
 * This class makes the ILM decisions on progressing to the next step, but agnostic of current cluster.
 */
public class IndexLifecycleSimulator {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleSimulator.class);

    private final PolicyStepsRegistry stepsRegistry;
    private final IndexLifecycleRunner runner;
    private long now;

    private final ClusterStateServiceSimulator clusterStateServiceSimulator;

    public static class ClusterStateServiceSimulator {
        private ClusterState clusterState;

        public ClusterStateServiceSimulator(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        // todo: if there is any async, this should be done one at a time through a queue.
        public synchronized void updateClusterState(String name, ClusterStateUpdateTask task) {
            ClusterState oldState = this.clusterState;
            try {
                this.clusterState = task.execute(oldState);
            } catch (Exception e) {
                logger.warn(e);
            }
            task.clusterStateProcessed(name, oldState, clusterState);
        }

        public synchronized ClusterState getClusterState() {
            return clusterState;
        }
    }

    public IndexLifecycleSimulator(ClusterStateServiceSimulator clusterStateServiceSimulator,
                                   Supplier<IndexLifecycleContext> contextFactory, long time, NamedXContentRegistry xContentRegistry) {
        this.now = time;
        this.clusterStateServiceSimulator = clusterStateServiceSimulator;
        this.stepsRegistry = new PolicyStepsRegistry(xContentRegistry, md -> contextFactory.get());
        this.runner = new IndexLifecycleRunner(stepsRegistry, new ILMHistoryStore() {
            @Override
            public void putAsync(ILMHistoryItem item) {
                // disabled
            }

            @Override
            public void close() {

            }
        },
            () -> now,
            clusterStateServiceSimulator::updateClusterState,
            () -> null);

        this.stepsRegistry.update(clusterStateServiceSimulator.getClusterState());
    }


    public void forwardTime(long now) {
        assert now >= this.now;
    }

    public ClusterState getClusterState() {
        return clusterStateServiceSimulator.getClusterState();
    }

    public void decide(ActionListener<ClusterState> listener) {
        IndexLifecycleMetadata currentMetadata = getClusterState().metaData().custom(IndexLifecycleMetadata.TYPE);

        if (currentMetadata == null) {
            listener.onResponse(getClusterState());
            return;
        }

        for (ObjectCursor<IndexMetaData> cursor : getClusterState().metaData().indices().values()) {
            IndexMetaData idxMeta = cursor.value;
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                final LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
                Step.StepKey stepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);

                try {
                    // simply do both for now, may want to only trigger the state change one when state actually changed.
                    // this also has the sideeffect of making test setup slightly easier (definitely a todo).
                    runner.runPolicyAfterStateChange(policyName, idxMeta);
                    runner.runPeriodicStep(policyName, idxMeta);
                } catch (Exception e) {
                    if (logger.isTraceEnabled()) {
                        logger.warn(new ParameterizedMessage("async action execution failed during policy trigger" +
                            " for index [{}] with policy [{}] in step [{}], lifecycle state: [{}]",
                            idxMeta.getIndex().getName(), policyName, stepKey, lifecycleState.asMap()), e);
                    } else {
                        logger.warn(new ParameterizedMessage("async action execution failed during policy trigger" +
                            " for index [{}] with policy [{}] in step [{}]",
                            idxMeta.getIndex().getName(), policyName, stepKey), e);

                    }
                    // Don't rethrow the exception, we don't want a failure for one index to be
                    // called to cause actions not to be triggered for further indices
                }
            }
        }

        // so far we have nothing async so invoke listener here:
        listener.onResponse(getClusterState());
    }
}
