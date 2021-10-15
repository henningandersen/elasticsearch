/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingLicenseChecker;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.memory.AutoscalingMemoryInfoService;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

public class TransportGetAutoscalingCapacityAction extends TransportMasterNodeAction<
    GetAutoscalingCapacityAction.Request,
    GetAutoscalingCapacityAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingCapacityAction.class);

    private final AutoscalingCalculateCapacityService capacityService;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final AutoscalingMemoryInfoService memoryInfoService;
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;
    private final ResponseCache responseCache = new ResponseCache();

    @Inject
    public TransportGetAutoscalingCapacityAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AutoscalingCalculateCapacityService.Holder capacityServiceHolder,
        final ClusterInfoService clusterInfoService,
        final SnapshotsInfoService snapshotsInfoService,
        final AutoscalingMemoryInfoService memoryInfoService,
        final AllocationDeciders allocationDeciders,
        final AutoscalingLicenseChecker autoscalingLicenseChecker
    ) {
        super(
            GetAutoscalingCapacityAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoscalingCapacityAction.Request::new,
            indexNameExpressionResolver,
            GetAutoscalingCapacityAction.Response::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.snapshotsInfoService = snapshotsInfoService;
        this.memoryInfoService = memoryInfoService;
        this.capacityService = capacityServiceHolder.get(allocationDeciders);
        this.clusterInfoService = clusterInfoService;
        this.autoscalingLicenseChecker = Objects.requireNonNull(autoscalingLicenseChecker);
        assert this.capacityService != null;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetAutoscalingCapacityAction.Request request,
        final ClusterState state,
        final ActionListener<GetAutoscalingCapacityAction.Response> listener
    ) {
        if (autoscalingLicenseChecker.isAutoscalingAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("autoscaling"));
            return;
        }

        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;

        responseCache.get(state, cancellableTask::isCancelled, listener);
    }

    private GetAutoscalingCapacityAction.Response computeCapacity(ClusterState state, Runnable ensureNotCancelled) {
        GetAutoscalingCapacityAction.Response response = new GetAutoscalingCapacityAction.Response(
            capacityService.calculate(
                state,
                clusterInfoService.getClusterInfo(),
                snapshotsInfoService.snapshotShardSizes(),
                memoryInfoService.snapshot(),
                ensureNotCancelled
            )
        );
        logger.debug("autoscaling capacity response [{}]", response);
        return response;
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingCapacityAction.Request request, final ClusterState state) {
        return null;
    }

    private class ResponseCache extends CancellableSingleObjectCache<
        Tuple<Long, ClusterState>,
        Long,
        GetAutoscalingCapacityAction.Response> {
        private final AtomicLong round = new AtomicLong();

        public void get(ClusterState state, BooleanSupplier isCancelled, ActionListener<GetAutoscalingCapacityAction.Response> listener) {
            final long beforeRound = round.get();
            ActionListener<GetAutoscalingCapacityAction.Response> wrappedListener = ActionListener.runBefore(listener, () -> {
                // we ensure that any client serially calling capacity are guaranteed to get a fresh response.
                round.compareAndSet(beforeRound, beforeRound + 1);
                // and ensure that we clear out outdated results since the normal case is to not need the caching.
                clearIfNotFresh(Tuple.tuple(beforeRound + 1, state));
            });
            super.get(Tuple.tuple(beforeRound, state), isCancelled, wrappedListener);
        }

        @Override
        protected void refresh(
            Tuple<Long, ClusterState> input,
            Runnable ensureNotCancelled,
            ActionListener<GetAutoscalingCapacityAction.Response> listener
        ) {
            ActionListener.completeWith(listener, () -> computeCapacity(input.v2(), ensureNotCancelled));
        }

        @Override
        protected Long getKey(Tuple<Long, ClusterState> input) {
            return input.v1();
        }

        @Override
        protected boolean isFresh(Long currentKey, Long newKey) {
            return newKey <= currentKey;
        }
    }
    // private class ResponseCache {
    // private AtomicReference<ListenableActionFuture<GetAutoscalingCapacityAction.Response>> current = new AtomicReference<>();
    //
    // public void get(final ClusterState state, final ActionListener<GetAutoscalingCapacityAction.Response> listener) {
    // ListenableActionFuture<GetAutoscalingCapacityAction.Response> future = new ListenableActionFuture<>();
    // ListenableActionFuture<GetAutoscalingCapacityAction.Response> existing = current.compareAndExchange(null, future);
    // if (existing == null) {
    // future.addListener(listener);
    // threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new ActionRunnable<GetAutoscalingCapacityAction.Response>(future) {
    // @Override
    // protected void doRun() throws Exception {
    // computeCapacity(state);
    // future.onResponse(computeCapacity(state));
    // }
    //
    // @Override
    // public void onAfter() {
    // assert current.get() == future;
    // current.set(null);
    // }
    // });
    // } else {
    // existing.addListener(listener);
    // }
    // }
    // }
}
