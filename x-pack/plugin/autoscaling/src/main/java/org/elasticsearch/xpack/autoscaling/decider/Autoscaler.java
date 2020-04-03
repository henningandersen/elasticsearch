/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.autoscaling.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.xpack.core.ilm.RolloverAction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Autoscaler predicts whether a scale out is necessary by forecasting the current cluster state and checking if allocation deciders
 * will accept it against the cluster configuration.
 */
// todo: a service or decider using refactored shared rollover functionality
public class Autoscaler {
    private static final Logger logger = LogManager.getLogger(Autoscaler.class);
    private final MetadataRolloverService rolloverService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardsAllocator;

    public Autoscaler(MetadataRolloverService rolloverService, AllocationDeciders allocationDeciders, ShardsAllocator shardsAllocator) {
        this.rolloverService = rolloverService;
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocator = shardsAllocator;
    }

    /**
     * Return whether to scale up/out or not for hot layer.
     * @param state current state.
     * @param clusterInfo current cluster info
     * @return true if scale up/out is necessary.
     */
    public boolean scaleHot(ClusterState state, ClusterInfo clusterInfo, Predicate<IndexMetadata> hotPredicate) {
        state = simulateScaleHot(state, clusterInfo);

        return hasUnassigned(state, hotPredicate) || shardsCannotMoveToTier(state, clusterInfo, hotPredicate);
    }

    /**
     * Reactive scale, used for all tiers but hot.
     */
    public boolean reactiveScaleTier(ClusterState state, ClusterInfo clusterInfo, Predicate<IndexMetadata> tierPredicate) {
        state = simulateAllocationOfState(state, clusterInfo);
        return hasUnassigned(state, tierPredicate) || shardsCannotMoveToTier(state, clusterInfo, tierPredicate);
    }

    private boolean shardsCannotMoveToTier(ClusterState state, ClusterInfo clusterInfo, Predicate<IndexMetadata> tierPredicate) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state, clusterInfo, System.nanoTime());
        Metadata Metadata = state.metadata();
        return state.getRoutingNodes()
            .shards(s -> true)
            .stream()
            .filter(shard -> tierPredicate.test(Metadata.getIndexSafe(shard.index())))
            .filter(shard -> allocationDeciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation) == Decision.NO)
            .filter(shard -> canAllocate(shard, allocation) == false)
            .findAny()
            .isPresent();
    }

    // todo: determine that disk allocation decider is only decider.
    private boolean canAllocate(ShardRouting shard, RoutingAllocation allocation) {
        return StreamSupport.stream(allocation.routingNodes().spliterator(), false)
            .anyMatch(node -> allocationDeciders.canAllocate(shard, node, allocation) != Decision.NO);
    }

    // todo: verify only reason is disk
    private boolean hasUnassigned(ClusterState state, Predicate<IndexMetadata> predicate) {
        Metadata Metadata = state.metadata();
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .map(u -> Metadata.getIndexSafe(u.index()))
            .anyMatch(predicate);
    }

    // allow test to verify more internal resulting state.
    ClusterState simulateScaleHot(ClusterState state, ClusterInfo clusterInfo) {
        state = futureHotState(state);

        return simulateAllocationOfState(state, clusterInfo);
    }

    private ClusterState simulateAllocationOfState(ClusterState state, ClusterInfo clusterInfo) {
        state = allocate(state, clusterInfo, a -> {});
        state = allocate(state, clusterInfo, this::startShards);
        return state;
    }

    private ClusterState allocate(ClusterState state, ClusterInfo clusterInfo, Consumer<RoutingAllocation> allocationManipulator) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state, clusterInfo, System.nanoTime());

        allocationManipulator.accept(allocation);
        shardsAllocator.allocate(allocation);
        return updateClusterState(state, allocation);
    }

    private ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata);

        return newStateBuilder.build();
    }

    private void startShards(RoutingAllocation allocation) {
        // simulate that all shards are started so replicas can recover and replicas to get to green.
        // also starts initializing relocated shards, which removes the relocation source too.
        allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).forEach(s -> {
            logger.info("Starting [{}]", s);
            allocation.routingNodes().startShard(logger, s, allocation.changes());
        });
    }

    /**
     * Forecast a future cluster state for the hot layer by simulating a rollover of all ILM controlled indices.
     */
    private ClusterState futureHotState(ClusterState state) {
        Set<String> aliases = new HashSet<>();
        for (IndexMetadata imd : state.metadata()) {
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(imd.getSettings());
            if (rolloverAlias != null) {
                aliases.add(rolloverAlias);
            }
        }
        // todo: include anything that looks like a stream.

        for (String alias : aliases) {
            try {
                state = rolloverService.rolloverClusterState(
                    state,
                    alias,
                    null,
                    new CreateIndexRequest("_na"),
                    Collections.emptyList(),
                    true
                ).clusterState;
            } catch (Exception e) {
                logger.warn("Unable to rollover [{}]", alias);
            }
        }
        return state;
    }
}
