/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * A transparent wrapper around an {@link AllocationDecider} that overrides the
 * provided decider in specific cases to allow node replacement to override
 * allocation deciders that would otherwise prevent the shard from moving to
 * the replacement target
 */
public class NodeReplaceOverrideWrapper extends AllocationDecider {

    private final AllocationDecider original;

    public NodeReplaceOverrideWrapper(AllocationDecider original) {
        this.original = original;
    }

    public AllocationDecider getOriginal() {
        return original;
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return original.canRebalance(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final Metadata metadata = allocation.metadata();
        // If the node is on the source node and moving to the target node,
        // always allow it, regardless of what the wrapped decider would say.
        // This overrides things like disk space, etc.
        if (isReplacementTarget(metadata, node.nodeId()) && isReplacementSource(metadata, shardRouting.currentNodeId())) {
            return Decision.single(Decision.Type.YES, original.getName(),
                "node [%s] is being replaced by [%s] and may receive shards from it", node.nodeId(), shardRouting.currentNodeId());
        } else {
            return original.canAllocate(shardRouting, node, allocation);
        }
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return original.canRemain(shardRouting, node, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return original.canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return original.canAllocate(indexMetadata, node, allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return original.shouldAutoExpandToNode(indexMetadata, node, allocation);
    }

    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        return original.canRebalance(allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return original.canForceAllocatePrimary(shardRouting, node, allocation);
    }

    @Override
    public String getName() {
        return "node_replace_wrapped[" + original.getName() + "]";
    }
}
