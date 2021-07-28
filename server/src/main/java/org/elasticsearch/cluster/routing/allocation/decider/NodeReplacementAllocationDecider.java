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
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

public class NodeReplacementAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_replacement";

    private static final Decision NO_REPLACEMENTS = Decision.single(Decision.Type.YES, NAME,
        "no node replacements are currently ongoing, allocation is allowed");

    public static boolean replacementOngoing(Metadata metadata) {
        return metadata.nodeShutdowns().values().stream()
            // TODO: change this to "REPLACE" type
            .anyMatch(shutdown -> shutdown.getType().equals(SingleNodeShutdownMetadata.Type.REMOVE));
    }

    public static boolean isReplacementSource(Metadata metadata, String nodeId) {
//        Optional.ofNullable(metadata.nodeShutdowns().get(nodeId))
//            .map();
        return false;
    }

    public static boolean isReplacementTarget(Metadata metadata, String nodeId) {
        return false;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTarget(allocation.metadata(), node.nodeId())) {
            // The target node is a replacement target, that means we only allow allocating shards from the source node to the target
            if (isReplacementSource(allocation.metadata(), shardRouting.currentNodeId())) {
                return Decision.single(Decision.Type.YES, NAME,
                    "node [{}] is replacing node [{}], and may receive shards from it", node.nodeId(), "source");
            } else {
                return Decision.single(Decision.Type.NO, NAME,
                    "node [{}] is replacing node [{}], so no data from other nodes may be allocated to it", node.nodeId(), "source");
            }
        } else {
            // The node in question is not a replacement target, so allow allocation.

            // You might be wondering: "why do we allow allocating to the node that is being
            // replaced? Isn't it going to just move right off of the node anyway?"
            //
            // The answer is that if we were to forbid allocation to the source of a REPLACE shutdown,
            // then we could run into a situation where a user only has two nodes in the cluster—
            // nodeA being replaced by nodeB. Since we don't allow allocation to nodeB unless it's
            // relocating from nodeA, we would not be able to create a brand-new index because
            // it would be allocatable to neither nodeA nor nodeB. So we allow allocating to nodeA
            // even though we'll turn around and have to move it elsewhere soon.
            return Decision.ALWAYS;
        }
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementSource(allocation.metadata(), node.nodeId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [{}] is being replaced by node [{}], so no data may remain on it", node.nodeId(), "replacement");
        } else {
            return Decision.single(Decision.Type.YES, NAME, "node [{}] is not being replaced", node.nodeId());
        }
    }

    /**
     * See the comment in the else branch of {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)}
     * for a reason why we allow allocation that may potentially allocate to the source of a node
     * replacement shutdown.
     */
    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTarget(allocation.metadata(), node.nodeId())) {
            // The target node is a replacement target, that means we only allow allocating shards
            // from the source node to the target, since this index has no source node, we disallow it.
            return Decision.single(Decision.Type.NO, NAME,
                "node [{}] is replacing node [{}], so no other data may be allocated to it", node.nodeId(), "source");
        } else {
            // The node in question is not a replacement target, so allow allocation.
            return Decision.ALWAYS;
        }
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (replacementOngoing(allocation.metadata()) == false) {
            return NO_REPLACEMENTS;
        } else if (isReplacementTarget(allocation.metadata(), node.getId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [{}] is replacing node [{}], shards cannot auto expand to be on it", node.getId(), "source");
        } else if (isReplacementSource(allocation.metadata(), node.getId())) {
            return Decision.single(Decision.Type.NO, NAME,
                "node [{}] is being replaced by node [{}], shards cannot auto expand to be on it", node.getId(), "replacement");
        } else {
            return Decision.ALWAYS;
        }
    }
}
