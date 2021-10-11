/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

public class NoThrottlingAllocationDeciders extends AllocationDeciders {
    public NoThrottlingAllocationDeciders(AllocationDeciders delegate) {
        super(delegate);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Decision decision = super.canAllocate(shardRouting, node, allocation);
        if (decision.type() == Decision.Type.THROTTLE) {
            return Decision.YES;
        } else {
            return decision;
        }
    }
}
