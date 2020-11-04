/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Set;

public interface AutoscalingDeciderContext {
    ClusterState state();

    /**
     * Return current capacity of nodes governed by the policy. Can be null if the capacity of some nodes is unavailable. If a decider
     * relies on this value and gets a null current capacity, it should return a result with a null requiredCapacity (undecided).
     */
    AutoscalingCapacity currentCapacity();

    /**
     * Return the nodes governed by the policy.
     */
    Set<DiscoveryNode> nodes();

    /**
     * Return the roles of the policy
     */
    Set<String> roles();

    ClusterInfo info();

    SnapshotShardSizeInfo snapshotShardSizeInfo();

    ShardsAllocator shardsAllocator();

    AllocationDeciders allocationDeciders();
}
