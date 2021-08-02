/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class NodeReplacementAllocationDeciderTests  extends ESAllocationTestCase {
    private static final DiscoveryNode NODE_A = newNode("node-a", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private static final DiscoveryNode NODE_B = newNode("node-b", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created")
    );
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private NodeReplacementAllocationDecider decider = new NodeReplacementAllocationDecider();
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            decider,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new NodeShutdownAllocationDecider()
        )
    );
    private final AllocationService service = new AllocationService(
        allocationDeciders,
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE
    );

    private final String idxName = "test-idx";
    private final String idxUuid = "test-idx-uuid";
    private final IndexMetadata indexMetadata = IndexMetadata.builder(idxName)
        .settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, idxUuid)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        )
        .build();

    public void testCanAllocateToSourceButNotTarget() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"), NODE_A.getId(), NODE_B.getId());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        RoutingNode routingNode = new RoutingNode(NODE_A.getId(), NODE_A, shard);
        allocation.debugDecision(true);

        Decision decision = decider.canAllocate(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + NODE_A.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    private ClusterState prepareState(ClusterState initialState, String sourceNode, String targetNode) {
        final SingleNodeShutdownMetadata nodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
            .setNodeId(sourceNode)
            // .setReplacementTarget(targetNode)
            .setType(SingleNodeShutdownMetadata.Type.RESTART) // TODO: use replace
            .setReason(this.getTestName())
            .setStartedAtMillis(1L)
            .build();
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(new HashMap<>()).putSingleNodeMetadata(
            nodeShutdownMetadata
        );
        return ClusterState.builder(initialState)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).build())
            .metadata(Metadata.builder().put(IndexMetadata.builder(indexMetadata))
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata))
            .build();
    }
}
