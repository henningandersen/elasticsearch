package org.elasticsearch.xpack.ilm.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

public class AutoscalerTests extends ESTestCase {

    public void testScaleHot() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("new");
        lifecycleState.setAction("init");
        lifecycleState.setStep("init");
        IndexMetaData indexMetadata = IndexMetaData.builder("test-1")
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, "test")
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "test"))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .putAlias(AliasMetaData.builder("test"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);

        String nodeId2 = randomAlphaOfLength(10);
        DiscoveryNode node2 = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), false).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9301), nodeId2);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetadata, false))
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).add(node2).build())
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders yesDeciders = new AllocationDeciders(Collections.singleton(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings)));
        BalancedShardsAllocator shardsAllocator = new BalancedShardsAllocator(Settings.EMPTY, clusterSettings);
        EmptyClusterInfoService clusterInfoService = EmptyClusterInfoService.INSTANCE;
        GatewayAllocator noopGatewayAllocator = new GatewayAllocator() {
            @Override
            public void allocateUnassigned(RoutingAllocation allocation) {
                //noop
            }
        };
        AllocationService allocationService = new AllocationService(yesDeciders,
            noopGatewayAllocator,
            shardsAllocator,
            clusterInfoService);

        clusterState = allocationService.reroute(clusterState, "test");

        assertFalse(new Autoscaler(yesDeciders, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY)
            .scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));

        AllocationDeciders noDeciders = new AllocationDeciders(Collections.singleton(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.NO;
            }

            @Override
            public Decision canAllocate(IndexMetaData indexMetaData, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }

            @Override
            public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }

            @Override
            public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.NO;
            }
        }));

        assertTrue(new Autoscaler(noDeciders, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY)
            .scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));
    }
}
