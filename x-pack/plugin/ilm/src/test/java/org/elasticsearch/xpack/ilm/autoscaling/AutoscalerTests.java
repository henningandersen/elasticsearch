/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

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

        ClusterState clusterState = twoNodesWithIndex(indexMetadata);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders yesDeciders = new AllocationDeciders(Collections.singleton(new SameShardAllocationDecider(Settings.EMPTY,
            clusterSettings)));
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

        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Collections.emptyList());
        assertFalse(new Autoscaler(yesDeciders, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY, xContentRegistry)
            .scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));

        AllocationDeciders aboveThreshold = new AllocationDeciders(List.of(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return canAllocate(shardRouting, allocation);
                }

                @Override
                public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return Decision.NO;
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {

                    boolean skipLowTresholdChecks = shardRouting.primary() &&
                        shardRouting.active() == false && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;
                    return skipLowTresholdChecks ? Decision.YES : Decision.NO;
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
                    return canAllocate(shardRouting, node, allocation);
                }
            }));

        assertTrue(new Autoscaler(aboveThreshold, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY, xContentRegistry)
            .scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));

        ClusterState scaledState = new Autoscaler(aboveThreshold, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY, xContentRegistry)
            .simulateScaleHot(clusterState, clusterInfoService.getClusterInfo());

        assertFalse(StreamSupport.stream(scaledState.getRoutingNodes().unassigned().spliterator(), false).anyMatch(ShardRouting::primary));
        assertFalse(scaledState.getRoutingNodes().unassigned().isEmpty());
    }

    private ClusterState twoNodesWithIndex(IndexMetaData indexMetadata) {
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);

        String nodeId2 = randomAlphaOfLength(10);
        DiscoveryNode node2 = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(Node.NODE_MASTER_SETTING.getKey(), false).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9301), nodeId2);

        return ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetadata, false)
                .putCustom(IndexLifecycleMetadata.TYPE,
                    new IndexLifecycleMetadata(Map.of("test", new LifecyclePolicyMetadata(policy(), null, 0, 0)), OperationMode.RUNNING))
            )
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).add(node2).build())
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();
    }


    public void testScaleILM() {
        IndexMetaData indexMetadata = IndexMetaData.builder("test-1")
            .settings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_NAME, "test")
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "test"))
            .putAlias(AliasMetaData.builder("test"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        ClusterState clusterState = twoNodesWithIndex(indexMetadata);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders yesDeciders = new AllocationDeciders(Collections.singleton(new SameShardAllocationDecider(Settings.EMPTY,
            clusterSettings)));
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

        NamedXContentRegistry xContentRegistry = xContentRegistry();
        Autoscaler autoscaler = new Autoscaler(yesDeciders, shardsAllocator, new IndexNameExpressionResolver(), Settings.EMPTY, xContentRegistry);
        assertFalse(runScaleILM(autoscaler,clusterState, clusterInfoService.getClusterInfo(), i -> true));
    }

    private boolean runScaleILM(Autoscaler autoscaler, ClusterState clusterState, ClusterInfo clusterInfo, Predicate<IndexMetaData> predicate) {
        PlainActionFuture<Boolean> result = new PlainActionFuture<>();
        autoscaler.scaleILM(clusterState, clusterInfo, predicate, result);
        return result.actionGet();
    }

    private LifecyclePolicy policy() {
        RolloverAction rollover = new RolloverAction(ByteSizeValue.ZERO, TimeValue.timeValueSeconds(1), null);
        Phase hot = new Phase("hot", TimeValue.ZERO, Map.of("rollover", rollover));
        AllocateAction allocate = new AllocateAction(null, null, null, Map.of("data", "warm"));
        Phase warm = new Phase("warm", TimeValue.timeValueSeconds(2), Map.of("allocate", allocate));
        return new LifecyclePolicy("test", Map.of("warm", warm, "hot", hot));
    }

    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                (p) -> TimeseriesLifecycleType.INSTANCE),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse)
        ));
        return new NamedXContentRegistry(entries);
    }
}
