/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.autoscaling.decider;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.RolloverAction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoscalerTests extends ESTestCase {

    public void test() {
        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            IndexMetadata indexMetadata = IndexMetadata.builder("test-1")
                .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "test"))
                .putAlias(AliasMetadata.builder("test"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(1)
                .build();

            ClusterState clusterState = twoNodesWithIndex(indexMetadata);

            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            AllocationDeciders yesDeciders = getAllocationDeciders(clusterSettings, Decision.YES, Decision.YES, Decision.YES);
            BalancedShardsAllocator shardsAllocator = new BalancedShardsAllocator(Settings.EMPTY, clusterSettings);
            EmptyClusterInfoService clusterInfoService = EmptyClusterInfoService.INSTANCE;
            GatewayAllocator noopGatewayAllocator = new GatewayAllocator() {
                @Override
                public void allocateUnassigned(RoutingAllocation allocation) {
                    // noop
                }
            };
            AllocationService allocationService = new AllocationService(
                yesDeciders,
                noopGatewayAllocator,
                shardsAllocator,
                clusterInfoService
            );

            clusterState = allocationService.reroute(clusterState, "test");

            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Collections.emptyList());

            AllocationDeciders aboveLowThreshold = getAllocationDeciders(clusterSettings, Decision.YES, Decision.NO, Decision.YES);

            AllocationDeciders aboveHighThreshold = getAllocationDeciders(clusterSettings, Decision.NO, Decision.NO, Decision.NO);

            Function<AllocationDeciders, Autoscaler> autoscalerFactory = deciders -> new Autoscaler(
                createRolloverService(
                    testThreadPool,
                    new AllocationService(deciders, noopGatewayAllocator, shardsAllocator, clusterInfoService)
                ),
                deciders,
                shardsAllocator
            );

            Autoscaler belowAutoscaler = autoscalerFactory.apply(yesDeciders);
            assertFalse(belowAutoscaler.scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));
            assertFalse(belowAutoscaler.reactiveScaleTier(clusterState, clusterInfoService.getClusterInfo(), i -> true));

            Autoscaler aboveLowAutoscaler = autoscalerFactory.apply(aboveLowThreshold);
            assertTrue(aboveLowAutoscaler.scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));
            assertFalse(aboveLowAutoscaler.reactiveScaleTier(clusterState, clusterInfoService.getClusterInfo(), i -> true));

            Autoscaler aboveHighAutoscaler = autoscalerFactory.apply(aboveHighThreshold);
            assertTrue(aboveHighAutoscaler.scaleHot(clusterState, clusterInfoService.getClusterInfo(), i -> true));
            assertTrue(aboveHighAutoscaler.reactiveScaleTier(clusterState, clusterInfoService.getClusterInfo(), i -> true));

            ClusterState futureBelowState = belowAutoscaler.simulateScaleHot(clusterState, clusterInfoService.getClusterInfo());
            assertTrue(futureBelowState.getRoutingNodes().unassigned().isEmpty());

            ClusterState futureAboveLowState = aboveLowAutoscaler.simulateScaleHot(clusterState, clusterInfoService.getClusterInfo());
            assertFalse(futureAboveLowState.getRoutingNodes().unassigned().isEmpty());
            assertFalse(
                StreamSupport.stream(futureAboveLowState.getRoutingNodes().unassigned().spliterator(), false)
                    .anyMatch(ShardRouting::primary)
            );

            ClusterState futureAboveHighState = aboveHighAutoscaler.simulateScaleHot(clusterState, clusterInfoService.getClusterInfo());
            assertFalse(futureAboveHighState.getRoutingNodes().unassigned().isEmpty());
            assertTrue(
                StreamSupport.stream(futureAboveHighState.getRoutingNodes().unassigned().spliterator(), false)
                    .anyMatch(ShardRouting::primary)
            );
        } finally {
            testThreadPool.shutdown();
        }
    }

    private MetadataRolloverService createRolloverService(ThreadPool testThreadPool, AllocationService allocationService) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool);
        Environment env = mock(Environment.class);
        when(env.sharedDataFile()).thenReturn(null);
        IndicesService indicesService;
        try {
            indicesService = mockIndicesServices();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(mockIndexNameExpressionResolver.resolveDateMathExpression(any())).then(returnsFirstArg());

        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            allocationService,
            null,
            env,
            null,
            testThreadPool,
            null,
            Collections.emptyList(),
            false
        );
        MetadataIndexAliasesService indexAliasesService = new MetadataIndexAliasesService(
            clusterService,
            indicesService,
            new AliasValidator(),
            null,
            xContentRegistry()
        );
        return new MetadataRolloverService(testThreadPool, createIndexService, indexAliasesService, mockIndexNameExpressionResolver);
    }

    private AllocationDeciders getAllocationDeciders(
        ClusterSettings clusterSettings,
        Decision skipLowThresholdAllocateDecision,
        Decision allocateDecision,
        Decision remainDecision
    ) {
        return new AllocationDeciders(List.of(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                boolean skipLowTresholdChecks = shardRouting.primary()
                    && shardRouting.active() == false
                    && shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE;
                return skipLowTresholdChecks ? skipLowThresholdAllocateDecision : allocateDecision;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return remainDecision;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return canAllocate(shardRouting, node, allocation);
            }
        }));
    }

    private ClusterState twoNodesWithIndex(IndexMetadata indexMetadata) {
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = new DiscoveryNode(
            "node1",
            nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Map.of("datax", "hot"),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        String nodeId2 = randomAlphaOfLength(10);
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            nodeId2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            Map.of("datax", "warm"),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).add(node2).build())
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();
    }

    @SuppressWarnings("unchecked")
    private IndicesService mockIndicesServices() throws Exception {
        /*
         * Throws Exception because Eclipse uses the lower bound for
         * CheckedFunction's exception type so it thinks the "when" call
         * can throw Exception. javac seems to be ok inferring something
         * else.
         */
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.withTempIndexService(any(IndexMetadata.class), any(CheckedFunction.class))).then(invocationOnMock -> {
            IndexService indexService = mock(IndexService.class);
            IndexMetadata indexMetadata = (IndexMetadata) invocationOnMock.getArguments()[0];
            when(indexService.index()).thenReturn(indexMetadata.getIndex());
            MapperService mapperService = mock(MapperService.class);
            when(indexService.mapperService()).thenReturn(mapperService);
            when(mapperService.documentMapper()).thenReturn(null);
            when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {
            });
            when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
            return ((CheckedFunction) invocationOnMock.getArguments()[1]).apply(indexService);
        });
        return indicesService;
    }
}
