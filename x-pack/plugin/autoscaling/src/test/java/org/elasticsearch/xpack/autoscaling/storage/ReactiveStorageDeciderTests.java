/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReactiveStorageDeciderTests extends ESTestCase {

    public void testNoTierSpecified() {
        String attribute = randomBoolean() ? null : randomAlphaOfLength(8);
        String tier = attribute != null || randomBoolean() ? null : randomAlphaOfLength(8);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new ReactiveStorageDecider(attribute, tier));

        assertThat(exception.getMessage(), equalTo("must specify both [tier_attribute] [" + attribute + "] and [tier] [" + tier + "]"));
    }


    public void testScale() {
        int hotNodes = randomIntBetween(2, 10);
        ClusterState state = randomClusterStateAndInfo(hotNodes);

        ReactiveStorageDecider decider = new ReactiveStorageDecider("tier", "hot");

        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        AllocationDeciders allocationDeciders = new AllocationDeciders(ClusterModule.createAllocationDeciders(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), Collections.emptyList()));

        ClusterInfo underAllocated = createClusterInfo(state, 1L, 0L, Map.of());
        ClusterInfo overAllocated = createClusterInfo(state, 0L, 1L, Map.of());

        AutoscalingDeciderContextFactory contextFactory = (s, i) -> new TestAutoscalingDeciderContext(s, i, allocator, allocationDeciders);
        TestAutoscalingDeciderContext context = new TestAutoscalingDeciderContext(state, underAllocated, allocator, allocationDeciders);
        // todo: verify name+reason
        verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);

        ClusterState lastState = null;
        int count = 0;
        while (lastState != state && count++ < 1000) {
            lastState = state;
            // allocate some primaries and on later iterations replicas
            state = ReactiveStorageDecider.simulateStartAndAllocate(state, context);

            verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
            verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);
        }

        assertThat(lastState, sameInstance(state));
        assertTrue(state.getRoutingNodes().unassigned().isEmpty());
        assertThat(state.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(state.getRoutingNodes().shards(s -> true).size()));

        ClusterState extraNodeState = addExtraNodes(hotNodes, state);

        ClusterInfo overAllocatedWithEmptyExtra = createClusterInfo(extraNodeState, 0L, 1L, Map.of("extra-node", Long.MAX_VALUE));

        verifyDecision(extraNodeState, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(extraNodeState, overAllocatedWithEmptyExtra, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);

        state = addRandomIndices("additional", 1, hotNodes - 2, state);

        verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);

        extraNodeState = addExtraNodes(hotNodes, state);

        overAllocatedWithEmptyExtra = createClusterInfo(extraNodeState, 0L, 1L, Map.of("extra-node", Long.MAX_VALUE));
        verifyDecision(extraNodeState, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(extraNodeState, overAllocatedWithEmptyExtra, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
    }

    private void verifyDecision(ClusterState state, ClusterInfo info, ReactiveStorageDecider decider,
                                AutoscalingDeciderContextFactory contextFactory,
                                AutoscalingDecisionType expectedDecision) {
        assertThat(decider.scale(contextFactory.create(state, info)), decisionType(expectedDecision, state));
    }

    private ClusterState addExtraNodes(int hotNodes, ClusterState state) {
        ClusterState extraNodeState = state;
        for (int i = 0; i < hotNodes; ++i) {
            extraNodeState = addDataNode(extraNodeState, "hot", "extra-node");
        }
        return extraNodeState;
    }

    private ClusterInfo createClusterInfo(ClusterState state, long defaultFreeBytes, long shardSizes, Map<String, Long> override) {
        ClusterInfo info = mock(ClusterInfo.class);
        Map<String, DiskUsage> diskUsages =
            StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toMap(DiscoveryNode::getId,
            node -> {
                long free = override.getOrDefault(node.getName(), defaultFreeBytes);
                return new DiskUsage(node.getId(), null, "the_path", Math.max(1L, free), free);
            }));

        ImmutableOpenMap<String, DiskUsage> immutableDiskUsages =
            ImmutableOpenMap.<String, DiskUsage>builder().putAll(diskUsages).build();

        when(info.getNodeLeastAvailableDiskUsages()).thenReturn(immutableDiskUsages);
        when(info.getNodeMostAvailableDiskUsages()).thenReturn(immutableDiskUsages);

        when(info.getShardSize(any(), anyLong())).thenReturn(shardSizes);
        when(info.getDataPath(any())).thenReturn("the_path");
        return info;
    }

    private static interface AutoscalingDeciderContextFactory {
        AutoscalingDeciderContext create(ClusterState state, ClusterInfo info);
    }
    private static class TestAutoscalingDeciderContext implements AutoscalingDeciderContext {
        private ClusterState state;
        private ClusterInfo info;
        private ShardsAllocator shardsAllocator;
        private AllocationDeciders allocationDeciders;

        public TestAutoscalingDeciderContext(ClusterState state, ClusterInfo info, ShardsAllocator shardsAllocator, AllocationDeciders allocationDeciders) {
            this.state = state;
            this.info = info;
            this.shardsAllocator = shardsAllocator;
            this.allocationDeciders = allocationDeciders;
        }

        @Override
        public ClusterState state() {
            return state;
        }

        @Override
        public ClusterInfo info() {
            return info;
        }

        @Override
        public ShardsAllocator shardsAllocator() {
            return shardsAllocator;
        }

        @Override
        public AllocationDeciders allocationDeciders() {
            return allocationDeciders;
        }

        public TestAutoscalingDeciderContext withInfo(ClusterInfo info) {
            return new TestAutoscalingDeciderContext(state, info, shardsAllocator, allocationDeciders);
        }

        public TestAutoscalingDeciderContext withState(ClusterState state) {
            return new TestAutoscalingDeciderContext(state, info, shardsAllocator, allocationDeciders);
        }
    }

    private ClusterState randomClusterStateAndInfo(int hotNodes) {
        DiscoveryNodes nodes = randomDataNodes(hotNodes, randomIntBetween(1, 10));
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .nodes(nodes)
            .build();

        state = addRandomIndices("initial", hotNodes, hotNodes - 2, state);

        return state;
    }

    private ClusterState addRandomIndices(String prefix, int minShards, int maxReplicas, ClusterState state) {
        int shards = randomIntBetween(minShards, 100);
        Metadata.Builder builder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        while (shards > 0) {
            IndexMetadata indexMetadata = IndexMetadata.builder(prefix + "-" + shards)
                .settings(settings(Version.CURRENT)
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".tier", "hot"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, maxReplicas))
                .build();

            builder.put(indexMetadata, false);
            routingTableBuilder.addAsNew(indexMetadata);
            shards -= indexMetadata.getNumberOfShards() * (indexMetadata.getNumberOfReplicas() + 1);
        }

        return ClusterState.builder(state).metadata(builder).routingTable(routingTableBuilder.build()).build();
    }

    private DiscoveryNodes randomDataNodes(int hotNodes, int warmNodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        addataNodes("hot", builder, hotNodes);
        addataNodes("warm", builder, warmNodes);
        return builder.build();
    }

    private void addataNodes(String tier, DiscoveryNodes.Builder builder, int nodes) {
        IntStream.range(0, nodes).mapToObj(i -> newDataNode(tier, "node_" + i)).forEach(builder::add);
    }

    private DiscoveryNode newDataNode(String tier, String nodeName) {
        return new DiscoveryNode(nodeName, UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(),
            Map.of("tier", tier), Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
    }

    private ClusterState addDataNode(ClusterState state, String tier, String name) {
        return ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).add(newDataNode(tier, name))).build();
    }

    Matcher<AutoscalingDecision> decisionType(AutoscalingDecisionType type, ClusterState state) {
        return new TypeSafeMatcher<AutoscalingDecision>() {
            @Override
            protected boolean matchesSafely(AutoscalingDecision item) {
                return item.type() == type;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("decision of type ").appendValue(type).appendText(" for state\n" + Strings.toString(state, true,
                    true));
            }
        };
    }
}
