/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import com.carrotsearch.hppc.ObjectLookupContainer;
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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReactiveStorageDeciderTests extends ESTestCase {

    private static final List<String> SOME_ALLOCATION_DECIDERS = Arrays.asList(SameShardAllocationDecider.NAME,
        AwarenessAllocationDecider.NAME, EnableAllocationDecider.NAME);

    public void testNoTierSpecified() {
        String attribute = randomBoolean() ? null : randomAlphaOfLength(8);
        String tier = attribute != null || randomBoolean() ? null : randomAlphaOfLength(8);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new ReactiveStorageDecider(attribute, tier));

        assertThat(exception.getMessage(), equalTo("must specify both [tier_attribute] [" + attribute + "] and [tier] [" + tier + "]"));
    }


    public void testIsDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE));
        }
        decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(label -> new Decision.Single(randomValueOtherThan(Decision.Type.NO,
                () -> randomFrom(Decision.Type.values())), label, "test " + label))
            .forEach(decision::add);
        assertThat(ReactiveStorageDecider.isDiskOnlyNoDecision(decision), is(true));
    }

    public void testIsNotDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE, Decision.NO));
        }

        if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
            if (randomBoolean()) {
                decision.add(Decision.NO);
            } else {
                decision.add(new Decision.Single(Decision.Type.NO, randomFrom(SOME_ALLOCATION_DECIDERS), "test"));
            }
        } else if (randomBoolean()) {
            decision.add(new Decision.Single(Decision.Type.YES, DiskThresholdDecider.NAME, "test"));
        }
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(label -> new Decision.Single(randomFrom(Decision.Type.values()), label, "test " + label))
            .forEach(decision::add);

        assertThat(ReactiveStorageDecider.isDiskOnlyNoDecision(decision), is(false));
    }

    public void testScale() {
        int hotNodes = randomIntBetween(1, 8);
        // todo: work around DiskThresholdDecider single node early termination (no watermarks observed).
        int warmNodes = randomIntBetween(hotNodes == 1 ? 1 : 0, 3);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        // -1 to ensure we have one less shard copy than #nodes - since otherwise a copy may not be able to be allocated anywhere
        // else due to same shard allocator.
        state = addRandomIndices("initial", hotNodes, hotNodes - 1, state);

        state = addDataNodes("hot", "hot", state, hotNodes);
        state = addDataNodes("warm", "warm", state, warmNodes);

        ReactiveStorageDecider decider = new ReactiveStorageDecider("tier", "hot");

        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        AllocationDeciders allocationDeciders = new AllocationDeciders(ClusterModule.createAllocationDeciders(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), Collections.emptyList()));

        ClusterInfo underAllocated = createClusterInfo(state, 1L, 0L);
        // todo: warm has plenty space.
        ClusterInfo overAllocated = createClusterInfo(state, 0L, 1L);

        AutoscalingDeciderContextFactory contextFactory = (s, i) -> new TestAutoscalingDeciderContext(s, i, allocator, allocationDeciders);
        TestAutoscalingDeciderContext context = new TestAutoscalingDeciderContext(state, underAllocated, allocator, allocationDeciders);
        // todo: verify name+reason
        verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);

        ClusterState lastState = null;
        int count = 0;
        while (lastState != state && count++ < 1000) {
            lastState = state;
            state = startRandomShards(state, context);

            verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
            verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);
        }

        assertThat(lastState, sameInstance(state));
        assertTrue(state.getRoutingNodes().unassigned().isEmpty());
        assertThat(state.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(state.getRoutingNodes().shards(s -> true).size()));

        ClusterState extraNodeState = addDataNodes("hot", "extra", state, hotNodes);

        ToLongFunction<String> overrideExtraNodeFree = n -> n.startsWith("extra") ? Long.MAX_VALUE : 0L;
        ClusterInfo overAllocatedWithEmptyExtra = createClusterInfo(extraNodeState, overrideExtraNodeFree, 1L);

        verifyDecision(extraNodeState, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(extraNodeState, overAllocatedWithEmptyExtra, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);

        state = addRandomIndices("additional", 1, hotNodes - 1, state);

        verifyDecision(state, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(state, overAllocated, decider, contextFactory, AutoscalingDecisionType.SCALE_UP);

        extraNodeState = addDataNodes("hot", "extra", state, hotNodes);

        overAllocatedWithEmptyExtra = createClusterInfo(extraNodeState, overrideExtraNodeFree, 1L);
        verifyDecision(extraNodeState, underAllocated, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
        verifyDecision(extraNodeState, overAllocatedWithEmptyExtra, decider, contextFactory, AutoscalingDecisionType.NO_SCALE);
    }

    public void testSimulateStartAndAllocate() {
        int hotNodes = randomIntBetween(1, 8);
        int warmNodes = randomIntBetween(hotNodes == 1 ? 1 : 0, 3);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();

        state = addRandomIndices("initial", hotNodes, hotNodes - 1, state);

        state = addDataNodes("hot", "hot", state, hotNodes);
        state = addDataNodes("warm", "warm", state, warmNodes);

        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY);
        Collection<AllocationDecider> systemAllocationDeciders = ClusterModule.createAllocationDeciders(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), Collections.emptyList());
        AllocationDecider mockDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                String nodeName = node.node().getName();
                String nodeDigit = nodeName.substring(nodeName.length()-1);
                return (Integer.parseInt(nodeDigit) & 1) == (shardRouting.shardId().id() & 1) ? Decision.YES : Decision.NO;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return canAllocate(shardRouting, node, allocation);
            }
        };

        AllocationDeciders allocationDeciders =
            new AllocationDeciders(Stream.concat(Stream.of(mockDecider), systemAllocationDeciders.stream()).collect(Collectors.toList()));
        ClusterInfo underAllocated = createClusterInfo(state, 1L, 0L);
        TestAutoscalingDeciderContext context = new TestAutoscalingDeciderContext(state, underAllocated, allocator, allocationDeciders);

        ClusterState lastState = null;
        int maxRounds = state.getRoutingNodes().unassigned().size() + 1;
        int count = 0;
        while (lastState != state && count++ < maxRounds) {
            ClusterState simulatedState = ReactiveStorageDecider.simulateAllocationOfState(state, context);
            assertThat(simulatedState.nodes(), sameInstance(state.nodes()));
            assertThat(simulatedState.metadata().indices().keys().toArray(), equalTo(state.metadata().indices().keys().toArray()));
            assertThat(simulatedState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING), empty());
            assertThat(simulatedState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING), empty());
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, simulatedState.getRoutingNodes(), simulatedState,
                underAllocated,
                System.nanoTime());
            StreamSupport.stream(simulatedState.getRoutingNodes().spliterator(), false)
                .flatMap(n -> n.shardsWithState(ShardRoutingState.STARTED).stream().map(s -> Tuple.tuple(n, s)))
                .forEach(t -> validateAllocation(t.v1(), t.v2(), allocation));
            StreamSupport.stream(simulatedState.getRoutingNodes().unassigned().spliterator(), false)
                .forEach(s -> validateUnassigned(s, allocation));

            lastState = state;
            state = startRandomShards(state, context);
        }
        assertThat(state, sameInstance(lastState));
        assertThat(ReactiveStorageDecider.simulateAllocationOfState(state, context), sameInstance(state));
    }

    private void validateUnassigned(ShardRouting unassigned, RoutingAllocation allocation) {
        for (RoutingNode routingNode : allocation.routingNodes()) {
            assertThat(allocation.deciders().canAllocate(unassigned, routingNode, allocation).type(), not(equalTo(Decision.Type.YES)));
        }
    }

    private void validateAllocation(RoutingNode node, ShardRouting shard, RoutingAllocation allocation) {
        assertThat(allocation.deciders().canRemain(shard, node, allocation).type(), equalTo(Decision.Type.YES));
    }

    private ClusterState startRandomShards(ClusterState state, TestAutoscalingDeciderContext context) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(
            context.allocationDeciders(),
            routingNodes,
            state,
            context.info(),
            System.nanoTime()
        );

        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        List<ShardRouting> shards = randomSubsetOf(Math.min(randomIntBetween(1, 100), initializingShards.size()), initializingShards);

        // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
        shards.stream()
            .filter(s -> s.primary() == false)
            .forEach(s -> {
                allocation.routingNodes().startShard(logger, s, allocation.changes());
            });
        shards.stream()
            .filter(s -> s.primary() == true)
            .forEach(s -> {
                allocation.routingNodes().startShard(logger, s, allocation.changes());
            });
        context.shardsAllocator().allocate(allocation);

        // ensure progress by only relocating a shard if we started more than one shard.
        if (shards.size() > 1 && randomBoolean()) {
            List<ShardRouting> started = allocation.routingNodes().shardsWithState(ShardRoutingState.STARTED);
            if (started.isEmpty() == false) {
                ShardRouting toMove = randomFrom(started);
                Set<RoutingNode> candidates = StreamSupport.stream(routingNodes.spliterator(), false)
                    .filter(n -> allocation.deciders().canAllocate(toMove, n, allocation) == Decision.YES)
                    .collect(Collectors.toSet());
                if (candidates.isEmpty() == false) {
                    allocation.routingNodes().relocateShard(toMove, randomFrom(candidates).nodeId(), 0L, allocation.changes());
                }
            }
        }

        return ReactiveStorageDecider.updateClusterState(state, allocation);

    }

    private void verifyDecision(ClusterState state, ClusterInfo info, ReactiveStorageDecider decider,
                                AutoscalingDeciderContextFactory contextFactory,
                                AutoscalingDecisionType expectedDecision) {
        assertThat(decider.scale(contextFactory.create(state, info)), decisionType(expectedDecision, state));
    }

    private ClusterInfo createClusterInfo(ClusterState state, long defaultFreeBytes, long shardSizes) {
        return createClusterInfo(state, s -> defaultFreeBytes, shardSizes);

    }
    private ClusterInfo createClusterInfo(ClusterState state, ToLongFunction<String> freeBytesFunction, long shardSizes) {
        ClusterInfo info = mock(ClusterInfo.class);
        Map<String, DiskUsage> diskUsages =
            StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toMap(DiscoveryNode::getId,
            node -> {
                long free = freeBytesFunction.applyAsLong(node.getName());
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

    private ClusterState addDataNodes(String tier, String prefix, ClusterState state, int nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(state.nodes());
        IntStream.range(0, nodes).mapToObj(i -> newDataNode(tier, prefix + "_" + i)).forEach(builder::add);
        return ClusterState.builder(state).nodes(builder).build();
    }

    private static DiscoveryNode newDataNode(String tier, String nodeName) {
        return new DiscoveryNode(nodeName, UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(),
            Map.of("tier", tier), Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
    }

    private static enum NodeSet {
        hot("hot"),
        warm("warm"),
        extra("hot");

        private final String tier;

        NodeSet(String tier) {
            this.tier = tier;
        }

        ClusterState addDataNodes(ClusterState state, int nodes) {
            String prefix = name();
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(state.nodes());
            IntStream.range(0, nodes).mapToObj(i -> newDataNode(tier, prefix + "_" + i)).forEach(builder::add);
            return ClusterState.builder(state).nodes(builder).build();
        }

        static NodeSet fromNode(DiscoveryNode node) {
            return valueOf(node.getName().substring(0, node.getName().indexOf("_")));
        }
    }

    private static class ClusterInfoSpec {
        final long free;
        final long total;

        public ClusterInfoSpec(long free, long total) {
            this.free = free;
            this.total = total;
        }
    }

    private ClusterInfoSpec clusterInfo(long free, long total) {
        return new ClusterInfoSpec(free, total);
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
