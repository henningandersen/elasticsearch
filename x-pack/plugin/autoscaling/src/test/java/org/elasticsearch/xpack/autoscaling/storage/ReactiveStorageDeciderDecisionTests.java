/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Test the higher level parts of {@link ReactiveStorageDeciderService} that all require a similar setup.
 */
public class ReactiveStorageDeciderDecisionTests extends AutoscalingTestCase {
    private static final Logger logger = LogManager.getLogger(ReactiveStorageDeciderDecisionTests.class);

    private static final AllocationDecider CAN_ALLOCATE_NO_DECIDER = new AllocationDecider() {
        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
        }
    };
    private static final AllocationDecider CAN_REMAIN_NO_DECIDER = new AllocationDecider() {
        @Override
        public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
        }
    };
    private static final BalancedShardsAllocator SHARDS_ALLOCATOR = new BalancedShardsAllocator(Settings.EMPTY);

    private ClusterState state;
    private final int hotNodes = randomIntBetween(1, 8);
    private final int warmNodes = randomIntBetween(1, 3);
    // these are the shards that the decider tests work on
    private Set<ShardId> subjectShards;
    // say NO with disk label for subject shards
    private AllocationDecider mockCanAllocateDiskDecider = new AllocationDecider() {
        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) return allocation.decision(
                Decision.NO,
                DiskThresholdDecider.NAME,
                "test"
            );
            return super.canAllocate(shardRouting, node, allocation);
        }
    };
    // say NO with disk label for subject shards
    private AllocationDecider mockCanRemainDiskDecider = new AllocationDecider() {
        @Override
        public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) return allocation.decision(
                Decision.NO,
                DiskThresholdDecider.NAME,
                "test"
            );
            return super.canAllocate(shardRouting, node, allocation);
        }
    };

    @Before
    public void setup() {
        DiscoveryNode.setAdditionalRoles(
            Set.of(DataTier.DATA_CONTENT_NODE_ROLE, DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE, DataTier.DATA_COLD_NODE_ROLE)
        );
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = addRandomIndices(hotNodes, hotNodes, state);
        state = addDataNodes("data_hot", "hot", state, hotNodes);
        state = addDataNodes("data_warm", "warm", state, warmNodes);
        this.state = state;

        Set<ShardId> shardIds = shardIds(state.getRoutingNodes().unassigned());
        this.subjectShards = new HashSet<>(randomSubsetOf(randomIntBetween(1, shardIds.size()), shardIds));
    }

    public void testStoragePreventsAllocation() {
        ClusterState lastState = null;
        int maxRounds = state.getRoutingNodes().unassigned().size() + 3; // (allocated + start + detect-same)
        int round = 0;
        while (lastState != state && round < maxRounds) {
            boolean prevents = hasAllocatableSubjectShards();
            assert round != 0 || prevents;
            verify(ReactiveStorageDeciderService::storagePreventsAllocation, prevents, mockCanAllocateDiskDecider);
            verify(ReactiveStorageDeciderService::storagePreventsAllocation, false, mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
            verify(ReactiveStorageDeciderService::storagePreventsAllocation, false);
            if (hasUnassignedSubjectShards()) {
                verifyScale(1, "not enough storage available for unassigned shards", mockCanAllocateDiskDecider);
            } else {
                assert prevents == false;
                verifyScale(0, "storage ok", mockCanAllocateDiskDecider);
            }
            verifyScale(0, "storage ok", mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
            verifyScale(0, "storage ok");
            verifyScale(addDataNodes("data_hot", "additional", state, hotNodes), 0, "storage ok", mockCanAllocateDiskDecider);
            lastState = state;
            startRandomShards();
            ++round;
        }
        assert round > 0;
        assertThat(state, sameInstance(lastState));
        assertThat(
            ReactiveStorageDeciderService.simulateStartAndAllocate(state, createContext(mockCanAllocateDiskDecider)),
            sameInstance(state)
        );
    }

    public void testStoragePreventsMove() {
        // allocate shards (will allocate all primaries)
        allocate();

        // pick set of shards to force on to warm nodes.
        Set<ShardId> warmShards = Sets.union(new HashSet<>(randomSubsetOf(shardIds(state.getRoutingNodes().unassigned()))), subjectShards);

        // start warm shards. Only use primary shards for simplicity.
        withRoutingAllocation(
            allocation -> allocation.routingNodes()
                .shardsWithState(ShardRoutingState.INITIALIZING)
                .stream()
                .filter(ShardRouting::primary)
                .filter(s -> warmShards.contains(s.shardId()))
                .forEach(shard -> allocation.routingNodes().startShard(logger, shard, allocation.changes()))
        );

        do {
            startRandomShards();
            // all of the relevant replicas are assigned too.
        } while (StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .map(ShardRouting::shardId)
            .anyMatch(warmShards::contains));

        // relocate warm shards to warm nodes and start them
        withRoutingAllocation(
            allocation -> allocation.routingNodes()
                .shardsWithState(ShardRoutingState.STARTED)
                .stream()
                .filter(ShardRouting::primary)
                .filter(s -> warmShards.contains(s.shardId()))
                .forEach(
                    shard -> allocation.routingNodes()
                        .startShard(
                            logger,
                            allocation.routingNodes()
                                .relocateShard(shard, randomNodeId(allocation.routingNodes(), "warm"), 0L, allocation.changes())
                                .v2(),
                            allocation.changes()
                        )
                )
        );

        verify(ReactiveStorageDeciderService::storagePreventsRemainOrMove, true, mockCanAllocateDiskDecider);
        verify(ReactiveStorageDeciderService::storagePreventsRemainOrMove, false, mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verify(ReactiveStorageDeciderService::storagePreventsRemainOrMove, false);

        verifyScale(1, "not enough storage available for assigned shards", mockCanAllocateDiskDecider);
        verifyScale(0, "storage ok", mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok");
        verifyScale(addDataNodes("data_hot", "additional", state, hotNodes), 0, "storage ok", mockCanAllocateDiskDecider);
    }

    public void testStoragePreventsRemain() {
        allocate();
        // we can only decide on a move for started shards (due to for instance ThrottlingAllocationDecider assertion).
        for (int i = 0; i < randomIntBetween(1, 4) || hasStartedSubjectShard() == false; ++i) {
            startRandomShards();
        }

        verify(ReactiveStorageDeciderService::storagePreventsRemainOrMove, true, mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verify(
            ReactiveStorageDeciderService::storagePreventsRemainOrMove,
            false,
            mockCanRemainDiskDecider,
            CAN_REMAIN_NO_DECIDER,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService::storagePreventsRemainOrMove, false);

        // maybe relocate, which means no longer started. This is OK for scale, since it simulates start.
        startRandomShards();

        verifyScale(1, "not enough storage available for assigned shards", mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", mockCanRemainDiskDecider, CAN_REMAIN_NO_DECIDER, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok");
    }

    public void testSimulateStartAndAllocate() {
        AllocationDecider mockDecider = new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                String nodeName = node.node().getName();
                String nodeDigit = nodeName.substring(nodeName.length() - 1);
                return (Integer.parseInt(nodeDigit) & 1) == (shardRouting.shardId().id() & 1) ? Decision.YES : Decision.NO;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return canAllocate(shardRouting, node, allocation);
            }
        };

        ClusterState lastState = null;
        int maxRounds = state.getRoutingNodes().unassigned().size() + 3; // (allocate + start + detect-same)
        int round = 0;
        while (lastState != state && round < maxRounds) {
            ClusterState simulatedState = ReactiveStorageDeciderService.simulateStartAndAllocate(state, createContext(mockDecider));
            assertThat(simulatedState.nodes(), sameInstance(state.nodes()));
            assertThat(simulatedState.metadata().indices().keys().toArray(), equalTo(state.metadata().indices().keys().toArray()));
            assertThat(simulatedState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING), empty());
            assertThat(simulatedState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING), empty());
            RoutingAllocation allocation = createRoutingAllocation(simulatedState, createAllocationDeciders(mockDecider));
            StreamSupport.stream(simulatedState.getRoutingNodes().spliterator(), false)
                .flatMap(n -> n.shardsWithState(ShardRoutingState.STARTED).stream().map(s -> Tuple.tuple(n, s)))
                .forEach(t -> verifyAllocation(t.v1(), t.v2(), allocation));
            StreamSupport.stream(simulatedState.getRoutingNodes().unassigned().spliterator(), false)
                .forEach(s -> verifyUnassigned(s, allocation));

            lastState = state;
            startRandomShards(createAllocationDeciders(mockDecider));
            ++round;
        }
        assert round > 0;
        assertThat(state, sameInstance(lastState));
        assertThat(ReactiveStorageDeciderService.simulateStartAndAllocate(state, createContext(mockDecider)), sameInstance(state));
    }

    public interface BooleanVerificationSubject {
        boolean invoke(
            ClusterState state,
            AutoscalingDeciderContext context,
            Predicate<IndexMetadata> indexMetadataPredicate,
            Predicate<DiscoveryNode> nodePredicate
        );
    }

    public void verify(BooleanVerificationSubject subject, boolean expected, AllocationDecider... allocationDeciders) {
        Predicate<DiscoveryNode> hotNodePredicate = n -> n.getName().startsWith("hot");
        assertThat(subject.invoke(state, createContext(allocationDeciders), i -> true, hotNodePredicate), equalTo(expected));
    }

    public void verifyScale(int direction, String reason, AllocationDecider... allocationDeciders) {
        verifyScale(state, direction, reason, allocationDeciders);
    }

    public static void verifyScale(ClusterState state, int direction, String reason, AllocationDecider... allocationDeciders) {
        ReactiveStorageDeciderService decider = new ReactiveStorageDeciderService();
        TestAutoscalingDeciderContext context = createContext(
            state,
            Set.of(DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE),
            allocationDeciders
        );
        AutoscalingDeciderResult result = decider.scale(new ReactiveStorageDeciderConfiguration(), context);
        // todo: change this to be deterministic tests.
        if (context.currentCapacity != null && context.currentCapacity.tier() != null && context.currentCapacity.tier().storage() != null) {
            assertThat(
                Long.signum(result.requiredCapacity().tier().storage().getBytes() - context.currentCapacity.tier().storage().getBytes()),
                equalTo(direction)
            );
            assertThat(result.reason().summary(), equalTo(reason));
        } else {
            assertThat(result.requiredCapacity(), is(nullValue()));
            assertThat(result.reason().summary(), equalTo("current capacity not available"));
        }
    }

    private void verifyUnassigned(ShardRouting unassigned, RoutingAllocation allocation) {
        for (RoutingNode routingNode : allocation.routingNodes()) {
            assertThat(allocation.deciders().canAllocate(unassigned, routingNode, allocation).type(), not(equalTo(Decision.Type.YES)));
        }
    }

    private void verifyAllocation(RoutingNode node, ShardRouting shard, RoutingAllocation allocation) {
        assertThat(allocation.deciders().canRemain(shard, node, allocation).type(), equalTo(Decision.Type.YES));
    }

    private boolean hasAllocatableSubjectShards() {
        AllocationDeciders deciders = createAllocationDeciders();
        RoutingAllocation allocation = createRoutingAllocation(state, createAllocationDeciders());
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .filter(shard -> subjectShards.contains(shard.shardId()))
            .anyMatch(
                shard -> StreamSupport.stream(allocation.routingNodes().spliterator(), false)
                    .anyMatch(node -> deciders.canAllocate(shard, node, allocation) != Decision.NO)
            );
    }

    private boolean hasUnassignedSubjectShards() {
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .anyMatch(shard -> subjectShards.contains(shard.shardId()));
    }

    private boolean hasStartedSubjectShard() {
        return state.getRoutingNodes()
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::primary)
            .map(ShardRouting::shardId)
            .anyMatch(subjectShards::contains);
    }

    private static AllocationDeciders createAllocationDeciders(AllocationDecider... extraDeciders) {
        Set<Setting<?>> allSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING,
                DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING,
                DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING
            )
        ).collect(Collectors.toSet());

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, allSettings);
        Collection<AllocationDecider> systemAllocationDeciders = ClusterModule.createAllocationDeciders(
            Settings.builder()
                .put(
                    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                    Integer.MAX_VALUE
                )
                .build(),
            clusterSettings,
            Collections.emptyList()
        );
        return new AllocationDeciders(
            Stream.of(
                Stream.of(extraDeciders),
                Stream.of(new DataTierAllocationDecider(clusterSettings)),
                systemAllocationDeciders.stream()
            ).flatMap(s -> s).collect(Collectors.toList())
        );
    }

    private static RoutingAllocation createRoutingAllocation(ClusterState state, AllocationDeciders allocationDeciders) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        return new RoutingAllocation(allocationDeciders, routingNodes, state, createClusterInfo(state), null, System.nanoTime());
    }

    private void withRoutingAllocation(Consumer<RoutingAllocation> block) {
        RoutingAllocation allocation = createRoutingAllocation(state, createAllocationDeciders());
        block.accept(allocation);
        state = ReactiveStorageDeciderService.updateClusterState(state, allocation);
    }

    private void allocate() {
        withRoutingAllocation(SHARDS_ALLOCATOR::allocate);
    }

    private void startRandomShards() {
        startRandomShards(createAllocationDeciders());
    }

    private void startRandomShards(AllocationDeciders allocationDeciders) {
        RoutingAllocation allocation = createRoutingAllocation(state, allocationDeciders);

        List<ShardRouting> initializingShards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
        initializingShards.sort(Comparator.comparing(ShardRouting::shardId).thenComparing(ShardRouting::primary, Boolean::compare));
        List<ShardRouting> shards = randomSubsetOf(Math.min(randomIntBetween(1, 100), initializingShards.size()), initializingShards);

        // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
        shards.stream()
            .filter(Predicate.not(ShardRouting::primary))
            .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
        shards.stream()
            .filter(ShardRouting::primary)
            .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
        SHARDS_ALLOCATOR.allocate(allocation);

        // ensure progress by only relocating a shard if we started more than one shard.
        if (shards.size() > 1 && randomBoolean()) {
            List<ShardRouting> started = allocation.routingNodes().shardsWithState(ShardRoutingState.STARTED);
            if (started.isEmpty() == false) {
                ShardRouting toMove = randomFrom(started);
                Set<RoutingNode> candidates = StreamSupport.stream(allocation.routingNodes().spliterator(), false)
                    .filter(n -> allocation.deciders().canAllocate(toMove, n, allocation) == Decision.YES)
                    .collect(Collectors.toSet());
                if (candidates.isEmpty() == false) {
                    allocation.routingNodes().relocateShard(toMove, randomFrom(candidates).nodeId(), 0L, allocation.changes());
                }
            }
        }

        state = ReactiveStorageDeciderService.updateClusterState(state, allocation);

    }

    private TestAutoscalingDeciderContext createContext(AllocationDecider... allocationDeciders) {
        return createContext(state, Set.of(DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE), allocationDeciders);
    }

    private TestAutoscalingDeciderContext createContext(DiscoveryNodeRole role, AllocationDecider... allocationDeciders) {
        return createContext(state, Set.of(role), allocationDeciders);
    }

    private static TestAutoscalingDeciderContext createContext(
        ClusterState state,
        Set<DiscoveryNodeRole> roles,
        AllocationDecider... allocationDeciders
    ) {
        final BalancedShardsAllocator shardsAllocator = new BalancedShardsAllocator(Settings.EMPTY);
        return new TestAutoscalingDeciderContext(
            state,
            roles,
            shardsAllocator,
            createAllocationDeciders(allocationDeciders),
            randomAutoscalingCapacity()
        );
    }

    private static class TestAutoscalingDeciderContext implements AutoscalingDeciderContext {
        private final ClusterState state;
        private final ShardsAllocator shardsAllocator;
        private final AllocationDeciders allocationDeciders;
        private final AutoscalingCapacity currentCapacity;
        private final Set<DiscoveryNode> nodes;
        private final Set<DiscoveryNodeRole> roles;
        private ClusterInfo info;

        private TestAutoscalingDeciderContext(
            ClusterState state,
            Set<DiscoveryNodeRole> roles,
            ShardsAllocator shardsAllocator,
            AllocationDeciders allocationDeciders,
            AutoscalingCapacity currentCapacity
        ) {
            this.state = state;
            this.shardsAllocator = shardsAllocator;
            this.allocationDeciders = allocationDeciders;
            this.currentCapacity = currentCapacity;
            this.roles = roles;
            this.nodes = StreamSupport.stream(state.nodes().spliterator(), false)
                .filter(n -> roles.stream().anyMatch(n.getRoles()::contains))
                .collect(Collectors.toSet());
        }

        @Override
        public ClusterState state() {
            return state;
        }

        @Override
        public AutoscalingCapacity currentCapacity() {
            return currentCapacity;
        }

        @Override
        public Set<DiscoveryNode> nodes() {
            return nodes;
        }

        @Override
        public Set<DiscoveryNodeRole> roles() {
            return roles;
        }

        @Override
        public ClusterInfo info() {
            if (info == null) {
                info = createClusterInfo(state);
            }
            return info;
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizeInfo() {
            return null;
        }

        @Override
        public ShardsAllocator shardsAllocator() {
            return shardsAllocator;
        }

        @Override
        public AllocationDeciders allocationDeciders() {
            return allocationDeciders;
        }
    }

    private static ClusterInfo createClusterInfo(ClusterState state) {
        // testing does not depend on info so just pretend infinite space to let regular disk decider pass
        Map<String, DiskUsage> diskUsages = StreamSupport.stream(state.nodes().spliterator(), false)
            .collect(Collectors.toMap(DiscoveryNode::getId, node -> new DiskUsage(node.getId(), null, "the_path", 1, 1)));
        ImmutableOpenMap<String, DiskUsage> immutableDiskUsages = ImmutableOpenMap.<String, DiskUsage>builder().putAll(diskUsages).build();

        return new ClusterInfo() {
            @Override
            public ImmutableOpenMap<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
                return immutableDiskUsages;
            }

            @Override
            public ImmutableOpenMap<String, DiskUsage> getNodeMostAvailableDiskUsages() {
                return immutableDiskUsages;
            }

            @Override
            public String getDataPath(ShardRouting shardRouting) {
                return "the_path";
            }
        };
    }

    private static ClusterState addRandomIndices(int minShards, int maxShardCopies, ClusterState state) {
        String[] tierSettingNames = new String[] {
            DataTierAllocationDecider.INDEX_ROUTING_REQUIRE,
            DataTierAllocationDecider.INDEX_ROUTING_INCLUDE,
            DataTierAllocationDecider.INDEX_ROUTING_PREFER };
        int shards = randomIntBetween(minShards, 20);
        Metadata.Builder builder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        while (shards > 0) {
            IndexMetadata indexMetadata = IndexMetadata.builder("test" + "-" + shards)
                .settings(settings(Version.CURRENT).put(randomFrom(tierSettingNames), "data_hot"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, maxShardCopies - 1))
                .build();

            builder.put(indexMetadata, false);
            routingTableBuilder.addAsNew(indexMetadata);
            shards -= indexMetadata.getNumberOfShards() * (indexMetadata.getNumberOfReplicas() + 1);
        }

        return ClusterState.builder(state).metadata(builder).routingTable(routingTableBuilder.build()).build();
    }

    private static ClusterState addDataNodes(String tier, String prefix, ClusterState state, int nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(state.nodes());
        IntStream.range(0, nodes).mapToObj(i -> newDataNode(tier, prefix + "_" + i)).forEach(builder::add);
        return ClusterState.builder(state).nodes(builder).build();
    }

    private static DiscoveryNode newDataNode(String tier, String nodeName) {
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNode.getRoleFromRoleName(tier)),
            Version.CURRENT
        );
    }

    private static String randomNodeId(RoutingNodes routingNodes, String tier) {
        return randomFrom(
            ReactiveStorageDeciderService.nodesInTier(routingNodes, n -> n.getName().startsWith(tier)).collect(Collectors.toSet())
        ).nodeId();
    }

    private static Set<ShardId> shardIds(Iterable<ShardRouting> candidateShards) {
        return StreamSupport.stream(candidateShards.spliterator(), false).map(ShardRouting::shardId).collect(Collectors.toSet());
    }
}
