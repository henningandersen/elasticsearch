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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
    public static final DiskThresholdSettings DISK_THRESHOLD_SETTINGS = new DiskThresholdSettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    private ClusterState state;
    private int numHotShards;
    private final int hotNodes = randomIntBetween(1, 8);
    private final int warmNodes = randomIntBetween(1, 3);
    // these are the shards that the decider tests work on
    private Set<ShardId> subjectShards;
    // say NO with disk label for subject shards
    private final AllocationDecider mockCanAllocateDiskDecider = new AllocationDecider() {
        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) {
                return allocation.decision(Decision.NO, DiskThresholdDecider.NAME, "test");
            }
            return super.canAllocate(shardRouting, node, allocation);
        }
    };
    // say NO with disk label for subject shards
    private final AllocationDecider mockCanRemainDiskDecider = new AllocationDecider() {
        @Override
        public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) return allocation.decision(
                Decision.NO,
                DiskThresholdDecider.NAME,
                "test"
            );
            return super.canRemain(shardRouting, node, allocation);
        }
    };

    @Before
    public void setup() {
        DiscoveryNode.setAdditionalRoles(
            Set.of(DataTier.DATA_CONTENT_NODE_ROLE, DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE, DataTier.DATA_COLD_NODE_ROLE)
        );
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = addRandomIndices(hotNodes, hotNodes, state);
        this.numHotShards = StreamSupport.stream(state.metadata().indices().values().spliterator(), false)
            .mapToInt(index -> index.value.getTotalNumberOfShards())
            .sum();
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
            long numPrevents = numAllocatableSubjectShards();
            assert round != 0 || numPrevents > 0;

            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, numPrevents, mockCanAllocateDiskDecider);
            verify(
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                0,
                mockCanAllocateDiskDecider,
                CAN_ALLOCATE_NO_DECIDER
            );
            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, 0);
            if (numPrevents > 0) {
                verifyScale(numPrevents, "not enough storage available, needs " + numPrevents, mockCanAllocateDiskDecider);
            } else {
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
            new ReactiveStorageDeciderService.AllocationState(
                createContext(mockCanAllocateDiskDecider),
                new DiskThresholdSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
            ).state(),
            sameInstance(state)
        );
    }

    public void testStoragePreventsMove() {
        // this test does things backwards to avoid adding too much additional setup. It moves shards to warm nodes and then checks that
        // the reactive decider can calculate the storage necessary to move them back to hot nodes.

        // allocate all primary shards
        allocate();

        // pick set of shards to force on to warm nodes.
        Set<ShardId> warmShards = Sets.union(new HashSet<>(randomSubsetOf(shardIds(state.getRoutingNodes().unassigned()))), subjectShards);

        // start (to be) warm shards. Only use primary shards for simplicity.
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

        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            subjectShards.size(),
            mockCanAllocateDiskDecider
        );
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            mockCanAllocateDiskDecider,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0);

        verifyScale(subjectShards.size(), "not enough storage available, needs " + subjectShards.size(), mockCanAllocateDiskDecider);
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

        // the remain check only assumes the smallest shard need to move off. More detailed testing of AllocationState.unmovableSize in
        // {@link ReactiveStorageDeciderServiceTests#testUnmovableSize}
        long nodes = state.routingTable()
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(s -> subjectShards.contains(s.shardId()))
            .map(ShardRouting::currentNodeId)
            .distinct()
            .count();

        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            nodes,
            mockCanRemainDiskDecider,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            mockCanRemainDiskDecider,
            CAN_REMAIN_NO_DECIDER,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0);

        // only consider it once (move case) if both cannot remain and cannot allocate.
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            nodes,
            mockCanAllocateDiskDecider,
            mockCanRemainDiskDecider
        );

        verifyScale(nodes, "not enough storage available, needs " + nodes, mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", mockCanRemainDiskDecider, CAN_REMAIN_NO_DECIDER, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok");
    }

    public interface MissingEstimator {
        long invoke(ReactiveStorageDeciderService.AllocationState state);
    }

    public void verify(MissingEstimator subject, long expected, AllocationDecider... allocationDeciders) {
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            createContext(DataTier.DATA_HOT_NODE_ROLE, allocationDeciders),
            DISK_THRESHOLD_SETTINGS
        );
        assertThat(subject.invoke(allocationState), equalTo(expected));
    }

    public void verifyScale(long expectedDifference, String reason, AllocationDecider... allocationDeciders) {
        verifyScale(state, expectedDifference, reason, allocationDeciders);
    }

    public static void verifyScale(ClusterState state, long expectedDifference, String reason, AllocationDecider... allocationDeciders) {
        ReactiveStorageDeciderService decider = new ReactiveStorageDeciderService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TestAutoscalingDeciderContext context = createContext(state, Set.of(DataTier.DATA_HOT_NODE_ROLE), allocationDeciders);
        AutoscalingDeciderResult result = decider.scale(Settings.EMPTY, context);
        if (context.currentCapacity != null) {
            assertThat(
                result.requiredCapacity().tier().storage().getBytes() - context.currentCapacity.tier().storage().getBytes(),
                equalTo(expectedDifference)
            );
            assertThat(result.reason().summary(), equalTo(reason));
        } else {
            assertThat(result.requiredCapacity(), is(nullValue()));
            assertThat(result.reason().summary(), equalTo("current capacity not available"));
        }
    }

    private long numAllocatableSubjectShards() {
        AllocationDeciders deciders = createAllocationDeciders();
        RoutingAllocation allocation = createRoutingAllocation(state, createAllocationDeciders());
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .filter(shard -> subjectShards.contains(shard.shardId()))
            .filter(
                shard -> StreamSupport.stream(allocation.routingNodes().spliterator(), false)
                    .anyMatch(node -> deciders.canAllocate(shard, node, allocation) != Decision.NO)
            )
            .count();
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
        state = ReactiveStorageDeciderServiceTests.updateClusterState(state, allocation);
    }

    private void allocate() {
        withRoutingAllocation(SHARDS_ALLOCATOR::allocate);
    }

    private int startRandomShards() {
        return startRandomShards(createAllocationDeciders());
    }

    private int startRandomShards(AllocationDeciders allocationDeciders) {
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

        state = ReactiveStorageDeciderServiceTests.updateClusterState(state, allocation);

        return shards.size();

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
        return new TestAutoscalingDeciderContext(state, roles, createAllocationDeciders(allocationDeciders), randomCurrentCapacity());
    }

    private static AutoscalingCapacity randomCurrentCapacity() {
        if (randomInt(4) > 0) {
            // we only rely on storage.
            boolean includeMemory = randomBoolean();
            return AutoscalingCapacity.builder()
                .total(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null)
                .node(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null)
                .build();
        } else {
            return null;
        }
    }

    private static class TestAutoscalingDeciderContext implements AutoscalingDeciderContext {
        private final ClusterState state;
        private final AllocationDeciders allocationDeciders;
        private final AutoscalingCapacity currentCapacity;
        private final Set<DiscoveryNode> nodes;
        private final Set<DiscoveryNodeRole> roles;
        private ClusterInfo info;

        private TestAutoscalingDeciderContext(
            ClusterState state,
            Set<DiscoveryNodeRole> roles,
            AllocationDeciders allocationDeciders,
            AutoscalingCapacity currentCapacity
        ) {
            this.state = state;
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
        public AllocationDeciders allocationDeciders() {
            return allocationDeciders;
        }
    }

    private static ClusterInfo createClusterInfo(ClusterState state) {
        // we make a simple setup to detect the right decisions are made. The unmovable calculation is tested in more detail elsewhere.
        // the diskusage is set such that the disk threshold decider never rejects an allocation.
        Map<String, DiskUsage> diskUsages = StreamSupport.stream(state.nodes().spliterator(), false)
            .collect(Collectors.toMap(DiscoveryNode::getId, node -> new DiskUsage(node.getId(), null, "the_path", 1000, 1000)));
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

            @Override
            public long getShardSize(ShardRouting shardRouting, long defaultValue) {
                return 1L;
            }

            @Override
            public Long getShardSize(ShardRouting shardRouting) {
                return 1L;
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