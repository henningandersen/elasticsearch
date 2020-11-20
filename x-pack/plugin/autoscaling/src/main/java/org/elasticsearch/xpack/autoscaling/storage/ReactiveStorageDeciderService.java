/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING;

public class ReactiveStorageDeciderService implements AutoscalingDeciderService<ReactiveStorageDeciderConfiguration> {
    public static final String NAME = "reactive_storage";

    private static final Logger logger = LogManager.getLogger(ReactiveStorageDeciderService.class);

    private static final Predicate<DiscoveryNodeRole> DATA_ROLE_PREDICATE = DiscoveryNode.getPossibleRoles()
        .stream()
        .filter(DiscoveryNodeRole::canContainData)
        .collect(Collectors.toSet())::contains;

    public ReactiveStorageDeciderService() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(ReactiveStorageDeciderConfiguration decider, AutoscalingDeciderContext context) {
        AutoscalingCapacity autoscalingCapacity = context.currentCapacity();
        if (autoscalingCapacity == null || autoscalingCapacity.tier().storage() == null) {
            return new AutoscalingDeciderResult(null, new ReactiveReason("current capacity not available"));
        }

        AllocationState allocationState = new AllocationState(context);
        allocationState = allocationState.simulateStartAndAllocate();

        AutoscalingCapacity plusOne = AutoscalingCapacity.builder()
            .total(autoscalingCapacity.tier().storage().getBytes() + 1, null)
            .build();
        if (allocationState.storagePreventsAllocation()) {
            return new AutoscalingDeciderResult(plusOne, new ReactiveReason("not enough storage available for unassigned shards"));
        } else if (allocationState.storagePreventsRemainOrMove()) {
            return new AutoscalingDeciderResult(plusOne, new ReactiveReason("not enough storage available for assigned shards"));
        } else {
            // the message here is tricky, since storage might not be OK, but in that case, increasing storage alone would not help since
            // other deciders prevents allocation/moving shards.
            AutoscalingCapacity ok = AutoscalingCapacity.builder().total(autoscalingCapacity.tier().storage(), null).build();
            return new AutoscalingDeciderResult(ok, new ReactiveReason("storage ok"));
        }
    }

    static boolean isDiskOnlyNoDecision(Decision decision) {
        // we consider throttling==yes, throttling should be temporary.
        List<Decision> nos = decision.getDecisions()
            .stream()
            .filter(single -> single.type() == Decision.Type.NO)
            .collect(Collectors.toList());
        return nos.size() == 1 && DiskThresholdDecider.NAME.equals(nos.get(0).label());

    }

    static Stream<RoutingNode> nodesInTier(RoutingNodes routingNodes, Predicate<DiscoveryNode> nodeTierPredicate) {
        Predicate<RoutingNode> routingNodePredicate = rn -> nodeTierPredicate.test(rn.node());
        return StreamSupport.stream(routingNodes.spliterator(), false).filter(routingNodePredicate);
    }

    static ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        assert allocation.metadata() == oldState.metadata();
        if (allocation.routingNodesChanged() == false) {
            return oldState;
        }
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        return ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata).build();
    }

    static Predicate<IndexMetadata> indexTierPredicate(AutoscalingDeciderContext context) {
        return imd -> belongsToTier(
            imd,
            context.roles().stream().filter(DATA_ROLE_PREDICATE).map(DiscoveryNodeRole::roleName).collect(Collectors.toSet())::contains
        );
    }

    private enum OpType {
        AND,
        OR
    }

    private static boolean belongsToTier(IndexMetadata imd, Predicate<String> dataRoles) {
        // some logic replication to DataTierAllocationDecider here, since we do not necessarily have a node.
        Settings indexSettings = imd.getSettings();
        String indexRequire = INDEX_ROUTING_REQUIRE_SETTING.get(indexSettings);
        String indexInclude = INDEX_ROUTING_INCLUDE_SETTING.get(indexSettings);
        String indexExclude = INDEX_ROUTING_EXCLUDE_SETTING.get(indexSettings);

        if (Strings.hasText(indexRequire)) {
            if (allocationAllowed(OpType.AND, indexRequire, dataRoles) == false) {
                return false;
            }
        }
        if (Strings.hasText(indexInclude)) {
            if (allocationAllowed(OpType.OR, indexInclude, dataRoles) == false) {
                return false;
            }
        }
        if (Strings.hasText(indexExclude)) {
            if (allocationAllowed(OpType.OR, indexExclude, dataRoles)) {
                return false;
            }
        }

        String tierPreference = INDEX_ROUTING_PREFER_SETTING.get(indexSettings);
        // we only take first preference to ensure we spin up new tiers as required.
        if (Strings.hasText(tierPreference)) {
            String tier = Strings.tokenizeToStringArray(tierPreference, ",")[0];
            if (allocationAllowed(OpType.AND, tier, dataRoles) == false) {
                return false;
            }
        }

        return true;
    }

    private static boolean allocationAllowed(OpType opType, String tierSetting, Predicate<String> dataRoles) {
        // minor logic replication to DataTierAllcationDecider here, since we do not necessarily have a node.
        String[] values = Strings.tokenizeToStringArray(tierSetting, ",");
        for (String value : values) {
            // generic "data" roles are considered to have all tiers
            if (dataRoles.test(DiscoveryNodeRole.DATA_ROLE.roleName()) || dataRoles.test(value)) {
                if (opType == OpType.OR) {
                    return true;
                }
            } else {
                if (opType == OpType.AND) {
                    return false;
                }
            }
        }
        return opType != OpType.OR;
    }

    public static class AllocationState {
        private final ClusterState state;
        private final AllocationDeciders allocationDeciders;
        private final ShardsAllocator shardsAllocator;
        private final ClusterInfo info;
        private final SnapshotShardSizeInfo shardSizeInfo;
        private final Predicate<IndexMetadata> indexTierPredicate;
        private final Predicate<DiscoveryNode> nodeTierPredicate;

        public AllocationState(AutoscalingDeciderContext context) {
            this(context.state(), context.allocationDeciders(), context.shardsAllocator(),
                context.info(), context.snapshotShardSizeInfo(), indexTierPredicate(context), context.nodes()::contains);
        }
        public AllocationState(ClusterState state, AllocationDeciders allocationDeciders, ShardsAllocator shardsAllocator, ClusterInfo info, SnapshotShardSizeInfo shardSizeInfo, Predicate<IndexMetadata> indexTierPredicate, Predicate<DiscoveryNode> nodeTierPredicate) {
            this.state = state;
            this.allocationDeciders = allocationDeciders;
            this.shardsAllocator = shardsAllocator;
            this.info = info;
            this.shardSizeInfo = shardSizeInfo;
            this.indexTierPredicate = indexTierPredicate;
            this.nodeTierPredicate = nodeTierPredicate;
        }

        public AllocationState simulateStartAndAllocate() {
            // for ClusterInfo, we optimistically assume that all recoveries and relocations that were ongoing had copied all data over.
            // this ensures we do not scale up unnecessarily and at the same time, we expect that the recovery/relocation will then
            // complete before the next autoscaling poll.
            // todo: we should refine how we expose reservations, since this would allow us to more precisely adjust for this.
            // also, getting an uncertainty estimate from the node would be beneficial. Ideally, we should collect free bytes and
            // shard sizes in one call, lowering the uncertainty and potentially allowing some level of uncertainty estimate.\

            // for multi data paths, the simulation only ensures relocations are done and optimistically frees the space on both
            // least and most available paths. This approximation means we may not trigger autoscaling as quickly on multi data path
            // setups, but properly simulating shards moving around in a multi data path cluster is difficult.
            ImmutableOpenMap.Builder<String, DiskUsage> mostAvailable = ImmutableOpenMap.builder(info.getNodeMostAvailableDiskUsages());
            ImmutableOpenMap.Builder<String, DiskUsage> leastAvailable = ImmutableOpenMap.builder(info.getNodeLeastAvailableDiskUsages());

            ClusterState state = this.state;
            while (true) {
                RoutingNodes routingNodes = new RoutingNodes(state, false);
                RoutingAllocation allocation = new RoutingAllocation(
                    allocationDeciders,
                    routingNodes,
                    state,
                    info,
                    shardSizeInfo,
                    System.nanoTime()
                );

                List<ShardRouting> shards = allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING);
                // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
                shards.stream()
                    .filter(Predicate.not(ShardRouting::primary))
                    .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
                shards.stream()
                    .filter(ShardRouting::primary)
                    .forEach(s -> { allocation.routingNodes().startShard(logger, s, allocation.changes()); });
                shardsAllocator.allocate(allocation);
                ClusterState nextState = updateClusterState(state, allocation);

                if (nextState == state) {
                    // todo: adjust infos
                    return new AllocationState(nextState, allocationDeciders, shardsAllocator, info, shardSizeInfo, indexTierPredicate, nodeTierPredicate);
                } else {
                    state = nextState;
                }
            }
        }

        public boolean storagePreventsAllocation() {
            RoutingNodes routingNodes = new RoutingNodes(state, false);
            RoutingAllocation allocation = new RoutingAllocation(
                allocationDeciders,
                routingNodes,
                state,
                info,
                shardSizeInfo,
                System.nanoTime()
            );
            Metadata metadata = state.metadata();
            return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
                .filter(u -> indexTierPredicate.test(metadata.getIndexSafe(u.index())))
                .anyMatch(shard -> cannotAllocateDueToStorage(shard, allocation));
        }


        public boolean storagePreventsRemainOrMove() {
            RoutingNodes routingNodes = new RoutingNodes(state, false);
            RoutingAllocation allocation = new RoutingAllocation(
                allocationDeciders,
                routingNodes,
                state,
                info,
                shardSizeInfo,
                System.nanoTime()
            );
            Metadata metadata = state.metadata();
            return state.getRoutingNodes()
                .shardsWithState(ShardRoutingState.STARTED)
                .stream()
                .filter(shard -> indexTierPredicate.test(metadata.getIndexSafe(shard.index())))
                .filter(
                    shard -> allocationDeciders.canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation) == Decision.NO
                )
                .filter(shard -> canAllocate(shard, allocation) == false)
                .anyMatch(
                    shard -> cannotAllocateDueToStorage(shard, allocation)
                        || cannotRemainDueToStorage(shard, allocation)
                );
        }

        /**
         * Check that disk decider is only decider for a node preventing allocation of the shard.
         * @return true if and only if a node exists in the tier where only disk decider prevents allocation
         */
        private boolean cannotAllocateDueToStorage(
            ShardRouting shard,
            RoutingAllocation allocation) {
            assert allocation.debugDecision() == false;
            allocation.debugDecision(true);
            try {
                return nodesInTier(allocation.routingNodes(), nodeTierPredicate).map(
                    node -> allocationDeciders.canAllocate(shard, node, allocation)
                ).anyMatch(ReactiveStorageDeciderService::isDiskOnlyNoDecision);
            } finally {
                allocation.debugDecision(false);
            }
        }

        /**
         * Check that the disk decider is only decider that says NO to let shard remain on current node.
         * @return true if and only if disk decider is only decider that says NO to canRemain.
         */
        private boolean cannotRemainDueToStorage(ShardRouting shard, RoutingAllocation allocation) {
            assert allocation.debugDecision() == false;
            allocation.debugDecision(true);
            try {
                return isDiskOnlyNoDecision(
                    allocationDeciders.canRemain(shard, allocation.routingNodes().node(shard.currentNodeId()), allocation)
                );
            } finally {
                allocation.debugDecision(false);
            }
        }

        private boolean canAllocate(ShardRouting shard, RoutingAllocation allocation) {
            return nodesInTier(allocation.routingNodes(), nodeTierPredicate).anyMatch(
                node -> allocationDeciders.canAllocate(shard, node, allocation) != Decision.NO
            );
        }


        public ClusterState state() {
            return state;
        }
    }

    public static class Uncertainty {
        private long maxError;
        private String message;

        public Uncertainty(long maxError, String message) {
            assert maxError != 0;
            this.maxError = maxError;
            this.message = message;
        }

        /**
         * @return max error of this uncertainty. Positive value means that we might be shooting under the target with this amount.
         * Negative means that we might be shooting under the target. Long.MAX/MIN_VALUE when unbounded in either direction.
         */
        public long maxError() {
            return maxError;
        }

        public String message() {
            return message;
        }
    }
    public static class AdjustClusterInfoObserver implements RoutingChangesObserver {

        private final ImmutableOpenMap.Builder<String, DiskUsage> diskUsages1;
        private final ImmutableOpenMap.Builder<String, DiskUsage> diskUsages2;
        private final Function<ShardRouting, Long> sizer;
        private final Function<ShardRouting, String> pathFunction;
        private ClusterInfo info;
        private SnapshotShardSizeInfo snapshotShardSizeInfo;
        private Metadata metadata;
        private RoutingTable routingTable;
        private List<Uncertainty> uncertainties = new ArrayList<>();



        public AdjustClusterInfoObserver(ImmutableOpenMap.Builder<String, DiskUsage> diskUsage) {
            this.diskUsages1 = diskUsage;
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            free(initializedShard);
        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {
            free(startedShard);
            alloc(targetRelocatingShard);
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {
        }

        @Override
        public void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {
        }

        @Override
        public void replicaPromoted(ShardRouting replicaShard) {
        }

        @Override
        public void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica) {
        }

        private void free(ShardRouting shard) {
            DiskUsage diskUsage = this.diskUsages1.get(shard.currentNodeId());
            DiskUsage diskUsage2 = this.diskUsages2.get(shard.currentNodeId());
            long expectedShardSize = getExpectedShardSize(shard);
            if (diskUsage != null) {
                String shardPath = pathFunction.apply(shard);
                if (shardPath == null || diskUsage.getPath().equals(shardPath)) {
                    if (expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
                        DiskUsage newDiskUsage = free(diskUsage, expectedShardSize);
                        this.diskUsages1.put(shard.currentNodeId(), newDiskUsage);
                        if (diskUsage2 == diskUsage) {
                            this.diskUsages2.put(shard.currentNodeId(), newDiskUsage);
                        } else {
                            this.diskUsages2.put(shard.currentNodeId(), free(diskUsage2, expectedShardSize));
                            addUncertainty(-expectedShardSize, "multiple data paths on node [{}]", shard.currentNodeId()))
                        }
                    } else {
                        addUncertainty(diskUsage.getUsedBytes(), "no shard size for [{}]", shard);
                    }
                } else {
                    long uncertainty = expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? expectedShardSize :
                        (diskUsage2.getPath().equals(shardPath) ? diskUsage2.getUsedBytes() : Long.MAX_VALUE);
                    addUncertainty(uncertainty, "multiple data paths on node [{}]", shard.currentNodeId()))
                }
            } else {
                long uncertainty = expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? expectedShardSize : Long.MAX_VALUE;
                addUncertainty(uncertainty, "no disk usage for node [{}]", shard.currentNodeId());
            }
        }

        private void alloc(ShardRouting shard) {
            DiskUsage diskUsage = this.diskUsages1.get(shard.currentNodeId());
            DiskUsage diskUsage2 = this.diskUsages2.get(shard.currentNodeId());
            long expectedShardSize = getExpectedShardSize(shard);
            if (diskUsage != null) {
                String shardPath = pathFunction.apply(shard);
                if (shardPath == null || diskUsage.getPath().equals(shardPath)) {
                    if (expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
                        DiskUsage newDiskUsage = alloc(diskUsage, expectedShardSize);
                        this.diskUsages1.put(shard.currentNodeId(), newDiskUsage);
                        if (diskUsage2 == diskUsage) {
                            this.diskUsages2.put(shard.currentNodeId(), newDiskUsage);
                        } else {
                            this.diskUsages2.put(shard.currentNodeId(), alloc(diskUsage2, expectedShardSize));
                            addUncertainty(-expectedShardSize, "multiple data paths on node [{}]", shard.currentNodeId()))
                        }
                    } else {
                        addUncertainty(diskUsage.getUsedBytes(), "no shard size for [{}]", shard);
                    }
                } else {
                    long uncertainty = expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? expectedShardSize :
                        (diskUsage2.getPath().equals(shardPath) ? diskUsage2.getUsedBytes() : Long.MAX_VALUE);
                    addUncertainty(uncertainty, "multiple data paths on node [{}]", shard.currentNodeId()))
                }
            } else {
                long uncertainty = expectedShardSize != ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? expectedShardSize : Long.MAX_VALUE;
                addUncertainty(uncertainty, "no disk usage for node [{}]", shard.currentNodeId());
            }
        }

        private void addUncertainty(long maxError, String message, Object... messageParams) {
            uncertainties.add(new Uncertainty(maxError, String.format(Locale.ROOT, message, messageParams)));
        }

        private long getExpectedShardSize(ShardRouting shard) {
            long expectedShardSize = DiskThresholdDecider.getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, info,
                snapshotShardSizeInfo, metadata, routingTable);
            if (expectedShardSize == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
                if (shard.primary() == false) {
                    expectedShardSize = info.getShardSize(shard.moveActiveReplicaToPrimary(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                }
                // todo: we should ideally not have the level of uncertainty we have here.
            }
            return expectedShardSize == 0L ? ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE : expectedShardSize;
        }

        private DiskUsage free(DiskUsage diskUsage, long expectedShardSize) {
            assert expectedShardSize >= 0;
            return new DiskUsage(diskUsage.getNodeId(), diskUsage.getNodeName(), diskUsage.getPath(), diskUsage.getTotalBytes(),
                Math.addExact(diskUsage.getFreeBytes(), expectedShardSize));
        }
    }

    public static class ReactiveReason implements AutoscalingDeciderResult.Reason {
        private final String reason;

        public ReactiveReason(String reason) {
            this.reason = reason;
        }

        public ReactiveReason(StreamInput in) throws IOException {
            this.reason = in.readString();
        }

        @Override
        public String summary() {
            return reason;
        }

        @Override
        public String getWriteableName() {
            return ReactiveStorageDeciderService.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.endObject();
            return builder;
        }
    }
}
