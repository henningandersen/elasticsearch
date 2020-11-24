/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests the primitive methods in {@link ReactiveStorageDeciderService}. Tests of higher level methods are in
 * {@link ReactiveStorageDeciderDecisionTests}
 */
public class ReactiveStorageDeciderServiceTests extends ESTestCase {
    private static final List<String> SOME_ALLOCATION_DECIDERS = Arrays.asList(
        SameShardAllocationDecider.NAME,
        AwarenessAllocationDecider.NAME,
        EnableAllocationDecider.NAME
    );

    public void testIsDiskOnlyDecision() {
        Decision.Multi decision = new Decision.Multi();
        if (randomBoolean()) {
            decision.add(randomFrom(Decision.YES, Decision.ALWAYS, Decision.THROTTLE));
        }
        decision.add(new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "test"));
        randomSubsetOf(SOME_ALLOCATION_DECIDERS).stream()
            .map(
                label -> new Decision.Single(
                    randomValueOtherThan(Decision.Type.NO, () -> randomFrom(Decision.Type.values())),
                    label,
                    "test " + label
                )
            )
            .forEach(decision::add);

        assertThat(ReactiveStorageDeciderService.isDiskOnlyNoDecision(decision), is(true));
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

        assertThat(ReactiveStorageDeciderService.isDiskOnlyNoDecision(decision), is(false));
    }

    public void testNodesInTier() {
        int hotNodes = randomIntBetween(0, 8);
        int warmNodes = randomIntBetween(0, 8);
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = addDataNodes("hot", "hot", state, hotNodes);
        Set<DiscoveryNode> expectedHotNodes = StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toSet());
        state = addDataNodes("warm", "warm", state, warmNodes);

        Set<DiscoveryNode> hotTier = ReactiveStorageDeciderService.nodesInTier(state.getRoutingNodes(), n -> n.getName().startsWith("hot"))
            .map(RoutingNode::node)
            .collect(Collectors.toSet());
        assertThat(hotTier, equalTo(expectedHotNodes));
    }

    public void testUnmovableSize() {
        // disk is 100 kb. Default is 90 percent.
        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            String tenKb = ByteSizeValue.ofKb(10).toString();
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), tenKb);
            // also set these to pass validation
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), tenKb);
            settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), tenKb);
        }
        Settings settings = settingsBuilder.build();
        DiskThresholdSettings thresholdSettings = new DiskThresholdSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        String nodeId = randomAlphaOfLength(5);

        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(10)
            .build();
        metaBuilder.put(indexMetadata, true);
        stateBuilder.metadata(metaBuilder);
        ClusterState clusterState = stateBuilder.build();

        Set<ShardRouting> shards = IntStream.range(0, between(1, 10))
            .mapToObj(
                i -> TestShardRouting.newShardRouting(
                    new ShardId(indexMetadata.getIndex(), randomInt(10)),
                    nodeId,
                    randomBoolean(),
                    ShardRoutingState.STARTED
                )
            )
            .collect(Collectors.toSet());

        long minShardSize = randomLongBetween(1, 10);

        ImmutableOpenMap.Builder<String, DiskUsage> diskUsagesBuilder = ImmutableOpenMap.builder();
        diskUsagesBuilder.put(nodeId, new DiskUsage(nodeId, null, null, ByteSizeUnit.KB.toBytes(100), ByteSizeUnit.KB.toBytes(5)));
        ImmutableOpenMap<String, DiskUsage> diskUsages = diskUsagesBuilder.build();
        ImmutableOpenMap.Builder<String, Long> shardSizeBuilder = ImmutableOpenMap.builder();
        ShardRouting missingShard = randomBoolean() ? randomFrom(shards) : null;
        Collection<ShardRouting> shardsWithSizes = shards.stream().filter(s -> s != missingShard).collect(Collectors.toSet());
        for (ShardRouting shard : shardsWithSizes) {
            shardSizeBuilder.put(
                ClusterInfo.shardIdentifierFromRouting(shard),
                ByteSizeUnit.KB.toBytes(randomLongBetween(minShardSize, 100))
            );
        }
        if (shardsWithSizes.isEmpty() == false) {
            shardSizeBuilder.put(
                ClusterInfo.shardIdentifierFromRouting(randomFrom(shardsWithSizes)),
                ByteSizeUnit.KB.toBytes(minShardSize)
            );
        }
        ClusterInfo info = new ClusterInfo(diskUsages, diskUsages, shardSizeBuilder.build(), null, null);

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            clusterState,
            null,
            thresholdSettings,
            info,
            null,
            null
        );

        long result = allocationState.unmovableSize(nodeId, shards);
        if (missingShard != null && (missingShard.primary() || info.getShardSize(missingShard.moveActiveReplicaToPrimary()) == null)
            || minShardSize < 5) {
            // the diff between used and high watermark is 5 KB.
            assertThat(result, equalTo(ByteSizeUnit.KB.toBytes(5)));
        } else {
            assertThat(result, equalTo(ByteSizeUnit.KB.toBytes(minShardSize)));
        }
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
            Map.of("tier", tier),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
