/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReactiveStorageDecider implements AutoscalingDecider {
    private static final Logger logger = LogManager.getLogger(ReactiveStorageDecider.class);
    public static final String NAME = "reactive_storage";

    private static final ConstructingObjectParser<ReactiveStorageDecider, Void> PARSER;
    private static final ParseField TIER_ATTRIBUTE_FIELD = new ParseField("tier_attribute");
    private static final ParseField TIER_FIELD = new ParseField("tier");

    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> new ReactiveStorageDecider((String) a[0], (String) a[1]));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIER_ATTRIBUTE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIER_FIELD);
    }
    private final String tierAttribute;
    private final String tier;

    public ReactiveStorageDecider(String tierAttribute, String tier) {
        if ((tier == null) != (tierAttribute == null)) {
            throw new IllegalArgumentException("must specify both [tier_attribute] [" + tierAttribute + "] and [tier] [" + tier + "]");
        }
        this.tierAttribute = tierAttribute;
        this.tier = tier;
    }

    public ReactiveStorageDecider(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
    }

    public static ReactiveStorageDecider parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tierAttribute);
        out.writeOptionalString(tier);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (tierAttribute != null) {
                builder.field(TIER_ATTRIBUTE_FIELD.getPreferredName(), tierAttribute);
                builder.field(TIER_FIELD.getPreferredName(), tier);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public AutoscalingDecision scale(AutoscalingDeciderContext context) {
        ClusterState state = simulateAllocationOfState(context.state(), context);
        Predicate<IndexMetadata> indexTierPredicate = tierAttribute == null
            ? i -> true
            // we check the specific attribute to not match indices with no tier spec.
            : i -> tier.equals(i.getSettings().get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + tierAttribute));
        Predicate<DiscoveryNode> nodeTierPredicate = tierAttribute == null
            ? n -> true
            : n -> tier.equals(n.getAttributes().get(tierAttribute));

        if (diskDeciderPreventsAllocation(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.SCALE_UP, "not enough storage available for unassigned shards");
        } else if (diskDeciderPreventsMove(state, context, indexTierPredicate, nodeTierPredicate)) {
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.SCALE_UP, "not enough storage available for moving shards");
        } else {
            return new AutoscalingDecision(NAME, AutoscalingDecisionType.NO_SCALE, null);
        }
    }

    private boolean diskDeciderPreventsAllocation(ClusterState state, AutoscalingDeciderContext context, Predicate<IndexMetadata> indexTierPredicate, Predicate<DiscoveryNode> nodeTierPredicate) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(context.allocationDeciders(), routingNodes, state, context.info(),
            System.nanoTime());
        Metadata metadata = state.metadata();
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .filter(u -> indexTierPredicate.test(metadata.getIndexSafe(u.index())))
            .anyMatch(shard -> diskDeciderPreventsAllocationOfShard(shard, allocation, context, nodeTierPredicate));
    }

    private boolean diskDeciderPreventsMove(ClusterState state, AutoscalingDeciderContext context, Predicate<IndexMetadata> tierPredicate, Predicate<DiscoveryNode> nodeTierPredicate) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(context.allocationDeciders(), routingNodes, state, context.info(),
            System.nanoTime());
        Metadata metadata = state.metadata();
        return state.getRoutingNodes()
            .shards(s -> true)
            .stream()
            .filter(shard -> tierPredicate.test(metadata.getIndexSafe(shard.index())))
            .filter(shard -> context.allocationDeciders().canRemain(shard, routingNodes.node(shard.currentNodeId()), allocation) == Decision.NO)
            .filter(shard -> canAllocate(shard, allocation, context, nodeTierPredicate) == false)
            .anyMatch(shard -> diskDeciderPreventsAllocationOfShard(shard, allocation, context, nodeTierPredicate));
    }

    private boolean canAllocate(ShardRouting shard, RoutingAllocation allocation, AutoscalingDeciderContext context, Predicate<DiscoveryNode> nodeTierPredicate) {
        return nodesInTier(allocation, nodeTierPredicate)
            .anyMatch(node -> context.allocationDeciders().canAllocate(shard, node, allocation) != Decision.NO);
    }

    /**
     * Check that disk decider is only decider for a node preventing allocation of the shard.
     * @return true iff a node exists in the tier where only disk decider prevents allocation
     */
    private boolean diskDeciderPreventsAllocationOfShard(ShardRouting shard, RoutingAllocation allocation,
                                                         AutoscalingDeciderContext context, Predicate<DiscoveryNode> nodeTierPredicate) {
        allocation.debugDecision(true);
        try {
            return nodesInTier(allocation, nodeTierPredicate)
                .map(node -> context.allocationDeciders().canAllocate(shard, node, allocation))
                .anyMatch(decision ->
                    decision.getDecisions().size() == 1 && decision.getDecisions().get(0).label().equals(DiskThresholdDecider.NAME));
        } finally {
            allocation.debugDecision(false);
        }
    }

    private Stream<RoutingNode> nodesInTier(RoutingAllocation allocation, Predicate<DiscoveryNode> nodeTierPredicate) {
        Predicate<RoutingNode> routingNodePredicate = rn -> nodeTierPredicate.test(rn.node());
        return StreamSupport.stream(allocation.routingNodes().spliterator(), false).filter(routingNodePredicate);
    }

    private ClusterState simulateAllocationOfState(ClusterState state, AutoscalingDeciderContext context) {
        state = allocate(state, context, a -> {});
        state = allocate(state, context, this::startShards);
        return state;
    }


    private ClusterState allocate(ClusterState state, AutoscalingDeciderContext context,
                                  Consumer<RoutingAllocation> allocationManipulator) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(context.allocationDeciders(), routingNodes, state, context.info(),
            System.nanoTime());

        allocationManipulator.accept(allocation);
        context.shardsAllocator().allocate(allocation);
        return updateClusterState(state, allocation);
    }

    private ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState).routingTable(newRoutingTable).metadata(newMetadata);

        return newStateBuilder.build();
    }

    private void startShards(RoutingAllocation allocation) {
        // simulate that all shards are started so replicas can recover and replicas to get to green.
        // also starts initializing relocated shards, which removes the relocation source too.
        allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).forEach(s -> {
            logger.info("Starting [{}]", s);
            allocation.routingNodes().startShard(logger, s, allocation.changes());
        });
    }
}
