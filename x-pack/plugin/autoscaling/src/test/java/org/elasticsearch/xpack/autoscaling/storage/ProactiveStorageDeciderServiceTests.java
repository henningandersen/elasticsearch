/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProactiveStorageDeciderServiceTests extends AutoscalingTestCase {
    public void testScale() {

    }

    public void testForecastNoDates() {
        ClusterState originalState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(Tuple.tuple("test", between(1, 10))), List.of());
        ClusterState.Builder stateBuilder = ClusterState.builder(originalState);
        stateBuilder.routingTable(addRouting(originalState.metadata(), RoutingTable.builder()).build());
        ClusterState state = stateBuilder.build();
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            null,
            null,
            null,
            Set.of()
        );

        assertThat(allocationState.predict(Long.MAX_VALUE, System.currentTimeMillis()), Matchers.sameInstance(allocationState));
    }

    public void testForecast() {
        ClusterState originalState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(Tuple.tuple("test", between(1, 10))), List.of());
        ClusterState.Builder stateBuilder = ClusterState.builder(originalState);
        stateBuilder.routingTable(addRouting(originalState.metadata(), RoutingTable.builder()).build());
        ReactiveStorageDeciderServiceTests.addNode(stateBuilder);
        long lastCreated = randomNonNegativeLong();
        applyCreatedDates(originalState, stateBuilder, (IndexAbstraction.DataStream) originalState.metadata().getIndicesLookup().get("test"), lastCreated, 10);
        ClusterState state = stateBuilder.build();

        ClusterInfo info = randomClusterInfo(state);

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            null,
            info,
            null,
            Set.of()
        );


        allocationState.predict()
    }

    private RoutingTable.Builder addRouting(Iterable<IndexMetadata> indices, RoutingTable.Builder builder) {
        indices.forEach(builder::addAsNew);
        return builder;
    }

    private ClusterInfo randomClusterInfo(ClusterState state) {
        Map<String, Long> collect =
            state.routingTable().allShards().stream().map(ClusterInfo::shardIdentifierFromRouting).collect(Collectors.toMap(Function.identity(), id -> randomLongBetween(0, 1000)));
        return new ClusterInfo(null, null, ImmutableOpenMap.<String, Long>builder().putAll(collect).build(), null, null);
    }

    private ClusterState.Builder applyCreatedDates(ClusterState state, ClusterState.Builder builder, IndexAbstraction.DataStream ds,
                                                 long last,
                                           long decrement) {
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
        List<IndexMetadata> indices = ds.getIndices();
        long start = last - (decrement * (indices.size() - 1));
        for (int i = 0; i < indices.size(); ++i) {
            metadataBuilder.put(IndexMetadata.builder(indices.get(i)).creationDate(start + (i * decrement)).build(), false);
        }
        return builder.metadata(metadataBuilder);
    }
}
