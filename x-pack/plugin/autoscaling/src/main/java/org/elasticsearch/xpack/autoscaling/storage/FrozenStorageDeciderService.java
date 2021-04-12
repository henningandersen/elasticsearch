/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class FrozenStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "frozen_storage";

    public static final Setting<Double> PERCENTAGE = Setting.doubleSetting("percentage", 5.0d, 0.0d);

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        Metadata metadata = context.state().metadata();
        long dataSetSize = StreamSupport.stream(metadata.spliterator(), false)
            .filter(imd -> isFrozenIndex(imd.getSettings()))
            .mapToLong(imd -> estimateSize(imd, context.info()))
            .sum();

        long storageSize = (long) (PERCENTAGE.get(configuration) * dataSetSize);
        return new AutoscalingDeciderResult(AutoscalingCapacity.builder().total(storageSize, null).build(), new FrozenReason(dataSetSize));
    }

    private long estimateSize(IndexMetadata imd, ClusterInfo info) {
        int copies = imd.getNumberOfReplicas() + 1;
        return IntStream.range(0, imd.getNumberOfShards())
            .mapToObj(s -> new ShardId(imd.getIndex(), s))
            .mapToLong(s -> info.getShardDataSetSize(s).orElse(0L))
            .map(s -> s * copies)
            .sum();
    }

    static boolean isFrozenIndex(Settings indexSettings) {
        String tierPreference = DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(indexSettings);
        String[] preferredTiers = DataTierAllocationDecider.parseTierList(tierPreference);
        if (preferredTiers.length >= 1 && preferredTiers[0].equals(DataTier.DATA_FROZEN)) {
            assert preferredTiers.length == 1 : "frozen tier preference must be frozen only";
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(PERCENTAGE);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    public static class FrozenReason implements AutoscalingDeciderResult.Reason {
        private final long totalDataSetSize;

        public FrozenReason(long totalDataSetSize) {
            this.totalDataSetSize = totalDataSetSize;
        }

        public FrozenReason(StreamInput in) throws IOException {
            this.totalDataSetSize = in.readLong();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("total_data_set_size", totalDataSetSize);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalDataSetSize);
        }

        @Override
        public String summary() {
            return "total data set size [" + totalDataSetSize + "]";
        }
    }
}
