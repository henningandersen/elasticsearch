/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FrozenStorageDeciderServiceTests extends AutoscalingTestCase {

    public void testEstimateSize() {
        int shards = between(1, 10);
        int replicas = between(0, 9);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED,
                Version.CURRENT)).numberOfShards(shards).numberOfReplicas(replicas).build();
        ImmutableOpenMap.Builder<ShardId, Long> sizesBuilder = ImmutableOpenMap.builder();
        long expected = 0;
        for (int i = 0; i < shards; ++i) {
            long size = randomLongBetween(0, Integer.MAX_VALUE);
            expected += size * (replicas + 1);
            sizesBuilder.put(new ShardId(indexMetadata.getIndex(), i), size);
        }
        ClusterInfo info = new ClusterInfo(null, null, null, sizesBuilder.build(), null, null);
        assertThat(FrozenStorageDeciderService.estimateSize(indexMetadata, info), equalTo(expected));
    }
}
