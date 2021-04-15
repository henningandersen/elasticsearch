/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.autoscaling.AbstractFrozenAutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.shards.FrozenShardsDeciderService;
import org.elasticsearch.xpack.autoscaling.shards.LocalStateAutoscalingAndSearchableSnapshots;
import org.elasticsearch.xpack.autoscaling.storage.FrozenStorageDeciderService;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class FrozenStorageDeciderIT extends AbstractFrozenAutoscalingIntegTestCase {

    public void testScale() {
        IndicesStatsResponse statsResponse = client().admin().indices().stats(new IndicesStatsRequest().indices(restoredIndexName)).actionGet();
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        assertThat(
            capacity().results().get("frozen").requiredCapacity().total().storage(),
            equalTo(ByteSizeValue.ofBytes((long) (statsResponse.getPrimaries().store.totalDataSetSize().getBytes() * FrozenStorageDeciderService.DEFAULT_PERCENTAGE / 100)))
        );
    }

    @Override
    protected String deciderName() {
        return FrozenStorageDeciderService.NAME;
    }

    @Override
    protected Settings.Builder addDeciderSettings(Settings.Builder builder) {
        return builder.put(FrozenStorageDeciderService.PERCENTAGE.getKey(), FrozenStorageDeciderService.DEFAULT_PERCENTAGE);
    }
}
