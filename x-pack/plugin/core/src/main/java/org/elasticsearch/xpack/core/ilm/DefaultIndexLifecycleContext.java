/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.util.List;

public class DefaultIndexLifecycleContext implements IndexLifecycleContext {
    private final Client client;

    public DefaultIndexLifecycleContext(Client client) {
        this.client = client;
    }

    @Override
    public void rollover(RolloverRequest request, ActionListener<RolloverResponse> listener) {
        client.admin().indices().rolloverIndex(request, listener);
    }

    @Override
    public void open(String index, ActionListener<AcknowledgedResponse> listener) {
        client.admin().indices().open(new OpenIndexRequest(index), ActionListener.map(listener, r -> r));
    }

    @Override
    public void close(String index, ActionListener<AcknowledgedResponse> listener) {
        client.admin().indices().close(new CloseIndexRequest(index), ActionListener.map(listener, r -> r));
    }

    @Override
    public void freeze(String index, ActionListener<Void> listener) {
        client.admin().indices().execute(FreezeIndexAction.INSTANCE,
            new FreezeRequest(index), ActionListener.map(listener, r -> null));
    }

    @Override
    public void delete(String index, ActionListener<Void> listener) {
        client.admin().indices().delete(new DeleteIndexRequest(index), ActionListener.map(listener, r -> null));
    }

    @Override
    public void resizeIndex(String target, String source, Settings targetSettings, ActionListener<AcknowledgedResponse> listener) {
        ResizeRequest request = new ResizeRequest(target, source);
        request.getTargetIndexRequest().settings(targetSettings);
        client.admin().indices().resizeIndex(request, ActionListener.map(listener, r -> r));
    }

    @Override
    public void forceMerge(String index, int maxNumSegments, ActionListener<Void> listener) {
        client.admin().indices().forceMerge(new ForceMergeRequest(index).maxNumSegments(maxNumSegments),
            ActionListener.map(listener, r -> null));
    }

    @Override
    public void unfollow(String followerIndex, ActionListener<AcknowledgedResponse> listener) {
        UnfollowAction.Request request =
            new UnfollowAction.Request(followerIndex);
        client.execute(UnfollowAction.INSTANCE, request, ActionListener.map(listener, r -> r));
    }

    @Override
    public void pauseFollow(String followerIndex, ActionListener<AcknowledgedResponse> listener) {
        PauseFollowAction.Request request = new PauseFollowAction.Request(followerIndex);
        client.execute(PauseFollowAction.INSTANCE, request, ActionListener.map(listener, r -> r));
    }

    @Override
    public void updateSettings(String index, Settings settings, ActionListener<Void> listener) {
        client.admin().indices().updateSettings(new UpdateSettingsRequest(settings, index), ActionListener.map(listener, r -> null));
    }

    @Override
    public void aliases(IndicesAliasesRequest aliasesRequest, ActionListener<Void> listener) {
        client.admin().indices().aliases(aliasesRequest, ActionListener.map(listener, r -> null));
    }

    @Override
    public void segments(String index, ActionListener<SegmentsResponse> listener) {
        client.admin().indices().segments(new IndicesSegmentsRequest(index), ActionListener.map(listener, r -> new SegmentsResponse() {
            @Override
            public IndexSegments getSegments() {
                return r.getIndices().get(index);
            }

            @Override
            public int getFailedShards() {
                return r.getFailedShards();
            }

            @Override
            public DefaultShardOperationFailedException[] getShardFailures() {
                return r.getShardFailures();
            }
        }));
    }

    @Override
    public void followStats(String index, ActionListener<List<FollowStatsAction.StatsResponse>> listener) {
        FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
        request.setIndices(new String[]{index});
        client.execute(FollowStatsAction.INSTANCE, request, ActionListener.map(listener,
            FollowStatsAction.StatsResponses::getStatsResponses));
    }

    @Override
    public void stats(String index, ActionListener<IndexStats> listener) {
        client.admin().indices().stats(new IndicesStatsRequest().indices(index), ActionListener.map(listener, r -> r.getIndex(index)));
    }
}
