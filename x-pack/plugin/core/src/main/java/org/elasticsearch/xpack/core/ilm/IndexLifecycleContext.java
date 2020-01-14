package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.List;

/**
 * This index provides everything that individual ILM steps needs to perform its actions.
 *
 * Todo: rename to IndexLifecycleStepContext?
 */
public interface IndexLifecycleContext {
    void rollover(RolloverRequest request, ActionListener<RolloverResponse> listener);

    void open(String index, ActionListener<AcknowledgedResponse> listener);
    void close(String index, ActionListener<AcknowledgedResponse> listener);
    void freeze(String index, ActionListener<Void> listener);

    void delete(String index, ActionListener<Void> listener);

    void resizeIndex(String target, String source, Settings targetSettings, ActionListener<AcknowledgedResponse> listener);
    void forceMerge(String index, int maxNumSegments, ActionListener<Void> listener);

    void unfollow(String followerIndex, ActionListener<AcknowledgedResponse> listener);
    void pauseFollow(String followerIndex, ActionListener<AcknowledgedResponse> listener);

    void updateSettings(String index, Settings settings, ActionListener<Void> listener);

    void aliases(IndicesAliasesRequest aliasesRequest, ActionListener<Void> listener);

    // todo: reconsider response type?
    interface SegmentsResponse {
        IndexSegments getSegments();
        int getFailedShards();
        DefaultShardOperationFailedException[] getShardFailures();
    }
    void segments(String index, ActionListener<SegmentsResponse> listener);
    void followStats(String index, ActionListener<List<FollowStatsAction.StatsResponse>> listener);

    void stats(String index, ActionListener<IndexStats> listener);
}
