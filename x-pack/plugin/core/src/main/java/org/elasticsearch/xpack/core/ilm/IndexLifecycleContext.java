package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;

/**
 * This index provides everything that individual ILM steps needs to perform its actions.
 *
 * Todo: rename to AsyncActionStepContext?
 */
public interface IndexLifecycleContext {


    class RolloverResponse {
        private final boolean rolledOver;

        public RolloverResponse(boolean rolledOver) {
            this.rolledOver = rolledOver;
        }

        public boolean isRolledOver() {
            return rolledOver;
        }
    }
    void rolloverIndex(String alias, ActionListener<RolloverResponse> listener);

    void open(String index, ActionListener<AcknowledgedResponse> listener);
    void close(String index, ActionListener<AcknowledgedResponse> listener);
    void freeze(String index, ActionListener<Void> listener);

    void delete(String index, ActionListener<Void> listener);

    void resizeIndex(String target, String source, Settings targetSettings, ActionListener<AcknowledgedResponse> listener);
    void forceMerge(String index, int maxNumSegments, ActionListener<Void> listener);

    void unfollow(String index, ActionListener<AcknowledgedResponse> listener);
    void pauseFollow(String index, ActionListener<AcknowledgedResponse> listener);

    void updateSettings(String index, Settings settings, ActionListener<Void> listener);
    void setSingleNodeAllocation(String index, String nodeId, ActionListener<Void> listener);

    void aliases(IndicesAliasesRequest aliasesRequest, ActionListener<Void> listener);
}
