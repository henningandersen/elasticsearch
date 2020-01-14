package org.elasticsearch.xpack.ilm.history;

import java.io.Closeable;

import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.INDEX_TEMPLATE_VERSION;

public interface ILMHistoryStore extends Closeable {
    String ILM_HISTORY_INDEX_PREFIX = "ilm-history-" + INDEX_TEMPLATE_VERSION + "-";
    String ILM_HISTORY_ALIAS = "ilm-history-" + INDEX_TEMPLATE_VERSION;

    /**
     * Attempts to asynchronously index an ILM history entry
     */
    void putAsync(ILMHistoryItem item);

    @Override
    void close();
}
