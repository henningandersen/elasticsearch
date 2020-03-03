/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Objects;

/**
 * Updates the settings for an index.
 */
public class UpdateSettingsStep extends AsyncActionStep {
    public static final String NAME = "update-settings";

    private final Settings settings;

    public UpdateSettingsStep(StepKey key, StepKey nextStepKey, Settings settings) {
        super(key, nextStepKey);
        this.settings = settings;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, ClusterStateObserver observer, Listener listener, Client client) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexMetaData.getIndex().getName())
            .masterNodeTimeout(getMasterTimeout(currentState))
            .settings(settings);
        client.admin().indices().updateSettings(updateSettingsRequest,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    @Override
    public ClusterState performDryRun(IndexMetaData indexMetaData, ClusterState currentClusterState, DryRunContext context) {
        // todo: decide on async or sync.
        class MyClustateStateUpdater implements MetaDataUpdateSettingsService.ClusterStateUpdater {
            final ClusterState input;
            ClusterState result;

            MyClustateStateUpdater(ClusterState input) {
                this.input = input;
            }

            @Override
            public void submitStateUpdateTask(String reason, ClusterStateUpdateTask task) {
                assert result == null;
                try {
                    result = task.execute(input);
                    // notify
                } catch (Exception e) {
                    throw new ElasticsearchException(e);
                }
            }

            public ClusterState getResult() {
                assert result != null;
                return result;
            }
        }


        MyClustateStateUpdater updater = new MyClustateStateUpdater(currentClusterState);
        MetaDataUpdateSettingsService updateSettingsService = new MetaDataUpdateSettingsService(updater::submitStateUpdateTask,
            context::reroute, context.getIndexScopedSettings(),
            context.getIndicesService(), context.getThreadPool());
        UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest()
            .indices(new Index[] { indexMetaData.getIndex() })
            .settings(settings)
            .setPreserveExisting(false)
            .ackTimeout(AcknowledgedRequest.DEFAULT_ACK_TIMEOUT)
            .masterNodeTimeout(getMasterTimeout(currentClusterState));

        PlainActionFuture<ClusterStateUpdateResponse> future = new PlainActionFuture<>();
        updateSettingsService.updateSettings(clusterStateUpdateRequest, future);
        return updater.getResult();

    }

    public Settings getSettings() {
        return settings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), settings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateSettingsStep other = (UpdateSettingsStep) obj;
        return super.equals(obj) &&
                Objects.equals(settings, other.settings);
    }
}
