/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

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
