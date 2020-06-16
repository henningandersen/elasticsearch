/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.xpack.autoscaling.AutoscalingExtension;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collection;

public class MlAutoscalingExtension implements AutoscalingExtension {
    private MachineLearning plugin;

    public MlAutoscalingExtension(MachineLearning plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders() {
        return plugin.deciders();
    }
}
