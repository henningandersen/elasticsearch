/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.autoscaling.decision.AlwaysAutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;

public class MlAutoscalingDeciderService implements AutoscalingDeciderService<AlwaysAutoscalingDecider> {
    private final MetadataRolloverService rolloverService;

    @Inject
    public MlAutoscalingDeciderService(MetadataRolloverService rolloverService) {
        this.rolloverService = rolloverService;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public AutoscalingDecision scale(AlwaysAutoscalingDecider decider, AutoscalingDeciderContext context) {
        return null;
    }
}
