/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.util.Collection;
import java.util.List;

public class AutoscalingQAExtension implements AutoscalingExtension {
    private AutoscalingQA autoscalingQA;

    public AutoscalingQAExtension(AutoscalingQA autoscalingQA) {
        this.autoscalingQA = autoscalingQA;
    }

    @Override
    public Collection<AutoscalingDeciderService> deciders() {
        return List.of(new AutoscalingQADeciderService());
    }

}
