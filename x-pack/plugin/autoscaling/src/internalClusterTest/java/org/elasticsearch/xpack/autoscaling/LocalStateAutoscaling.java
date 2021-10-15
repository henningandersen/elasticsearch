/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.HashSet;
import java.util.Set;

public class LocalStateAutoscaling extends LocalStateCompositeXPackPlugin {

    public LocalStateAutoscaling(final Settings settings) {
        super(settings, null);
        plugins.add(new Autoscaling(new AutoscalingLicenseChecker(() -> true)) {
            @Override
            public Set<AutoscalingDeciderService> createDeciderServices(AllocationDeciders allocationDeciders) {
                Set<AutoscalingDeciderService> deciderServices = new HashSet<>(super.createDeciderServices(allocationDeciders));
                deciderServices.add(new AutoscalingQADeciderService());
                return deciderServices;
            }
        });
    }

}
