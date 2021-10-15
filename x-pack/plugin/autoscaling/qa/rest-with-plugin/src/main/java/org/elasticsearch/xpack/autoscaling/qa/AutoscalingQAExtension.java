/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.qa;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.autoscaling.AutoscalingExtension;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AutoscalingQAExtension implements AutoscalingExtension {
    private AutoscalingQA autoscalingQA;

    public AutoscalingQAExtension(AutoscalingQA autoscalingQA) {
        this.autoscalingQA = autoscalingQA;
    }

    @Override
    public Collection<AutoscalingDeciderService> deciders() {
        return List.of(new AutoscalingDeciderService() {
            @Override
            public String name() {
                return "qa";
            }

            @Override
            public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
                long end = System.currentTimeMillis() + 30000;
                do {
                    context.ensureNotCancelled();
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        assert false;
                    }
                } while (System.currentTimeMillis() < end);

                assert false;
                return null;
            }

            @Override
            public List<Setting<?>> deciderSettings() {
                return List.of();
            }

            @Override
            public List<DiscoveryNodeRole> roles() {
                return DiscoveryNodeRole.roles().stream().collect(Collectors.toUnmodifiableList());
            }

            @Override
            public boolean defaultOn() {
                return false;
            }
        });
    }
}
