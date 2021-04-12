/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.util.List;

public class FrozenStorageDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "frozen_storage";

    public static final Setting<Double> PERCENTAGE = Setting.doubleSetting("percentage", 5.0d, 0.0d);


    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        return null;
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(PERCENTAGE);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }
}
