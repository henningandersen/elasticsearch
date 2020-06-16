/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingDeciderServices;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderService;

public class MlAutoscalingIT extends MlSingleNodeTestCase {
    @Override
    protected Settings nodeSettings()  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(Autoscaling.AUTOSCALING_ENABLED_SETTING.getKey(), true);
        return newSettings.build();
    }

    public void testDeciderServiceRegistered() {
        assertTrue(getInstanceFromNode(AutoscalingDeciderServices.Holder.class).get().getDeciders()
            .stream().anyMatch(d -> d.getClass() == MlAutoscalingDeciderService.class));
    }

}
