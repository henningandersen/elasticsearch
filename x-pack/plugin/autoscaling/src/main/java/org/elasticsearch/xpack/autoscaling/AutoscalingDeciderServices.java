/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;

import java.util.Set;

public class AutoscalingDeciderServices {
    private static final Logger logger = LogManager.getLogger(AutoscalingDeciderServices.class);
    private Set<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders;

    public AutoscalingDeciderServices(Set<AutoscalingDeciderService<? extends AutoscalingDecider>> deciders) {
        this.deciders = deciders;
        assert this.deciders.size() >= 1;
        logger.info("Deciders: " + deciders);
    }

    public static class Holder {
        private final Autoscaling autoscaling;
        private final SetOnce<AutoscalingDeciderServices> servicesSetOnce = new SetOnce<>();

        public Holder(Autoscaling autoscaling) {
            this.autoscaling = autoscaling;
        }

        public AutoscalingDeciderServices get() {
            // defer constructing services until transport action creation time.
            AutoscalingDeciderServices autoscalingDeciderServices = servicesSetOnce.get();
            if (autoscalingDeciderServices == null) {
                autoscalingDeciderServices = new AutoscalingDeciderServices(autoscaling.createDeciders());
                servicesSetOnce.set(autoscalingDeciderServices);
            }

            return autoscalingDeciderServices;
        }
    }

    // temporary until we implement Ml autoscaling.
    public Set<AutoscalingDeciderService<? extends AutoscalingDecider>> getDeciders() {
        return deciders;
    }
}
