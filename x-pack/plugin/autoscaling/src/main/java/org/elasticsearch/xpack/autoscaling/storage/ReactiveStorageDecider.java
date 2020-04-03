/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;


import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecider;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;

import java.io.IOException;

public class ReactiveStorageDecider implements AutoscalingDecider<AutoscalingDeciderContext & AutoscalingDeciderContext> {
    public static final String NAME = "reactive_storage";

    private static final ConstructingObjectParser<ReactiveStorageDecider, Void> PARSER;
    private static final ParseField TIER_ATTRIBUTE_FIELD = new ParseField("tier_attribute");
    private static final ParseField TIER_FIELD = new ParseField("tier");

    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> new ReactiveStorageDecider((String) a[0], (String) a[1]));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIER_ATTRIBUTE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIER_FIELD);
    }
    private final String tierAttribute;
    private final String tier;

    public ReactiveStorageDecider(String tierAttribute, String tier) {
        if ((tier == null) != (tierAttribute == null)) {
            throw new IllegalArgumentException("must specify both [tier_attribute] [" + tierAttribute + "] and [tier] [" + tier + "]");
        }
        this.tierAttribute = tierAttribute;
        this.tier = tier;
    }

    public ReactiveStorageDecider(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
    }

    public static ReactiveStorageDecider parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tierAttribute);
        out.writeOptionalString(tier);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (tierAttribute != null) {
                builder.field(TIER_ATTRIBUTE_FIELD.getPreferredName(), tierAttribute);
                builder.field(TIER_FIELD.getPreferredName(), tier);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public AutoscalingDecision scale(AutoscalingDeciderContext context) {
        return null;
    }
}
