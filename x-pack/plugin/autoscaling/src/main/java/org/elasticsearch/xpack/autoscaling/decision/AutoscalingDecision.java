/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an autoscaling decision from a single decider
 */
public class AutoscalingDecision implements ToXContent, Writeable {

    private final AutoscalingCapacity requiredCapacity;
    private final Details details;

    public interface Details extends ToXContent, NamedWriteable {
        String getSummary();
    }

    public AutoscalingDecision(AutoscalingCapacity requiredCapacity,
                               AutoscalingCapacity.StorageAndMemory node,
                               Details details) {
        this.requiredCapacity = requiredCapacity;
        this.details = details;
    }

    public AutoscalingDecision(StreamInput in) throws IOException {
        this.requiredCapacity = in.readOptionalWriteable(AutoscalingCapacity::new);
        this.details = in.readOptionalNamedWriteable(Details.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(requiredCapacity);
        out.writeOptionalNamedWriteable(details);
    }

    public AutoscalingCapacity requiredCapacity() {
        return requiredCapacity;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (requiredCapacity != null) {
                builder.field("required_capacity", requiredCapacity);
            }

            if (details != null) {
                builder.field("summary", details.getSummary());
                builder.field("details", details);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingDecision that = (AutoscalingDecision) o;
        return Objects.equals(requiredCapacity, that.requiredCapacity) &&
            Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredCapacity, details);
    }
}
