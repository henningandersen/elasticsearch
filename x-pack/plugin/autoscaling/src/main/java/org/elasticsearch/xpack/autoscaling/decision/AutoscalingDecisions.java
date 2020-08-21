/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a collection of individual autoscaling decisions that can be aggregated into a single autoscaling decision for a tier
 */
public class AutoscalingDecisions implements ToXContent, Writeable {

    private final String tier;
    private final AutoscalingCapacity currentCapacity;
    private final Collection<AutoscalingDecision> decisions;

    public Collection<AutoscalingDecision> decisions() {
        return decisions;
    }

    public AutoscalingDecisions(
        final String tier,
        final AutoscalingCapacity currentCapacity,
        final Collection<AutoscalingDecision> decisions
    ) {
        Objects.requireNonNull(tier);
        Objects.requireNonNull(currentCapacity);
        this.tier = tier;
        this.currentCapacity = currentCapacity;
        Objects.requireNonNull(decisions);
        if (decisions.isEmpty()) {
            throw new IllegalArgumentException("decisions can not be empty");
        }
        this.decisions = decisions;
    }

    public AutoscalingDecisions(final StreamInput in) throws IOException {
        this.tier = in.readString();
        this.currentCapacity = new AutoscalingCapacity(in);
        this.decisions = in.readList(AutoscalingDecision::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(tier);
        currentCapacity.writeTo(out);
        out.writeCollection(decisions);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("tier", tier);
        AutoscalingCapacity requiredCapacity = requiredCapacity();
        if (requiredCapacity != null) {
            builder.field("required_capacity", requiredCapacity);
        }
        builder.field("current_capacity", currentCapacity);
        builder.array("decisions", decisions.toArray());
        builder.endObject();
        return builder;
    }

    public AutoscalingCapacity requiredCapacity() {
        if (decisions.isEmpty() || decisions.stream().map(AutoscalingDecision::requiredCapacity).anyMatch(Objects::isNull)) {
            // any undetermined decider cancels out any decision making.
            return null;
        }
        Optional<AutoscalingCapacity> result = decisions.stream()
            .map(AutoscalingDecision::requiredCapacity)
            .reduce(AutoscalingCapacity::upperBound);
        assert result.isPresent();
        return result.get();
    }

    public AutoscalingCapacity currentCapacity() {
        return currentCapacity;
    }

    public String tier() {
        return tier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoscalingDecisions that = (AutoscalingDecisions) o;
        return tier.equals(that.tier) && currentCapacity.equals(that.currentCapacity) && decisions.equals(that.decisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier, currentCapacity, decisions);
    }
}
