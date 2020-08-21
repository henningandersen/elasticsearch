/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class FixedAutoscalingDeciderConfiguration implements AutoscalingDeciderConfiguration {

    public static final String NAME = "fixed";

    private static final ConstructingObjectParser<FixedAutoscalingDeciderConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        c -> new FixedAutoscalingDeciderConfiguration((ByteSizeValue) c[0], (ByteSizeValue) c[1], (int) c[2])
    );

    private static final ParseField STORAGE = new ParseField("storage");
    private static final ParseField MEMORY = new ParseField("memory");
    private static final ParseField NODES = new ParseField("nodes");
    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.textOrNull(), STORAGE.getPreferredName()),
            STORAGE,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.textOrNull(), MEMORY.getPreferredName()),
            MEMORY,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NODES);
    }

    public static FixedAutoscalingDeciderConfiguration parse(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final ByteSizeValue storage;
    private final ByteSizeValue memory;
    private final int nodes;

    public FixedAutoscalingDeciderConfiguration() {
        this(null, null, 1);
    }

    public FixedAutoscalingDeciderConfiguration(ByteSizeValue storage, ByteSizeValue memory, int nodes) {
        this.storage = storage;
        this.memory = memory;
        this.nodes = nodes;
    }

    @SuppressWarnings("unused")
    public FixedAutoscalingDeciderConfiguration(final StreamInput in) throws IOException {
        this.storage = in.readOptionalWriteable(ByteSizeValue::new);
        this.memory = in.readOptionalWriteable(ByteSizeValue::new);
        this.nodes = in.readInt();
    }

    public ByteSizeValue storage() {
        return storage;
    }

    public ByteSizeValue memory() {
        return memory;
    }

    public int nodes() {
        return nodes;
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
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(storage);
        out.writeOptionalWriteable(memory);
        out.writeInt(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field("storage", storage);
            builder.field("memory", memory);
            builder.field("nodes", nodes);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FixedAutoscalingDeciderConfiguration that = (FixedAutoscalingDeciderConfiguration) o;
        return nodes == that.nodes && Objects.equals(storage, that.storage) && Objects.equals(memory, that.memory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage, memory, nodes);
    }
}
