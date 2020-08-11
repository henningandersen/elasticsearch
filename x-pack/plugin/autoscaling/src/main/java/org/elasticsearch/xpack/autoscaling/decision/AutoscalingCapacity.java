/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents current/required capacity of a single tier.
 */
public class AutoscalingCapacity implements ToXContent, Writeable {

    private final StorageAndMemory cluster;
    private final StorageAndMemory node;

    public static class StorageAndMemory implements ToXContent, Writeable {
        public final ByteSizeValue storage;
        public final ByteSizeValue memory;

        public StorageAndMemory(ByteSizeValue storage, ByteSizeValue memory) {
            this.storage = storage;
            this.memory = memory;
        }

        public StorageAndMemory(StreamInput in) throws IOException {
            this.storage = in.readOptionalWriteable(ByteSizeValue::new);
            this.memory = in.readOptionalWriteable(ByteSizeValue::new);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("storage", storage.getBytes());
            builder.field("memory", memory.getBytes());
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(storage);
            out.writeOptionalWriteable(memory);
        }

        public static StorageAndMemory max(StorageAndMemory sm1, StorageAndMemory sm2) {
            if (sm1 == null) {
                return sm2;
            }
            if (sm2 == null) {
                return sm1;
            }

            return new StorageAndMemory(max(sm1.memory, sm2.memory), max(sm1.storage, sm2.storage));
        }

        private static ByteSizeValue max(ByteSizeValue v1, ByteSizeValue v2) {
            if (v1 == null) {
                return v2;
            }
            if (v2 == null) {
                return v1;
            }

            return v1.compareTo(v2) < 0 ? v2 : v1;
        }
    }

    public AutoscalingCapacity(StorageAndMemory cluster, StorageAndMemory node) {
        assert cluster != null : "Cannot provide capacity without specifying cluster level capacity";
        assert node == null || node.memory == null
            // implies
            || cluster.memory != null : "Cannot provide node memory without cluster memory";
        assert node == null || node.storage == null
            // implies
            || cluster.storage != null : "Cannot provide node memory without cluster memory";

        this.cluster = cluster;
        this.node = node;
    }

    public AutoscalingCapacity(StreamInput in) throws IOException {
        this.cluster = new StorageAndMemory(in);
        this.node = in.readOptionalWriteable(StorageAndMemory::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        cluster.writeTo(out);
        out.writeOptionalWriteable(node);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (node != null) {
            builder.field("node", node);
        }
        builder.field("cluster", cluster);
        builder.endObject();
        return builder;
    }

    public static AutoscalingCapacity upperBound(AutoscalingCapacity c1, AutoscalingCapacity c2) {
        return new AutoscalingCapacity(StorageAndMemory.max(c1.cluster, c2.cluster), StorageAndMemory.max(c1.node, c2.node));
    }
}
