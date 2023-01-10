/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public enum ShardCopyRole implements Writeable, ToXContentFragment {
    DEFAULT((byte) 0, true, true),
    INDEX_ONLY((byte) 1, true, false),
    SEARCH_ONLY((byte) 2, false, true);

    private final byte code;
    private final boolean promotable;
    private final boolean searchable;

    ShardCopyRole(byte code, boolean promotable, boolean searchable) {
        this.code = code;
        this.promotable = promotable;
        this.searchable = searchable;
    }

    /**
     * @return whether a shard copy with this role may be promoted from replica to primary. If {@code index.number_of_replicas} is reduced,
     * unpromotable replicas are removed first.
     */
    public boolean isPromotableToPrimary() {
        return promotable;
    }

    /**
     * @return whether a shard copy with this role may be the target of a search.
     */
    public boolean isSearchable() {
        return searchable;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("role", toString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(code);
    }

    public static ShardCopyRole readFrom(StreamInput in) throws IOException {
        return switch (in.readByte()) {
            case 0 -> DEFAULT;
            case 1 -> INDEX_ONLY;
            case 2 -> SEARCH_ONLY;
            default -> throw new IllegalStateException("unknown role");
        };
    }
}
