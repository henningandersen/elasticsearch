/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

// todo: make this a per node request.
public class ShardMarkCommitAndIndexRequest extends ReplicatedWriteRequest<ShardMarkCommitAndIndexRequest> {
    private final TxID txID;

    public ShardMarkCommitAndIndexRequest(ShardId shardId, TxID txID) {
        super(shardId);
        this.txID = txID;
    }

    public ShardMarkCommitAndIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.txID = new TxID(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        txID.writeTo(out);
    }

    public TxID txid() {
        return txID;
    }

    @Override
    public String toString() {
        return "[" + shardId + "," + txID + "]";
    }
}
