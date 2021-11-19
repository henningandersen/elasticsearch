/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardMarkCommitAndIndexAction extends TransportWriteAction<
    ShardMarkCommitAndIndexRequest,
    ShardMarkCommitAndIndexRequest,
    ShardMarkCommitAndIndexResponse> {

    @Inject
    public TransportShardMarkCommitAndIndexAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
    ) {
        super(
            settings,
            ShardMarkCommitAndIndexAction.NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardMarkCommitAndIndexRequest::new,
            ShardMarkCommitAndIndexRequest::new,
            ExecutorSelector::getWriteExecutorForShard,
            false,
            indexingPressure,
            systemIndices
        );
    }

    @Override
    protected ShardMarkCommitAndIndexResponse newResponseInstance(StreamInput in) throws IOException {
        return new ShardMarkCommitAndIndexResponse(in);
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        ShardMarkCommitAndIndexRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardMarkCommitAndIndexRequest, ShardMarkCommitAndIndexResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            primary.commitTransaction(request.txid());
            return new PrimaryResult<>(request, new ShardMarkCommitAndIndexResponse());
        });
    }

    @Override
    protected void dispatchedShardOperationOnReplica(
        ShardMarkCommitAndIndexRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            replica.commitTransaction(request.txid());
            return new ReplicaResult();
        });
    }
}
