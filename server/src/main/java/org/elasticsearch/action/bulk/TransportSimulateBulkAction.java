/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.SimulateIngestService;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class TransportSimulateBulkAction extends TransportAbstractBulkAction {

    private IndicesService indicesService;

    @Inject
    public TransportSimulateBulkAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        IngestService ingestService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        IndicesService indicesService
    ) {
        super(
            SimulateBulkAction.INSTANCE,
            transportService,
            actionFilters,
            SimulateBulkRequest::new,
            threadPool,
            clusterService,
            ingestService,
            indexingPressure,
            systemIndices,
            System::nanoTime
        );
        this.indicesService = indicesService;
    }

    @Override
    protected void doInternalExecute(Task task, BulkRequest bulkRequest, Executor executor, ActionListener<BulkResponse> listener) {
        final long startTime = threadPool.relativeTimeInMillis();
        if (applyPipelines(task, bulkRequest, threadPool.executor(ThreadPool.Names.WRITE), listener)) return;

        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            DocWriteRequest<?> docRequest = bulkRequest.requests.get(i);
            assert docRequest instanceof IndexRequest; // This action is only ever called with IndexRequests
            IndexRequest request = (IndexRequest) docRequest;
            final SourceToParse sourceToParse = new SourceToParse(
                request.id(),
                request.source(),
                request.getContentType(),
                request.routing(),
                request.getDynamicTemplates(),
                DocumentSizeObserver.EMPTY_INSTANCE
            );

            ClusterState state = clusterService.state();
            // may want to use indexname expression resolver?
            IndexMetadata imd = state.metadata()
                .getIndexSafe(
                    state.metadata().getIndicesLookup().get(request.index()).getWriteIndex((IndexRequest) request, state.metadata())
                );

            Engine.Index indexResult;
            Exception exception = null;
            try {
                // todo: we could ensure to only create one index service per index instead.
                // also we should verify the performance reqs of the simulate API allows the overhead here.
                indexResult = indicesService.withTempIndexService(imd, indexService -> {
                    return IndexShard.prepareIndex(
                        indexService.mapperService(),
                        sourceToParse,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        -1,
                        -1,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        Long.MIN_VALUE,
                        false,
                        request.ifSeqNo(),
                        request.ifPrimaryTerm(),
                        startTime
                    );
                });
            } catch (Exception e) {
                exception = e;
            }

            responses.set(
                i,
                BulkItemResponse.success(
                    0,
                    DocWriteRequest.OpType.CREATE,
                    new SimulateIndexResponse(
                        request.id(),
                        request.index(),
                        request.version(),
                        request.source(),
                        request.getContentType(),
                        request.getExecutedPipelines(),
                        exception
                    )
                )
            );
        }
        listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime)));
    }

    /*
     * This overrides TransportSimulateBulkAction's getIngestService to allow us to provide an IngestService that handles pipeline
     * substitutions defined in the request.
     */
    @Override
    protected IngestService getIngestService(BulkRequest request) {
        IngestService rawIngestService = super.getIngestService(request);
        return new SimulateIngestService(rawIngestService, request);
    }

    @Override
    protected boolean shouldStoreFailure2(String indexName, Metadata metadata, long time) {
        return false;
    }
}
