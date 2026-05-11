/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/**
 * A {@link MutableObjectStoreUploadTracker} wrapper that also considers BCC generations up to a
 * caller-supplied {@code latestUploadedBccTermAndGen} as uploaded to the object store, regardless
 * of the state of the underlying tracker. This is used during prefetching so that BCC blobs whose
 * upload status is already known from the commit notification (but has not yet been propagated to
 * the tracker via {@link MutableObjectStoreUploadTracker#updateLatestUploadedBcc}) are correctly
 * routed to the object store rather than the indexing node.
 */
public class NotificationAwareObjectStoreUploadTracker implements MutableObjectStoreUploadTracker {

    private final MutableObjectStoreUploadTracker delegate;
    private final PrimaryTermAndGeneration latestUploadedBccTermAndGen;

    public NotificationAwareObjectStoreUploadTracker(
        MutableObjectStoreUploadTracker delegate,
        PrimaryTermAndGeneration latestUploadedBccTermAndGen
    ) {
        this.delegate = delegate;
        this.latestUploadedBccTermAndGen = latestUploadedBccTermAndGen;
    }

    @Override
    public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
        if (bccTermAndGen.compareTo(latestUploadedBccTermAndGen) <= 0) {
            return UPLOADED;
        }
        return delegate.getLatestUploadInfo(bccTermAndGen);
    }

    @Override
    public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        delegate.updateLatestUploadedBcc(latestUploadedBccTermAndGen);
    }

    @Override
    public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        delegate.updateLatestCommitInfo(ccTermAndGen, nodeId);
    }
}
