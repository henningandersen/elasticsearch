/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This reference manager delegates all it's refresh calls to another (internal) ReaderManager
 * The main purpose for this is that if we have external refreshes happening we don't issue extra
 * refreshes to clear version map memory etc. this can cause excessive segment creation if heavy indexing
 * is happening and the refresh interval is low (ie. 1 sec)
 * <p>
 * This also prevents segment starvation where an internal reader holds on to old segments literally forever
 * since no indexing is happening and refreshes are only happening to the external reader manager, while with
 * this specialized implementation an external refresh will immediately be reflected on the internal reader
 * and old segments can be released in the same way previous version did this (as a side-effect of _refresh)
 */
@SuppressForbidden(reason = "reference counting is required here")
final class ExternalReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> implements IndexReaderManager {
    final InternalReaderManager internalReaderManager;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExternalRefreshWarmer warmer;
    private final LongSupplier maxSeqNoSupplier;
    private final AtomicLong lastKnownGlobalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
    private final List<PendingReader> pendingReaders = new ArrayList<>();

    ExternalReaderManager(InternalReaderManager internalReaderManager, ExternalRefreshWarmer warmer,
                          LongSupplier maxSeqNoSupplier) throws IOException {
        this.internalReaderManager = internalReaderManager;
        this.maxSeqNoSupplier = maxSeqNoSupplier;
        this.warmer = warmer;
        ElasticsearchDirectoryReader reader = internalReaderManager.acquire();
        try {
            warmer.warm(reader);
            this.current = reader;
            reader = null;
        } finally {
            if (reader != null) {
                release(reader);
            }
        }
    }

    @Override
    public boolean refresh(boolean blocking) throws IOException {
        return refreshInternal();
    }

    private boolean refreshInternal() throws IOException {
        internalReaderManager.maybeRefreshBlocking();
        synchronized (this) {
            if (closed.get()) {
                throw new AlreadyClosedException("ReaderManager is closed");
            }
            final ElasticsearchDirectoryReader newReader = internalReaderManager.acquire();
            final long maxSeqNo = maxSeqNoSupplier.getAsLong();
            if (pendingReaders.isEmpty() == false) {
                final PendingReader lastPendingReader = pendingReaders.get(pendingReaders.size() - 1);
                if (lastPendingReader.reader == newReader) {
                    release(newReader);
                    return false;
                }
                if (lastPendingReader.maxSeqNo == maxSeqNo) {
                    pendingReaders.set(pendingReaders.size() - 1, new PendingReader(maxSeqNo, newReader));
                    release(lastPendingReader.reader);
                    return true;
                }
            }
            pendingReaders.add(new PendingReader(maxSeqNo, newReader));
            return true;
        }
    }

    /**
     * Called when the global checkpoint has advanced enough to expose a new safe reader
     */
    void exposeSafeReaderOnNewGlobalCheckpoint(long globalCheckpoint) throws IOException {
        final long newGlobalCheckpoint = lastKnownGlobalCheckpoint.updateAndGet(curr -> SequenceNumbers.max(curr, globalCheckpoint));
        if (newGlobalCheckpoint == globalCheckpoint) {
            maybeRefreshBlocking(); // expose the new safe reader
        }
    }

    private void releasePendingReaders(List<PendingReader> pendingReaders) throws IOException {
        IOUtils.close(pendingReaders.stream().map(r -> (Closeable) () -> release(r.reader)).collect(Collectors.toList()));
    }

    @Override
    protected void afterClose() throws IOException {
        releasePendingReaders(pendingReaders);
    }

    /**
     * A listener that warms the segments if needed when acquiring a new reader
     */
    static final class ExternalRefreshWarmer {
        private final Engine.Warmer warmer;
        private final Logger logger;
        private final AtomicBoolean isEngineClosed;

        ExternalRefreshWarmer(Engine.Warmer warmer, Logger logger, AtomicBoolean isEngineClosed) {
            this.warmer = Objects.requireNonNull(warmer);
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        void warm(ElasticsearchDirectoryReader reader) {
            try {
                warmer.warm(reader);
            } catch (Exception e) {
                if (isEngineClosed.get() == false) {
                    logger.warn("failed to prepare/warm", e);
                }
            }
        }
    }

    static class PendingReader {
        final long maxSeqNo;
        final ElasticsearchDirectoryReader reader;

        PendingReader(long maxSeqNo, ElasticsearchDirectoryReader reader) {
            this.maxSeqNo = maxSeqNo;
            this.reader = reader;
        }
    }

    @Override
    protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
        final List<PendingReader> safePendingReaders = new ArrayList<>();
        final long globalCheckpoint = lastKnownGlobalCheckpoint.get();
        synchronized (this) {
            for (Iterator<PendingReader> iter = pendingReaders.iterator(); iter.hasNext(); ) {
                final PendingReader pendingReader = iter.next();
                if (pendingReader.maxSeqNo <= globalCheckpoint) {
                    iter.remove();
                    safePendingReaders.add(pendingReader);
                } else {
                    break;
                }
            }
        }
        if (safePendingReaders.isEmpty() == false) {
            final PendingReader toExposeReader = safePendingReaders.get(safePendingReaders.size() - 1);
            if (toExposeReader.reader == referenceToRefresh) {
                releasePendingReaders(safePendingReaders);
                return null;
            }
            final List<PendingReader> outdatedReaders = safePendingReaders.subList(0, safePendingReaders.size() - 1);
            boolean success = false;
            try {
                releasePendingReaders(outdatedReaders);
                warmer.warm(toExposeReader.reader);
                success = true;
            } finally {
                if (success == false) {
                    release(toExposeReader.reader);
                }
            }
            return toExposeReader.reader;
        } else {
            return null;
        }
    }

    @Override
    protected boolean tryIncRef(ElasticsearchDirectoryReader reference) throws IOException {
        return reference.tryIncRef();
    }

    @Override
    protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected int getRefCount(ElasticsearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
