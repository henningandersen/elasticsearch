/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.IndexMetadata;

/**
 * Ability to specialize calculating the required heap necessary to hold a shard.
 */
public interface RequiredHeapCalculator {
    /**
     * The number of bytes necessary to hold a single shard with the given metadata.
     * @param metadata
     * @return
     */
    public long bytes(IndexMetadata metadata);
}
