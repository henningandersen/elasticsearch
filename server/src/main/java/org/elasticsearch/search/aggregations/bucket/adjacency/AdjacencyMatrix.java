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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A multi bucket aggregation where the buckets are defined by a set of filters
 * (a bucket is produced per filter plus a bucket for each non-empty filter
 * intersection so A, B and A&amp;B).
 */
public interface AdjacencyMatrix extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific filter or pair (identified by its
     * key)
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    /**
     * The buckets created by this aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

    Bucket getBucketByKey(String key);

}
