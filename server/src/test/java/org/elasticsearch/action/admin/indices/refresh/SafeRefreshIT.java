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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SafeRefreshIT extends ESIntegTestCase {

    public void testRefreshWaitUntilSafe() {
        String indexName = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        createIndex(indexName, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), randomFrom(Translog.Durability.values()))
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).build());
        ensureGreen(indexName);
        client().admin().indices().prepareRefresh(indexName).get();

        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "_doc", Integer.toString(i)).setSource("f", "v").get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        final SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(new MatchAllQueryBuilder()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numDocs));
    }
}
