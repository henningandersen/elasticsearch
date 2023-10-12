/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.state;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Locale;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class UnsafeCloseIndexIT extends ESIntegTestCase {
    public void testCloseUnassignedIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1m")
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build())
        );

        final int nbDocs = randomIntBetween(1, 1);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );

        internalCluster().restartNode(internalCluster().nodesInclude(indexName).iterator().next(), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertAcked(indicesAdmin().prepareClose(indexName).setWaitForActiveShards(ActiveShardCount.NONE));

                return super.onNodeStopped(nodeName);
            }
        });
        CloseIndexIT.assertIndexIsClosed(indexName);


        String newNode = internalCluster().startDataOnlyNode();
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("index.routing.allocation.include._name", newNode)));
        waitForRelocation();

        assertAcked(client().admin().indices().prepareOpen(indexName));

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);

    }

}
