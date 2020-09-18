/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
@TestIssueLogging(value = "org.elasticsearch.cluster.action.shard:TRACE",
    issueUrl = "https://github.com/elastic/elasticsearch/issues/51329")
public class MarkShardFailedFailureIT extends ESIntegTestCase {

//    @Override
//    protected Settings nodeSettings(int nodeOrdinal) {
//        return Settings.builder()
//            .put(super.nodeSettings(nodeOrdinal))
//            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), CircuitBreaker.Type.NOOP)
//            .build();
//    }

    /**
     * Test that if master circuit breaks a mark shard failed, we survive.
     */
    public void testMarkShardFailedCircuitBroken() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        String replica = internalCluster().startDataOnlyNode();
        ensureGreen("test");
        // mappings
        index("test", "id2", "{}");

        NetworkDisruption.TwoPartitions partitions = new NetworkDisruption.TwoPartitions(primary, replica);

        setDisruptionScheme(new NetworkDisruption(partitions, NetworkDisruption.DISCONNECT));

        assertAcked(client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest()
            .transientSettings(Map.of(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "0b")))
            .actionGet());
        client(primary).prepareIndex("test").setId("id1").setSource("{}", XContentType.JSON).execute().actionGet();
//        index("test", "id1", "{}");

//        internalCluster().restartNode(master, new InternalTestCluster.RestartCallback() {
//            @Override
//            public Settings onNodeStopped(String nodeName) throws Exception {
//                return Settings.builder()
//                    .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "0b")
//                    .build();
//            }
//        });
//        index("test", "id2", "{}");

        assertAcked(client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest()
            .transientSettings(Settings.builder().putNull(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()))).actionGet());
    }
}
