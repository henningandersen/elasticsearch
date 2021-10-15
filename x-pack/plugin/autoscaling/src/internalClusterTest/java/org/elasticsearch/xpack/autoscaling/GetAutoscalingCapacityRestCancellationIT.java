/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GetAutoscalingCapacityRestCancellationIT extends AutoscalingIntegTestCase {



    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return super.nodeSettings(nodeOrdinal, otherSettings);
//        return Settings.builder()
//            .put(super.nodeSettings(nodeOrdinal, otherSettings))
//            .put(NetworkModule.TRANSPORT_TYPE_KEY, nodeTransportTypeKey)
//            .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> result = new ArrayList<>(super.nodePlugins());
        result.add(AutoscalingQA.class);
        result.add(Netty4Plugin.class);
        return Collections.unmodifiableList(result);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }


//    @Override
//    protected Settings restAdminSettings() {
//        final String value = basicAuthHeaderValue("autoscaling-admin", new SecureString("autoscaling-admin-password".toCharArray()));
//        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
//    }
//
//    @Override
//    protected Settings restClientSettings() {
//        final String value = basicAuthHeaderValue("autoscaling-user", new SecureString("autoscaling-user-password".toCharArray()));
//        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
//    }

    public void testCapacityRestCancellation() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            "test",
            new TreeSet<>(Set.of(DiscoveryNodeRole.DATA_ROLE.roleName())),
            new TreeMap<>(Map.of("qa", Settings.EMPTY))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());

//        Request putPolicyRequest = new Request("PUT", "/_autoscaling/policy/test");
//        putPolicyRequest.setJsonEntity(
//            "{\"roles\": "
//                + "  [\"master\", \"data\", \"data_content\", \"data_hot\", \"data_warm\", \"data_cold\", \"data_frozen\",\n"
//                + "   \"ingest\", \"ml\", \"transform\", \"remote_cluster_client\"],\n"
//                + " \"deciders\": { \"qa\": {} }\n"
//                + "}"
//        );
//
//        assertOK(createRestClient().performRequest(putPolicyRequest));

        try (RestClient restClient = createRestClient()) {

            PlainActionFuture<Response> future = new PlainActionFuture<>();
            Request getCapacityRequest = new Request("GET", "/_autoscaling/capacity");
            Cancellable cancellable = restClient.performRequestAsync(getCapacityRequest, wrapAsRestResponseListener(future));

            awaitTaskWithPrefix(GetAutoscalingCapacityAction.NAME);

            internalCluster().getInstance(LocalStateAutoscaling.class);
//            assertBusy(() -> { assertThat(runningTasks(), contains("cluster:admin/autoscaling/get_autoscaling_capacity")); });
            // improve this in a follow-up to wait of the task waiting.
            Thread.sleep(10);

            logger.info("--> cancelling");
            cancellable.cancel();

            expectThrows(CancellationException.class, future::actionGet);

            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.getAction().equals(ClusterStateAction.NAME)));

//            assertBusy(() -> { assertThat(runningTasks(), not(contains("cluster:admin/autoscaling/get_autoscaling_capacity"))); });
        }
    }
}
