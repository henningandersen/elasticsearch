/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.concurrent.CancellationException;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;

public class GetAutoscalingCapacityRestCancellationIT extends ESRestTestCase {

    @Override
    protected Settings restAdminSettings() {
        final String value = basicAuthHeaderValue("autoscaling-admin", new SecureString("autoscaling-admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    @Override
    protected Settings restClientSettings() {
        final String value = basicAuthHeaderValue("autoscaling-user", new SecureString("autoscaling-user-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    public void testCapacityRestCancellation() throws Exception {

        Request putPolicyRequest = new Request("PUT", "/_autoscaling/policy/test");
        putPolicyRequest.setJsonEntity(
            "{\"roles\": "
                + "  [\"master\", \"data\", \"data_content\", \"data_hot\", \"data_warm\", \"data_cold\", \"data_frozen\",\n"
                + "   \"ingest\", \"ml\", \"transform\", \"remote_cluster_client\"],\n"
                + " \"deciders\": { \"qa\": {} }\n"
                + "}"
        );

        assertOK(adminClient().performRequest(putPolicyRequest));

        PlainActionFuture<Response> future = new PlainActionFuture<>();
        Request getCapacityRequest = new Request("GET", "/_autoscaling/capacity");
        Cancellable cancellable = adminClient().performRequestAsync(getCapacityRequest, wrapAsRestResponseListener(future));

        assertBusy(() -> { assertThat(runningTasks(), contains("cluster:admin/autoscaling/get_autoscaling_capacity")); });
        // improve this in a follow-up to wait of the task waiting.
        Thread.sleep(10);

        cancellable.cancel();

        expectThrows(CancellationException.class, future::actionGet);

        assertBusy(() -> { assertThat(runningTasks(), not(contains("cluster:admin/autoscaling/get_autoscaling_capacity"))); });
    }
}
