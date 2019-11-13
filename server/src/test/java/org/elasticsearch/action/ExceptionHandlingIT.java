package org.elasticsearch.action;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;

import java.util.concurrent.CountDownLatch;

/**
 * Integration tests showing different exception handling behaviours, caused by onResponse exceptions being handled by the underlying
 * TransportAction.
 */
public class ExceptionHandlingIT extends ESIntegTestCase {

    /**
     * When creating index against a non-master we get a RemoteTransportException.
     *
     * The onFailure invocation happens in
     * <a href="https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java#L190">TransportMasterNodeAction:190</a>
     *
     * and is turned into a RemoteTransportException in
     * <a href="https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/transport/InboundHandler.java#L236">TransportService:236</a>
     *
     * since it is handled like any transport exception.
     *
     * Turning on trace would show: "failure when forwarding request [{}] to master [{}]"
     */
    public void testCreateIndexNonMaster() throws InterruptedException {
        String nonMaster = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        CountDownLatch latch = new CountDownLatch(1);
        client(nonMaster).admin().indices().prepareCreate("test").execute(new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    throw new TestException();
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, Matchers.instanceOf(RemoteTransportException.class));
                    latch.countDown();
                }
            });
        latch.await();
    }

    /**
     * When creating index directly against master we get the original exception from onResponse
     *
     * The onFailure invocation happens in
     * <a href="https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java#L162">TransportMasterNodeAction:162</a>
     *
     * coming from ActionListener.wrap being used
     *
     * Turning on debug would show: "unexpected exception during publication"
     */
    public void testCreateIndexMaster() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        internalCluster().masterClient().admin().indices().prepareCreate("test").execute(new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                throw new TestException();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, Matchers.instanceOf(TestException.class));
                latch.countDown();
            }
        });
        latch.await();
    }

    /**
     * When suing nodes stats, exceptions from onResponse are silently ignored because of (inadvertent?) double-fire check in
     * <a href="https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/action/support/nodes/TransportNodesAction.java#L218">TransportNodesAction:218</a>
     *
     *
     * @throws InterruptedException
     */
    public void testNodesStats() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareNodesStats().execute(
            new ActionListener<>() {
            @Override
            public void onResponse(NodesStatsResponse response) {
                latch.countDown();
                throw new TestException();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected");
            }
        });
        latch.await();
    }

    private static class TestException extends RuntimeException {
    }
}
