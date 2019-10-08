package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.health.ClusterHealthStatus.RED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TransportResizeActionIT extends ESIntegTestCase {

    public void testFailure() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        boolean shrink = randomBoolean();
        int shards = randomIntBetween(1, 5);
        int resizeFactor = randomIntBetween(1, 5);
        int initialShards = shrink ? resizeFactor * shards : shards;
        int resizedShards = shrink ? shards : resizeFactor * shards;
        ResizeType resizeType = resizeFactor == 1 ? ResizeType.CLONE : (shrink ? ResizeType.SHRINK : ResizeType.SPLIT);
        String node = internalCluster().getDataNodeInstance(Node.class).settings().get(Node.NODE_NAME_SETTING.getKey());
        createIndex("source", Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, initialShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), shards * resizeFactor)
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", node)
            .build());

        indexRandom(randomBoolean(), randomBoolean(),
            IntStream.range(0, 100).mapToObj(i -> client().prepareIndex("source", "type").setSource("a", "b", "c", "d"))
                .collect(Collectors.toList()));

        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, true)
            .build()).get();

        client().admin().indices().prepareResizeIndex("source", "target").setResizeType(resizeType)
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, resizedShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 2))
                .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", (String) null)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1) // fail resize.
                .build()).get();

        assertBusy(() -> {
            assertPreviousFailed(resizedShards, node);
        });
        logger.info("--> Checking RED status of target");
        assertThat(admin().cluster().prepareHealth("target").get().getStatus(), equalTo(RED));

        client().admin().cluster().prepareReroute().setRetryFailed(true).get();

        // assert busy because the shard will momentarily be initializing.
        assertBusy(() -> {
            assertPreviousFailed(resizedShards, node);
        });

        logger.info("--> Checking RED status of target");
        assertThat(admin().cluster().prepareHealth("target").get().getStatus(), equalTo(RED));

        // fix issue
        logger.info("--> Update settings to field limit 1000");
        client().admin().indices().prepareUpdateSettings("target").setSettings(Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1000).build()).get();

        assertPreviousFailed(resizedShards, node);

        logger.info("--> reroute retry_failed=false");
        client().admin().cluster().prepareReroute().setRetryFailed(false).get();

        assertPreviousFailed(resizedShards, node);

        logger.info("--> reroute retry_failed=true");
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();

        ensureGreen("target");
    }

    private void assertPreviousFailed(int shards, String node) {
        int goodShards = 0;
        for (int i = 0; i < shards; ++i) {
            ClusterAllocationExplainResponse explained =
                client().admin().cluster().prepareAllocationExplain().setIndex("target").setPrimary(true).setShard(i).get();
            AllocateUnassignedDecision allocateDecision = explained.getExplanation().getShardAllocationDecision().getAllocateDecision();
            assertTrue(allocateDecision.isDecisionTaken());
            if (allocateDecision.getAllocationDecision().equals(AllocationDecision.NO)
                && verifyNodeExplanation(explained, node)) {
                // OK, just one is fine.
                goodShards++;
            }
        }

        assertThat("Must see at least one failed resize due to count", goodShards, greaterThan(0));
    }

    private boolean verifyNodeExplanation(ClusterAllocationExplainResponse explained, String node) {
        return explained.getExplanation().getShardAllocationDecision().getAllocateDecision().getNodeDecisions().stream()
            .filter(nar -> nar.getNode().getName().equals(node))
            .filter(nar -> nar.getNodeDecision().equals(AllocationDecision.NO))
            .filter(nar -> nar.getCanAllocateDecision().type().equals(Decision.Type.NO))
            .filter(nar -> nar.getCanAllocateDecision().toString().contains("resize failed on previous attempt"))
            .findFirst().isPresent();
    }
}

