package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class MetaDataRolloverServiceTests extends ESTestCase {

    public void testRolloverAliasActions() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);

        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, false, null);
        assertThat(actions, hasSize(2));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Add) action).getAlias());
                foundAdd = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Remove) action).getAlias());
                foundRemove = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testRolloverAliasActionsWithExplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, true, null);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            AliasAction.Add addAction = (AliasAction.Add) action;
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertTrue(addAction.writeIndex());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertFalse(addAction.writeIndex());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testRolloverAliasActionsWithHiddenAliasAndExplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, true, true);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            assertThat(action, instanceOf(AliasAction.Add.class));
            AliasAction.Add addAction = (AliasAction.Add) action;
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertTrue(addAction.writeIndex());
                assertTrue(addAction.isHidden());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertFalse(addAction.writeIndex());
                assertTrue(addAction.isHidden());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testRolloverAliasActionsWithHiddenAliasAndImplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, targetIndex);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, rolloverRequest, false, true);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            if (action.getIndex().equals(targetIndex)) {
                assertThat(action, instanceOf(AliasAction.Add.class));
                AliasAction.Add addAction = (AliasAction.Add) action;
                assertEquals(sourceAlias, addAction.getAlias());
                assertThat(addAction.writeIndex(), nullValue());
                assertTrue(addAction.isHidden());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertThat(action, instanceOf(AliasAction.Remove.class));
                AliasAction.Remove removeAction = (AliasAction.Remove) action;
                assertEquals(sourceAlias, removeAction.getAlias());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testValidation() {
        String index1 = randomAlphaOfLength(10);
        String aliasWithWriteIndex = randomAlphaOfLength(10);
        String index2 = randomAlphaOfLength(10);
        String aliasWithNoWriteIndex = randomAlphaOfLength(10);
        Boolean firstIsWriteIndex = randomFrom(false, null);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        MetaData.Builder metaDataBuilder = MetaData.builder()
            .put(IndexMetaData.builder(index1)
                .settings(settings)
                .putAlias(AliasMetaData.builder(aliasWithWriteIndex))
                .putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(firstIsWriteIndex))
            );
        IndexMetaData.Builder indexTwoBuilder = IndexMetaData.builder(index2).settings(settings);
        if (firstIsWriteIndex == null) {
            indexTwoBuilder.putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(randomFrom(false, null)));
        }
        metaDataBuilder.put(indexTwoBuilder);
        MetaData metaData = metaDataBuilder.build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, new RolloverRequest(aliasWithNoWriteIndex,
                randomAlphaOfLength(10))));
        assertThat(exception.getMessage(), equalTo("source alias [" + aliasWithNoWriteIndex + "] does not point to a write index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, new RolloverRequest(randomFrom(index1, index2),
                randomAlphaOfLength(10))));
        assertThat(exception.getMessage(), equalTo("source alias is a concrete index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, new RolloverRequest(randomAlphaOfLength(5),
                randomAlphaOfLength(10)))
        );
        assertThat(exception.getMessage(), equalTo("source alias does not exist"));
        MetaDataRolloverService.validate(metaData, new RolloverRequest(aliasWithWriteIndex, randomAlphaOfLength(10)));
    }

    public void testGenerateRolloverIndexName() {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.generateRolloverIndexName(invalidIndexName, indexNameExpressionResolver));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAlphaOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(MetaDataRolloverService.generateRolloverIndexName(indexEndingInNumbers, indexNameExpressionResolver),
            equalTo(indexPrefix + "-" + String.format(Locale.ROOT, "%06d", num + 1)));
        assertThat(MetaDataRolloverService.generateRolloverIndexName("index-name-1", indexNameExpressionResolver),
            equalTo("index-name-000002"));
        assertThat(MetaDataRolloverService.generateRolloverIndexName("index-name-2", indexNameExpressionResolver),
            equalTo("index-name-000003"));
        assertEquals( "<index-name-{now/d}-000002>", MetaDataRolloverService.generateRolloverIndexName("<index-name-{now/d}-1>",
            indexNameExpressionResolver));
    }

    public void testCreateIndexRequest() {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest =
            MetaDataRolloverService.prepareCreateIndexRequest(rolloverIndex, rolloverIndex, rolloverRequest);
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }

    public void testRejectDuplicateAlias() {
        final IndexTemplateMetaData template = IndexTemplateMetaData.builder("test-template")
            .patterns(Arrays.asList("foo-*", "bar-*"))
            .putAlias(AliasMetaData.builder("foo-write")).putAlias(AliasMetaData.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final MetaData metaData = MetaData.builder().put(createMetaData(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, randomBoolean()));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedTemplates() {
        final IndexTemplateMetaData template = IndexTemplateMetaData.builder("test-template")
            .patterns(Collections.singletonList("*"))
            .putAlias(AliasMetaData.builder("foo-write")).putAlias(AliasMetaData.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final MetaData metaData = MetaData.builder().put(createMetaData(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, randomFrom(Boolean.FALSE, null)));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    private static IndexMetaData createMetaData(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetaData.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }
}
