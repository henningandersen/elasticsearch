package org.elasticsearch.xpack.ilm.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.RolloverAction;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 * Autoscaler predicts whether a scale out is necessary by forecasting the current cluster state and checking if allocation deciders
 * will accept it against the cluster configuration.
 */
//todo: a service using generic rollover functionality
public class Autoscaler {
    private final static Logger logger = LogManager.getLogger(Autoscaler.class);
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$"); // copied from rollover
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardsAllocator;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;

    public Autoscaler(AllocationDeciders allocationDeciders, ShardsAllocator shardsAllocator,
                      IndexNameExpressionResolver indexNameExpressionResolver, Settings settings) {
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocator = shardsAllocator;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
    }

    /**
     * Return whether to scale up/out or not for hot layer.
     * @param state current state.
     * @param clusterInfo current cluster info
     * @return true if scale up/out is necessary.
     */
    public boolean scaleHot(ClusterState state, ClusterInfo clusterInfo, Predicate<IndexMetaData> hotPredicate) {
        state = simulateScaleHot(state, clusterInfo);

        MetaData metaData = state.metaData();
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .map(u -> metaData.getIndexSafe(u.index())).anyMatch(hotPredicate);
    }

    // allow test to verify more internal resulting state.
    ClusterState simulateScaleHot(ClusterState state, ClusterInfo clusterInfo) {
        state = futureHotState(state);

        state = allocate(state, clusterInfo, a -> {});
        state = allocate(state, clusterInfo, this::startPrimaryShards);
        return state;
    }

    private ClusterState allocate(ClusterState state, ClusterInfo clusterInfo, Consumer<RoutingAllocation> allocationManipulator) {
        RoutingNodes routingNodes = new RoutingNodes(state, false);
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state, clusterInfo,
            System.nanoTime());

        allocationManipulator.accept(allocation);
        shardsAllocator.allocate(allocation);
        return updateClusterState(state, allocation);
    }

    private ClusterState updateClusterState(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        final MetaData newMetaData = allocation.updateMetaDataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetaData); // validates the routing table is coherent with the cluster state metadata

        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState)
            .routingTable(newRoutingTable)
            .metaData(newMetaData);

        return newStateBuilder.build();
    }

    private void startPrimaryShards(RoutingAllocation allocation) {
        // simulate that all primaries are started so replicas can recover.
        allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).stream()
            .filter(ShardRouting::primary)
            .filter(s -> s.relocatingNodeId() == null)
            .forEach(s -> allocation.routingNodes().startShard(logger, s, allocation.changes()));
    }

    /**
     * Forecast a future cluster state for the hot layer by simulating a rollover of all ILM controlled indices.
     */
    private ClusterState futureHotState(ClusterState state) {
        Set<String> aliases = new HashSet<>();
        for (IndexMetaData imd : state.metaData()) {
            Map<String, String> customData = imd.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
            if (customData != null) {
                String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(imd.getSettings());
                aliases.add(rolloverAlias);
            }
        }

        MetaData.Builder newMetaData = MetaData.builder(state.metaData());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
        aliases.stream().map(a -> simulateRollover(a, state))
            .forEach(index -> {
                newMetaData.put(index, false);
                routingTableBuilder.addAsNew(index);

            });

        return ClusterState.builder(state).metaData(newMetaData).routingTable(routingTableBuilder.build()).build();
    }

    private IndexMetaData simulateRollover(String aliasName, ClusterState currentState) {
        final MetaData metaData = currentState.metaData();
        SortedMap<String, AliasOrIndex> lookup = metaData.getAliasAndIndexLookup();
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) lookup.get(aliasName);
        IndexMetaData indexMetaData = alias.getWriteIndex();
        final String sourceProvidedName = indexMetaData.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            indexMetaData.getIndex().getName());

        String unresolvedName = generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        return createIndexMetaData(rolloverIndexName, currentState);
    }

    private IndexMetaData createIndexMetaData(String indexName, ClusterState currentState) {
        List<IndexTemplateMetaData> templates =
            Collections.unmodifiableList(MetaDataIndexTemplateService.findTemplates(currentState.metaData(), indexName));

        Settings.Builder indexSettingsBuilder = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            indexSettingsBuilder.put(templates.get(i).settings());
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
        }
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
        }
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
        }
        if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        return IndexMetaData.builder(indexName).settings(indexSettingsBuilder).build();
    }

    // copied from rollover
    static String generateRolloverIndexName(String sourceIndexName, IndexNameExpressionResolver indexNameExpressionResolver) {
        String resolvedName = indexNameExpressionResolver.resolveDateMathExpression(sourceIndexName);
        final boolean isDateMath = sourceIndexName.equals(resolvedName) == false;
        if (INDEX_NAME_PATTERN.matcher(resolvedName).matches()) {
            int numberIndex = sourceIndexName.lastIndexOf("-");
            assert numberIndex != -1 : "no separator '-' found";
            int counter = Integer.parseInt(sourceIndexName.substring(numberIndex + 1, isDateMath ? sourceIndexName.length()-1 :
                sourceIndexName.length()));
            String newName = sourceIndexName.substring(0, numberIndex) + "-" + String.format(Locale.ROOT, "%06d", ++counter)
                + (isDateMath ? ">" : "");
            return newName;
        } else {
            throw new IllegalArgumentException("index name [" + sourceIndexName + "] does not match pattern '^.*-\\d+$'");
        }
    }

}
