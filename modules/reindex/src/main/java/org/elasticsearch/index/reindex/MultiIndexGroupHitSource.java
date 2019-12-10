package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A hit source that can handle multiple index groups
 */
public class MultiIndexGroupHitSource {
    private final SearchRequest searchRequest;
    private final List<Set<String>> indexGroups;
    private final ScrollableHitSourceFactory hitSourceFactory;
    private final ScrollableHitSource.Checkpoint groupCheckpoint;
    private final Consumer<ScrollableHitSource.AsyncResponse> onResponse;
    private int currentGroup;

    public interface ScrollableHitSourceFactory {
        ScrollableHitSource create(SearchRequest request, ScrollableHitSource.Checkpoint checkpoint,
                                   Consumer<ScrollableHitSource.AsyncResponse> onResponse);
    }

    public MultiIndexGroupHitSource(List<Set<String>> indexGroups, SearchRequest searchRequest, ScrollableHitSourceFactory hitSourceFactory, Checkpoint checkpoint, Consumer<ScrollableHitSource.AsyncResponse> onResponse) {
        this.searchRequest = searchRequest;
        this.indexGroups = indexGroups;
        this.hitSourceFactory = hitSourceFactory;
        this.groupCheckpoint = checkpoint.groupCheckpoint;
        this.currentGroup = checkpoint.group;
        this.onResponse = onResponse;
    }

    public void start() {
        startGroup(currentGroup, groupCheckpoint);
    }

    private void startGroup(int group, ScrollableHitSource.Checkpoint groupCheckpoint) {
        SearchRequest searchRequest = searchRequestForGroup(indexGroups.get(group));
        ScrollableHitSource hitSource = hitSourceFactory.create(searchRequest, groupCheckpoint, (response) -> {
            if (response.response().getHits().isEmpty()) {
                if (group + 1 < indexGroups.size()) {
                    startGroup(group + 1, null);
                } else {
                    // terminate
                    onResponse.accept(response);
                }
            } else {
                onResponse.accept(response);
            }
        });

        hitSource.start();
    }

    private SearchRequest searchRequestForGroup(Set<String> group) {
        SearchRequest searchRequest = new SearchRequest(this.searchRequest);
        searchRequest.indices(group.toArray(String[]::new));
        return searchRequest;
    }

    public static class Checkpoint implements ToXContent {
        private static final String GROUP = "group";
        private static final String GROUP_CHECKPOINT = "group_checkpoint";

        private static final ConstructingObjectParser<Checkpoint, Void> PARSER =
            new ConstructingObjectParser<>("reindex/multicheckpoint", a -> new Checkpoint((int) a[0],
                (ScrollableHitSource.Checkpoint) a[1]));

        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(GROUP));
            PARSER.declareObject(ConstructingObjectParser.constructorArg(),
                (p,c) -> ScrollableHitSource.Checkpoint.fromXContent(p), new ParseField(GROUP_CHECKPOINT));
        }

        private int group;
        private ScrollableHitSource.Checkpoint groupCheckpoint;

        public Checkpoint(int group, ScrollableHitSource.Checkpoint groupCheckpoint) {
            this.group = group;
            this.groupCheckpoint = groupCheckpoint;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(GROUP, group);
            builder.field(GROUP_CHECKPOINT, groupCheckpoint);
            return null;
        }

        public static Checkpoint fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }
}
