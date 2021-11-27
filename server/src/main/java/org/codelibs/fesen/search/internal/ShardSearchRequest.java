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

package org.codelibs.fesen.search.internal;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.IndicesRequest;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.action.search.SearchShardTask;
import org.codelibs.fesen.action.search.SearchType;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.cluster.metadata.AliasMetadata;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.collect.ImmutableOpenMap;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.core.CheckedFunction;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.index.query.BoolQueryBuilder;
import org.codelibs.fesen.index.query.MatchNoneQueryBuilder;
import org.codelibs.fesen.index.query.QueryBuilder;
import org.codelibs.fesen.index.query.QueryRewriteContext;
import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.index.query.Rewriteable;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.indices.AliasFilterParsingException;
import org.codelibs.fesen.indices.InvalidAliasNameException;
import org.codelibs.fesen.search.Scroll;
import org.codelibs.fesen.search.SearchSortValuesAndFormats;
import org.codelibs.fesen.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.search.query.QuerySearchResult;
import org.codelibs.fesen.search.sort.FieldSortBuilder;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.transport.TransportRequest;

import static org.codelibs.fesen.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Shard level request that represents a search.
 * It provides all the methods that the {@link SearchContext} needs.
 * Provides a cache key based on its content that can be used to cache shard level response.
 */
public class ShardSearchRequest extends TransportRequest implements IndicesRequest {
    private final String clusterAlias;
    private final ShardId shardId;
    private final int numberOfShards;
    private final SearchType searchType;
    private final Scroll scroll;
    private final String[] types;
    private final float indexBoost;
    private final Boolean requestCache;
    private final long nowInMillis;
    private final boolean allowPartialSearchResults;
    private final String[] indexRoutings;
    private final String preference;
    private final OriginalIndices originalIndices;

    private boolean canReturnNullResponseIfMatchNoDocs;
    private SearchSortValuesAndFormats bottomSortValues;

    //these are the only mutable fields, as they are subject to rewriting
    private AliasFilter aliasFilter;
    private SearchSourceBuilder source;
    private final ShardSearchContextId readerId;
    private final TimeValue keepAlive;

    public ShardSearchRequest(OriginalIndices originalIndices,
                              SearchRequest searchRequest,
                              ShardId shardId,
                              int numberOfShards,
                              AliasFilter aliasFilter,
                              float indexBoost,
                              long nowInMillis,
                              @Nullable String clusterAlias,
                              String[] indexRoutings) {
        this(originalIndices, searchRequest, shardId, numberOfShards, aliasFilter,
            indexBoost, nowInMillis, clusterAlias, indexRoutings, null, null);
    }

    public ShardSearchRequest(OriginalIndices originalIndices,
                              SearchRequest searchRequest,
                              ShardId shardId,
                              int numberOfShards,
                              AliasFilter aliasFilter,
                              float indexBoost,
                              long nowInMillis,
                              @Nullable String clusterAlias,
                              String[] indexRoutings,
                              ShardSearchContextId readerId,
                              TimeValue keepAlive) {
        this(originalIndices,
            shardId,
            numberOfShards,
            searchRequest.searchType(),
            searchRequest.source(),
            searchRequest.types(),
            searchRequest.requestCache(),
            aliasFilter,
            indexBoost,
            searchRequest.allowPartialSearchResults(),
            indexRoutings,
            searchRequest.preference(),
            searchRequest.scroll(),
            nowInMillis,
            clusterAlias,
            readerId,
            keepAlive);
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
    }

    public ShardSearchRequest(ShardId shardId,
                              String[] types,
                              long nowInMillis,
                              AliasFilter aliasFilter) {
        this(OriginalIndices.NONE, shardId, -1, SearchType.QUERY_THEN_FETCH, null, types,
            null, aliasFilter, 1.0f, false, Strings.EMPTY_ARRAY, null, null, nowInMillis, null, null, null);
    }

    private ShardSearchRequest(OriginalIndices originalIndices,
                               ShardId shardId,
                               int numberOfShards,
                               SearchType searchType,
                               SearchSourceBuilder source,
                               String[] types,
                               Boolean requestCache,
                               AliasFilter aliasFilter,
                               float indexBoost,
                               boolean allowPartialSearchResults,
                               String[] indexRoutings,
                               String preference,
                               Scroll scroll,
                               long nowInMillis,
                               @Nullable String clusterAlias,
                               ShardSearchContextId readerId,
                               TimeValue keepAlive) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.types = types;
        this.requestCache = requestCache;
        this.aliasFilter = aliasFilter;
        this.indexBoost = indexBoost;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.indexRoutings = indexRoutings;
        this.preference = preference;
        this.scroll = scroll;
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
        this.originalIndices = originalIndices;
        this.readerId = readerId;
        this.keepAlive = keepAlive;
        assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
    }

    public ShardSearchRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        types = in.readStringArray();
        aliasFilter = new AliasFilter(in);
        indexBoost = in.readFloat();
        nowInMillis = in.readVLong();
        requestCache = in.readOptionalBoolean();
        clusterAlias = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            allowPartialSearchResults = in.readBoolean();
        } else {
            allowPartialSearchResults = in.readOptionalBoolean();
        }
        indexRoutings = in.readStringArray();
        preference = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            canReturnNullResponseIfMatchNoDocs = in.readBoolean();
            bottomSortValues = in.readOptionalWriteable(SearchSortValuesAndFormats::new);
        } else {
            canReturnNullResponseIfMatchNoDocs = false;
            bottomSortValues = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            this.readerId = in.readOptionalWriteable(ShardSearchContextId::new);
            this.keepAlive = in.readOptionalTimeValue();
        } else {
            this.readerId = null;
            this.keepAlive = null;
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
        assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
    }

    public ShardSearchRequest(ShardSearchRequest clone) {
        this.shardId = clone.shardId;
        this.searchType = clone.searchType;
        this.numberOfShards = clone.numberOfShards;
        this.scroll = clone.scroll;
        this.source = clone.source;
        this.types = clone.types;
        this.aliasFilter = clone.aliasFilter;
        this.indexBoost = clone.indexBoost;
        this.nowInMillis = clone.nowInMillis;
        this.requestCache = clone.requestCache;
        this.clusterAlias = clone.clusterAlias;
        this.allowPartialSearchResults = clone.allowPartialSearchResults;
        this.indexRoutings = clone.indexRoutings;
        this.preference = clone.preference;
        this.canReturnNullResponseIfMatchNoDocs = clone.canReturnNullResponseIfMatchNoDocs;
        this.bottomSortValues = clone.bottomSortValues;
        this.originalIndices = clone.originalIndices;
        this.readerId = clone.readerId;
        this.keepAlive = clone.keepAlive;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        innerWriteTo(out, false);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    protected final void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (!asKey) {
            out.writeVInt(numberOfShards);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        out.writeStringArray(types);
        aliasFilter.writeTo(out);
        out.writeFloat(indexBoost);
        if (asKey == false) {
            out.writeVLong(nowInMillis);
        }
        out.writeOptionalBoolean(requestCache);
        out.writeOptionalString(clusterAlias);
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeBoolean(allowPartialSearchResults);
        } else {
            out.writeOptionalBoolean(allowPartialSearchResults);
        }
        if (asKey == false) {
            out.writeStringArray(indexRoutings);
            out.writeOptionalString(preference);
        }
        if (out.getVersion().onOrAfter(Version.V_7_7_0) && asKey == false) {
            out.writeBoolean(canReturnNullResponseIfMatchNoDocs);
            out.writeOptionalWriteable(bottomSortValues);
        }
        if (out.getVersion().onOrAfter(Version.V_7_10_0) && asKey == false) {
            out.writeOptionalWriteable(readerId);
            out.writeOptionalTimeValue(keepAlive);
        }
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    public ShardId shardId() {
        return shardId;
    }

    public String[] types() {
        return types;
    }

    public SearchSourceBuilder source() {
        return source;
    }

    public AliasFilter getAliasFilter() {
        return aliasFilter;
    }

    public void setAliasFilter(AliasFilter aliasFilter) {
        this.aliasFilter = aliasFilter;
    }

    public void source(SearchSourceBuilder source) {
        this.source = source;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public SearchType searchType() {
        return searchType;
    }

    public float indexBoost() {
        return indexBoost;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    public Boolean requestCache() {
        return requestCache;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public Scroll scroll() {
        return scroll;
    }

    public String[] indexRoutings() {
        return indexRoutings;
    }

    public String preference() {
        return preference;
    }

    /**
     * Sets the bottom sort values that can be used by the searcher to filter documents
     * that are after it. This value is computed by coordinating nodes that throttles the
     * query phase. After a partial merge of successful shards the sort values of the
     * bottom top document are passed as an hint on subsequent shard requests.
     */
    public void setBottomSortValues(SearchSortValuesAndFormats values) {
        this.bottomSortValues = values;
    }

    public SearchSortValuesAndFormats getBottomSortValues() {
        return bottomSortValues;
    }

    /**
     * Returns true if the caller can handle null response {@link QuerySearchResult#nullInstance()}.
     * Defaults to false since the coordinator node needs at least one shard response to build the global
     * response.
     */
    public boolean canReturnNullResponseIfMatchNoDocs() {
        return canReturnNullResponseIfMatchNoDocs;
    }

    public void canReturnNullResponseIfMatchNoDocs(boolean value) {
        this.canReturnNullResponseIfMatchNoDocs = value;
    }

    private static final ThreadLocal<BytesStreamOutput> scratch = ThreadLocal.withInitial(BytesStreamOutput::new);

    /**
     * Returns a non-null value if this request should execute using a specific point-in-time reader;
     * otherwise, using the most up to date point-in-time reader.
     */
    public ShardSearchContextId readerId() {
        return readerId;
    }

    /**
     * Returns a non-null to specify the time to live of the point-in-time reader that is used to execute this request.
     */
    public TimeValue keepAlive() {
        return keepAlive;
    }

    /**
     * Returns the cache key for this shard search request, based on its content
     */
    public BytesReference cacheKey() throws IOException {
        BytesStreamOutput out = scratch.get();
        try {
            this.innerWriteTo(out, true);
            // copy it over since we don't want to share the thread-local bytes in #scratch
            return out.copyBytes();
        } finally {
            out.reset();
        }
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardId[" + shardId() + "]";
    }

    public Rewriteable<Rewriteable> getRewriteable() {
        return new RequestRewritable(this);
    }

    static class RequestRewritable implements Rewriteable<Rewriteable> {

        final ShardSearchRequest request;

        RequestRewritable(ShardSearchRequest request) {
            this.request = request;
        }

        @Override
        public Rewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            SearchSourceBuilder newSource = request.source() == null ? null : Rewriteable.rewrite(request.source(), ctx);
            AliasFilter newAliasFilter = Rewriteable.rewrite(request.getAliasFilter(), ctx);

            QueryShardContext shardContext = ctx.convertToShardContext();

            FieldSortBuilder primarySort = FieldSortBuilder.getPrimaryFieldSortOrNull(newSource);
            if (shardContext != null
                    && primarySort != null
                    && primarySort.isBottomSortShardDisjoint(shardContext, request.getBottomSortValues())) {
                assert newSource != null : "source should contain a primary sort field";
                newSource = newSource.shallowCopy();
                int trackTotalHitsUpTo = SearchRequest.resolveTrackTotalHitsUpTo(request.scroll, request.source);
                if (trackTotalHitsUpTo == TRACK_TOTAL_HITS_DISABLED
                        && newSource.suggest() == null
                        && newSource.aggregations() == null) {
                    newSource.query(new MatchNoneQueryBuilder());
                } else {
                    newSource.size(0);
                }
                request.source(newSource);
                request.setBottomSortValues(null);
            }

            if (newSource == request.source() && newAliasFilter == request.getAliasFilter()) {
                return this;
            } else {
                request.source(newSource);
                request.setAliasFilter(newAliasFilter);
                return new RequestRewritable(request);
            }
        }
    }

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p>
     * The list of filtering aliases should be obtained by calling Metadata.filteringAliases.
     * Returns {@code null} if no filtering is required.</p>
     */
    public static QueryBuilder parseAliasFilter(CheckedFunction<BytesReference, QueryBuilder, IOException> filterParser,
                                                IndexMetadata metadata, String... aliasNames) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        Index index = metadata.getIndex();
        ImmutableOpenMap<String, AliasMetadata> aliases = metadata.getAliases();
        Function<AliasMetadata, QueryBuilder> parserFunction = (alias) -> {
            if (alias.filter() == null) {
                return null;
            }
            try {
                return filterParser.apply(alias.filter().uncompressed());
            } catch (IOException ex) {
                throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
            }
        };
        if (aliasNames.length == 1) {
            AliasMetadata alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parserFunction.apply(alias);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BoolQueryBuilder combined = new BoolQueryBuilder();
            for (String aliasName : aliasNames) {
                AliasMetadata alias = aliases.get(aliasName);
                if (alias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(index, aliasNames[0],
                        "Unknown alias name was passed to alias Filter");
                }
                QueryBuilder parsedFilter = parserFunction.apply(alias);
                if (parsedFilter != null) {
                    combined.should(parsedFilter);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return combined;
        }
    }
}
