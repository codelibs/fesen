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

package org.codelibs.fesen.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.search.SearchType;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.common.UUIDs;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.unit.TimeValue;
import org.codelibs.fesen.common.util.BigArrays;
import org.codelibs.fesen.common.util.MockBigArrays;
import org.codelibs.fesen.common.util.MockPageCacheRecycler;
import org.codelibs.fesen.index.IndexService;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.cache.IndexCache;
import org.codelibs.fesen.index.cache.query.QueryCache;
import org.codelibs.fesen.index.engine.Engine;
import org.codelibs.fesen.index.mapper.MappedFieldType;
import org.codelibs.fesen.index.mapper.MapperService;
import org.codelibs.fesen.index.query.AbstractQueryBuilder;
import org.codelibs.fesen.index.query.ParsedQuery;
import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.indices.breaker.NoneCircuitBreakerService;
import org.codelibs.fesen.search.DefaultSearchContext;
import org.codelibs.fesen.search.DocValueFormat;
import org.codelibs.fesen.search.Scroll;
import org.codelibs.fesen.search.SearchShardTarget;
import org.codelibs.fesen.search.internal.AliasFilter;
import org.codelibs.fesen.search.internal.LegacyReaderContext;
import org.codelibs.fesen.search.internal.ReaderContext;
import org.codelibs.fesen.search.internal.ShardSearchContextId;
import org.codelibs.fesen.search.internal.ShardSearchRequest;
import org.codelibs.fesen.search.rescore.RescoreContext;
import org.codelibs.fesen.search.slice.SliceBuilder;
import org.codelibs.fesen.search.sort.SortAndFormats;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSearchContextTests extends ESTestCase {

    public void testPreProcess() throws Exception {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);
        when(shardSearchRequest.types()).thenReturn(new String[]{});

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        int maxResultWindow = randomIntBetween(50, 100);
        int maxRescoreWindow = randomIntBetween(50, 100);
        int maxSlicesPerScroll = randomIntBetween(50, 100);
        Settings settings = Settings.builder()
            .put("index.max_result_window", maxResultWindow)
            .put("index.max_slices_per_scroll", maxSlicesPerScroll)
            .put("index.max_rescore_window", maxRescoreWindow)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();

        IndexService indexService = mock(IndexService.class);
        IndexCache indexCache = mock(IndexCache.class);
        QueryCache queryCache = mock(QueryCache.class);
        when(indexCache.query()).thenReturn(queryCache);
        when(indexService.cache()).thenReturn(indexCache);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), anyObject(), anyObject(), anyString())).thenReturn(queryShardContext);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(randomBoolean());
        when(indexService.mapperService()).thenReturn(mapperService);

        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {


            final Supplier<Engine.SearcherSupplier> searcherSupplier = () -> new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {
                }

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        IndexReader reader = w.getReader();
                        return new Engine.Searcher("test", reader, IndexSearcher.getDefaultSimilarity(),
                            IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), reader);
                    } catch (IOException exc) {
                        throw new AssertionError(exc);
                    }
                }
            };

            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerWithoutScroll = new ReaderContext(
                newContextId(), indexService, indexShard, searcherSupplier.get(), randomNonNegativeLong(), false);

            DefaultSearchContext contextWithoutScroll = new DefaultSearchContext(readerWithoutScroll, shardSearchRequest, target, null,
                bigArrays, null, timeout, null, false, Version.CURRENT);
            contextWithoutScroll.from(300);
            contextWithoutScroll.close();

            // resultWindow greater than maxResultWindow and scrollContext is null
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> contextWithoutScroll.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Result window is too large, from + size must be less than or equal to:"
                + " [" + maxResultWindow + "] but was [310]. See the scroll api for a more efficient way to request large data sets. "
                + "This limit can be set by changing the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                + "] index level setting."));

            // resultWindow greater than maxResultWindow and scrollContext isn't null
            when(shardSearchRequest.scroll()).thenReturn(new Scroll(TimeValue.timeValueMillis(randomInt(1000))));
            ReaderContext readerContext = new LegacyReaderContext(
                newContextId(), indexService, indexShard, searcherSupplier.get(), shardSearchRequest, randomNonNegativeLong());
            DefaultSearchContext context1 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null,
                bigArrays, null, timeout, null, false, Version.CURRENT);
            context1.from(300);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Batch size is too large, size must be less than or equal to: ["
                + maxResultWindow + "] but was [310]. Scroll batch sizes cost as much memory as result windows so they are "
                + "controlled by the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey() + "] index level setting."));

            // resultWindow not greater than maxResultWindow and both rescore and sort are not null
            context1.from(0);
            DocValueFormat docValueFormat = mock(DocValueFormat.class);
            SortAndFormats sortAndFormats = new SortAndFormats(new Sort(), new DocValueFormat[]{docValueFormat});
            context1.sort(sortAndFormats);

            RescoreContext rescoreContext = mock(RescoreContext.class);
            when(rescoreContext.getWindowSize()).thenReturn(500);
            context1.addRescore(rescoreContext);

            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));
            assertThat(exception.getMessage(), equalTo("Cannot use [sort] option in conjunction with [rescore]."));

            // rescore is null but sort is not null and rescoreContext.getWindowSize() exceeds maxResultWindow
            context1.sort(null);
            exception = expectThrows(IllegalArgumentException.class, () -> context1.preProcess(false));

            assertThat(exception.getMessage(), equalTo("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
                + "It must be less than [" + maxRescoreWindow + "]. This prevents allocating massive heaps for storing the results "
                + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                + "] index level setting."));

            readerContext.close();
            readerContext = new ReaderContext(
                newContextId(), indexService, indexShard, searcherSupplier.get(), randomNonNegativeLong(), false);
            // rescore is null but sliceBuilder is not null
            DefaultSearchContext context2 = new DefaultSearchContext(readerContext, shardSearchRequest, target,
                null, bigArrays, null, timeout, null, false, Version.CURRENT);

            SliceBuilder sliceBuilder = mock(SliceBuilder.class);
            int numSlices = maxSlicesPerScroll + randomIntBetween(1, 100);
            when(sliceBuilder.getMax()).thenReturn(numSlices);
            context2.sliceBuilder(sliceBuilder);

            exception = expectThrows(IllegalArgumentException.class, () -> context2.preProcess(false));
            assertThat(exception.getMessage(), equalTo("The number of slices [" + numSlices + "] is too large. It must "
                + "be less than [" + maxSlicesPerScroll + "]. This limit can be set by changing the [" +
                IndexSettings.MAX_SLICES_PER_SCROLL.getKey() + "] index level setting."));

            // No exceptions should be thrown
            when(shardSearchRequest.getAliasFilter()).thenReturn(AliasFilter.EMPTY);
            when(shardSearchRequest.indexBoost()).thenReturn(AbstractQueryBuilder.DEFAULT_BOOST);

            DefaultSearchContext context3 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null,
                bigArrays, null, timeout, null, false, Version.CURRENT);
            ParsedQuery parsedQuery = ParsedQuery.parsedMatchAllQuery();
            context3.sliceBuilder(null).parsedQuery(parsedQuery).preProcess(false);
            assertEquals(context3.query(), context3.buildFilteredQuery(parsedQuery.query()));

            when(queryShardContext.getIndexSettings()).thenReturn(indexSettings);
            when(queryShardContext.fieldMapper(anyString())).thenReturn(mock(MappedFieldType.class));
            when(shardSearchRequest.indexRoutings()).thenReturn(new String[0]);

            readerContext.close();
            readerContext = new ReaderContext(newContextId(), indexService, indexShard,
                searcherSupplier.get(), randomNonNegativeLong(), false);
            DefaultSearchContext context4 = new DefaultSearchContext(readerContext, shardSearchRequest, target, null, bigArrays, null,
                timeout, null, false, Version.CURRENT);
            context4.sliceBuilder(new SliceBuilder(1,2)).parsedQuery(parsedQuery).preProcess(false);
            Query query1 = context4.query();
            context4.sliceBuilder(new SliceBuilder(0,2)).parsedQuery(parsedQuery).preProcess(false);
            Query query2 = context4.query();
            assertTrue(query1 instanceof MatchNoDocsQuery || query2 instanceof MatchNoDocsQuery);

            readerContext.close();
            threadPool.shutdown();
        }
    }

    public void testClearQueryCancellationsOnClose() throws IOException {
        TimeValue timeout = new TimeValue(randomIntBetween(1, 100));
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.searchType()).thenReturn(SearchType.DEFAULT);
        ShardId shardId = new ShardId("index", UUID.randomUUID().toString(), 1);
        when(shardSearchRequest.shardId()).thenReturn(shardId);

        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = mock(IndexShard.class);
        QueryCachingPolicy queryCachingPolicy = mock(QueryCachingPolicy.class);
        when(indexShard.getQueryCachingPolicy()).thenReturn(queryCachingPolicy);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        IndexService indexService = mock(IndexService.class);
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(indexService.newQueryShardContext(eq(shardId.id()), anyObject(), anyObject(), anyString())).thenReturn(queryShardContext);

        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir);
             IndexReader reader = w.getReader();
             Engine.Searcher searcher = new Engine.Searcher("test", reader,
                 IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(),
                 IndexSearcher.getDefaultQueryCachingPolicy(), reader)) {

            Engine.SearcherSupplier searcherSupplier = new Engine.SearcherSupplier(Function.identity()) {
                @Override
                protected void doClose() {

                }

                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return searcher;
                }
            };
            SearchShardTarget target = new SearchShardTarget("node", shardId, null, OriginalIndices.NONE);
            ReaderContext readerContext = new ReaderContext(
                newContextId(), indexService, indexShard, searcherSupplier, randomNonNegativeLong(), false);

            DefaultSearchContext context = new DefaultSearchContext(
                readerContext, shardSearchRequest, target, null, bigArrays, null, timeout, null, false, Version.CURRENT);
            assertThat(context.searcher().hasCancellations(), is(false));
            context.searcher().addQueryCancellation(() -> {});
            assertThat(context.searcher().hasCancellations(), is(true));

            context.close();
            assertThat(context.searcher().hasCancellations(), is(false));

        } finally {
            threadPool.shutdown();
        }
    }

    private ShardSearchContextId newContextId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }
}
