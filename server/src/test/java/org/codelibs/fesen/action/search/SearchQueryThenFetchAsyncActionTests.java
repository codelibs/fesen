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

package org.codelibs.fesen.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.search.QueryPhaseResultConsumer;
import org.codelibs.fesen.action.search.SearchActionListener;
import org.codelibs.fesen.action.search.SearchPhase;
import org.codelibs.fesen.action.search.SearchPhaseContext;
import org.codelibs.fesen.action.search.SearchPhaseController;
import org.codelibs.fesen.action.search.SearchPhaseResults;
import org.codelibs.fesen.action.search.SearchQueryThenFetchAsyncAction;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.action.search.SearchShardIterator;
import org.codelibs.fesen.action.search.SearchTask;
import org.codelibs.fesen.action.search.SearchTransportService;
import org.codelibs.fesen.action.search.TransportSearchAction;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.routing.GroupShardsIterator;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.breaker.CircuitBreaker;
import org.codelibs.fesen.common.breaker.NoopCircuitBreaker;
import org.codelibs.fesen.common.lucene.search.TopDocsAndMaxScore;
import org.codelibs.fesen.common.util.concurrent.EsExecutors;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.search.DocValueFormat;
import org.codelibs.fesen.search.SearchPhaseResult;
import org.codelibs.fesen.search.SearchShardTarget;
import org.codelibs.fesen.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.search.collapse.CollapseBuilder;
import org.codelibs.fesen.search.internal.AliasFilter;
import org.codelibs.fesen.search.internal.ShardSearchContextId;
import org.codelibs.fesen.search.internal.ShardSearchRequest;
import org.codelibs.fesen.search.query.QuerySearchResult;
import org.codelibs.fesen.search.sort.SortBuilders;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.InternalAggregationTestCase;
import org.codelibs.fesen.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchQueryThenFetchAsyncActionTests extends ESTestCase {
    public void testBottomFieldSort() throws Exception {
        testCase(false, false);
    }

    public void testScrollDisableBottomFieldSort() throws Exception {
        testCase(true, false);
    }

    public void testCollapseDisableBottomFieldSort() throws Exception {
        testCase(false, true);
    }

    private void testCase(boolean withScroll, boolean withCollapse) throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider =
                new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        int numShards = randomIntBetween(10, 20);
        int numConcurrent = randomIntBetween(1, 4);
        AtomicInteger numWithTopDocs = new AtomicInteger();
        AtomicInteger successfulOps = new AtomicInteger();
        AtomicBoolean canReturnNullResponse = new AtomicBoolean(false);
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteQuery(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                    SearchActionListener<SearchPhaseResult> listener) {
                int shardId = request.shardId().id();
                if (request.canReturnNullResponseIfMatchNoDocs()) {
                    canReturnNullResponse.set(true);
                }
                if (request.getBottomSortValues() != null) {
                    assertNotEquals(shardId, (int) request.getBottomSortValues().getFormattedSortValues()[0]);
                    numWithTopDocs.incrementAndGet();
                }
                QuerySearchResult queryResult = new QuerySearchResult(new ShardSearchContextId("N/A", 123),
                        new SearchShardTarget("node1", new ShardId("idx", "na", shardId), null, OriginalIndices.NONE), null);
                SortField sortField = new SortField("timestamp", SortField.Type.LONG);
                if (withCollapse) {
                    queryResult.topDocs(
                            new TopDocsAndMaxScore(new CollapseTopFieldDocs("collapse_field",
                                    new TotalHits(1,
                                            withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                    new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                                    new SortField[] { sortField }, new Object[] { 0L }), Float.NaN),
                            new DocValueFormat[] { DocValueFormat.RAW });
                } else {
                    queryResult.topDocs(new TopDocsAndMaxScore(new TopFieldDocs(
                            new TotalHits(1, withScroll ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                            new FieldDoc[] { new FieldDoc(randomInt(1000), Float.NaN, new Object[] { request.shardId().id() }) },
                            new SortField[] { sortField }), Float.NaN), new DocValueFormat[] { DocValueFormat.RAW });
                }
                queryResult.from(0);
                queryResult.size(1);
                successfulOps.incrementAndGet();
                new Thread(() -> listener.onResponse(queryResult)).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
                new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS), numShards, randomBoolean(), primaryNode,
                replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        if (withScroll) {
            searchRequest.scroll(TimeValue.timeValueMillis(100));
        } else {
            searchRequest.source().trackTotalHitsUpTo(2);
        }
        if (withCollapse) {
            searchRequest.source().collapse(new CollapseBuilder("collapse_field"));
        }
        searchRequest.allowPartialSearchResults(false);
        Executor executor = EsExecutors.newDirectExecutorService();
        SearchPhaseController controller =
                new SearchPhaseController(writableRegistry(), r -> InternalAggregationTestCase.emptyReduceContextBuilder());
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        QueryPhaseResultConsumer resultConsumer =
                new QueryPhaseResultConsumer(searchRequest, executor, new NoopCircuitBreaker(CircuitBreaker.REQUEST), controller,
                        task.getProgressListener(), writableRegistry(), shardsIter.size(), exc -> {});
        SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(logger, searchTransportService,
                (clusterAlias, node) -> lookup.get(node), Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(), Collections.emptyMap(), controller, executor, resultConsumer, searchRequest, null, shardsIter,
                timeProvider, null, task, SearchResponse.Clusters.EMPTY) {
            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        latch.countDown();
                    }
                };
            }
        };
        action.start();
        latch.await();
        assertThat(successfulOps.get(), equalTo(numShards));
        if (withScroll) {
            assertFalse(canReturnNullResponse.get());
            assertThat(numWithTopDocs.get(), equalTo(0));
        } else {
            assertTrue(canReturnNullResponse.get());
            if (withCollapse) {
                assertThat(numWithTopDocs.get(), equalTo(0));
            } else {
                assertThat(numWithTopDocs.get(), greaterThanOrEqualTo(1));
            }
        }
        SearchPhaseController.ReducedQueryPhase phase = action.results.reduce();
        assertThat(phase.numReducePhases, greaterThanOrEqualTo(1));
        if (withScroll) {
            assertThat(phase.totalHits.value, equalTo((long) numShards));
            assertThat(phase.totalHits.relation, equalTo(TotalHits.Relation.EQUAL_TO));
        } else {
            assertThat(phase.totalHits.value, equalTo(2L));
            assertThat(phase.totalHits.relation, equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        }
        assertThat(phase.sortedTopDocs.scoreDocs.length, equalTo(1));
        assertThat(phase.sortedTopDocs.scoreDocs[0], instanceOf(FieldDoc.class));
        assertThat(((FieldDoc) phase.sortedTopDocs.scoreDocs[0]).fields.length, equalTo(1));
        assertThat(((FieldDoc) phase.sortedTopDocs.scoreDocs[0]).fields[0], equalTo(0));
    }
}
