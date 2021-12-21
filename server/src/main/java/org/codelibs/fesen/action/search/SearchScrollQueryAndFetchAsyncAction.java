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

import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.util.concurrent.AtomicArray;
import org.codelibs.fesen.search.fetch.QueryFetchSearchResult;
import org.codelibs.fesen.search.fetch.ScrollQueryFetchSearchResult;
import org.codelibs.fesen.search.internal.InternalScrollSearchRequest;
import org.codelibs.fesen.transport.Transport;

import java.util.function.BiFunction;

final class SearchScrollQueryAndFetchAsyncAction extends SearchScrollAsyncAction<ScrollQueryFetchSearchResult> {

    private final SearchTask task;
    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

    SearchScrollQueryAndFetchAsyncAction(Logger logger, ClusterService clusterService, SearchTransportService searchTransportService,
                                         SearchPhaseController searchPhaseController, SearchScrollRequest request, SearchTask task,
                                         ParsedScrollId scrollId, ActionListener<SearchResponse> listener) {
        super(scrollId, logger, clusterService.state().nodes(), listener, searchPhaseController, request, searchTransportService);
        this.task = task;
        this.queryFetchResults = new AtomicArray<>(scrollId.getContext().length);
    }

    @Override
    protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                       SearchActionListener<ScrollQueryFetchSearchResult> searchActionListener) {
        searchTransportService.sendExecuteScrollFetch(connection, internalRequest, task, searchActionListener);
    }

    @Override
    protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
        return sendResponsePhase(searchPhaseController.reducedScrollQueryPhase(queryFetchResults.asList()), queryFetchResults);
    }

    @Override
    protected void onFirstPhaseResult(int shardId, ScrollQueryFetchSearchResult result) {
        queryFetchResults.setOnce(shardId, result.result());
    }
}
