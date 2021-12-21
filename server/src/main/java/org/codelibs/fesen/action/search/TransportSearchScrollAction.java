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

import static org.codelibs.fesen.action.search.ParsedScrollId.QUERY_AND_FETCH_TYPE;
import static org.codelibs.fesen.action.search.ParsedScrollId.QUERY_THEN_FETCH_TYPE;
import static org.codelibs.fesen.action.search.TransportSearchHelper.parseScrollId;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.transport.TransportService;

public class TransportSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final SearchPhaseController searchPhaseController;

    @Inject
    public TransportSearchScrollAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                       SearchTransportService searchTransportService, SearchPhaseController searchPhaseController) {
        super(SearchScrollAction.NAME, transportService, actionFilters,
            (Writeable.Reader<SearchScrollRequest>) SearchScrollRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected void doExecute(Task task, SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        try {
            ParsedScrollId scrollId = parseScrollId(request.scrollId());
            Runnable action;
            switch (scrollId.getType()) {
                case QUERY_THEN_FETCH_TYPE:
                    action = new SearchScrollQueryThenFetchAsyncAction(logger, clusterService, searchTransportService,
                        searchPhaseController, request, (SearchTask)task, scrollId, listener);
                    break;
                case QUERY_AND_FETCH_TYPE: // TODO can we get rid of this?
                    action = new SearchScrollQueryAndFetchAsyncAction(logger, clusterService, searchTransportService,
                        searchPhaseController, request, (SearchTask)task, scrollId, listener);
                    break;
                default:
                    throw new IllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            }
            action.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
