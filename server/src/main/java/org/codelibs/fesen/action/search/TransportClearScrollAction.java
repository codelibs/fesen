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

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.transport.TransportService;

public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportClearScrollAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                      SearchTransportService searchTransportService, NamedWriteableRegistry namedWriteableRegistry) {
        super(ClearScrollAction.NAME, transportService, actionFilters, ClearScrollRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        Runnable runnable = new ClearScrollController(
            request, listener, clusterService.state().nodes(), logger, searchTransportService);
        runnable.run();
    }

}