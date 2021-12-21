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

package org.codelibs.fesen.action.admin.cluster.remote;

import static java.util.stream.Collectors.toList;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.search.SearchTransportService;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.transport.RemoteClusterService;
import org.codelibs.fesen.transport.TransportService;

public final class TransportRemoteInfoAction extends HandledTransportAction<RemoteInfoRequest, RemoteInfoResponse> {

    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportRemoteInfoAction(TransportService transportService, ActionFilters actionFilters,
                                     SearchTransportService searchTransportService) {
        super(RemoteInfoAction.NAME, transportService, actionFilters, RemoteInfoRequest::new);
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
    }

    @Override
    protected void doExecute(Task task, RemoteInfoRequest remoteInfoRequest, ActionListener<RemoteInfoResponse> listener) {
        listener.onResponse(new RemoteInfoResponse(remoteClusterService.getRemoteConnectionInfos().collect(toList())));
    }
}
