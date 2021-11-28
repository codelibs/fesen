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

package org.codelibs.fesen.rest.action.admin.cluster;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.client.Requests.restoreSnapshotRequest;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

/**
 * Restores a snapshot
 */
public class RestRestoreSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_snapshot/{repository}/{snapshot}/_restore"));
    }

    @Override
    public String getName() {
        return "restore_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RestoreSnapshotRequest restoreSnapshotRequest = restoreSnapshotRequest(request.param("repository"), request.param("snapshot"));
        restoreSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", restoreSnapshotRequest.masterNodeTimeout()));
        restoreSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        request.applyContentParser(p -> restoreSnapshotRequest.source(p.mapOrdered()));
        return channel -> client.admin().cluster().restoreSnapshot(restoreSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
