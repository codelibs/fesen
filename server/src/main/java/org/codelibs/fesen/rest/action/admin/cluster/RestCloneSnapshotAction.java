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

import static org.codelibs.fesen.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codelibs.fesen.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.xcontent.support.XContentMapValues;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

/**
 * Clones indices from one snapshot into another snapshot in the same repository
 */
public class RestCloneSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(PUT, "/_snapshot/{repository}/{snapshot}/_clone/{target_snapshot}"));
    }

    @Override
    public String getName() {
        return "clone_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final Map<String, Object> source = request.contentParser().map();
        final CloneSnapshotRequest cloneSnapshotRequest =
                new CloneSnapshotRequest(request.param("repository"), request.param("snapshot"), request.param("target_snapshot"),
                        XContentMapValues.nodeStringArrayValue(source.getOrDefault("indices", Collections.emptyList())));
        cloneSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cloneSnapshotRequest.masterNodeTimeout()));
        cloneSnapshotRequest.indicesOptions(IndicesOptions.fromMap(source, cloneSnapshotRequest.indicesOptions()));
        return channel -> client.admin().cluster().cloneSnapshot(cloneSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
