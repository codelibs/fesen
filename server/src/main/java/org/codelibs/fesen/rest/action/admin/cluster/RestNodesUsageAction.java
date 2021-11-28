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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.codelibs.fesen.action.admin.cluster.node.usage.NodesUsageRequest;
import org.codelibs.fesen.action.admin.cluster.node.usage.NodesUsageResponse;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.BytesRestResponse;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.RestResponse;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.rest.action.RestActions;
import org.codelibs.fesen.rest.action.RestBuilderListener;

public class RestNodesUsageAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_nodes/usage"), new Route(GET, "/_nodes/{nodeId}/usage"),
                new Route(GET, "/_nodes/usage/{metric}"), new Route(GET, "/_nodes/{nodeId}/usage/{metric}")));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));

        NodesUsageRequest nodesUsageRequest = new NodesUsageRequest(nodesIds);
        nodesUsageRequest.timeout(request.param("timeout"));

        if (metrics.size() == 1 && metrics.contains("_all")) {
            nodesUsageRequest.all();
        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "request [%s] contains _all and individual metrics [%s]",
                    request.path(), request.param("metric")));
        } else {
            nodesUsageRequest.clear();
            nodesUsageRequest.restActions(metrics.contains("rest_actions"));
            nodesUsageRequest.aggregations(metrics.contains("aggregations"));
        }

        return channel -> client.admin().cluster().nodesUsage(nodesUsageRequest, new RestBuilderListener<NodesUsageResponse>(channel) {

            @Override
            public RestResponse buildResponse(NodesUsageResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                RestActions.buildNodesHeader(builder, channel.request(), response);
                builder.field("cluster_name", response.getClusterName().value());
                response.toXContent(builder, channel.request());
                builder.endObject();

                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public String getName() {
        return "nodes_usage_action";
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
