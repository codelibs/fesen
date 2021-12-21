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

package org.codelibs.fesen.rest.action.cat;

import java.util.List;

import org.codelibs.fesen.action.admin.cluster.node.info.NodeInfo;
import org.codelibs.fesen.action.admin.cluster.node.info.NodesInfoRequest;
import org.codelibs.fesen.action.admin.cluster.node.info.NodesInfoResponse;
import org.codelibs.fesen.action.admin.cluster.node.info.PluginsAndModules;
import org.codelibs.fesen.action.admin.cluster.state.ClusterStateRequest;
import org.codelibs.fesen.action.admin.cluster.state.ClusterStateResponse;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.common.Table;
import org.codelibs.fesen.plugins.PluginInfo;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.RestResponse;
import org.codelibs.fesen.rest.action.RestActionListener;
import org.codelibs.fesen.rest.action.RestResponseListener;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.rest.RestRequest.Method.GET;

public class RestPluginsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/plugins"));
    }

    @Override
    public String getName() {
        return "cat_plugins_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/plugins\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) throws Exception {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear()
                    .addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestResponseListener<NodesInfoResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final NodesInfoResponse nodesInfoResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(request, clusterStateResponse, nodesInfoResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("id", "default:false;desc:unique node id");
        table.addCell("name", "alias:n;desc:node name");
        table.addCell("component", "alias:c;desc:component");
        table.addCell("version", "alias:v;desc:component version");
        table.addCell("description", "alias:d;default:false;desc:plugin details");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo) {
        DiscoveryNodes nodes = state.getState().nodes();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            if (info == null) {
                continue;
            }
            PluginsAndModules plugins = info.getInfo(PluginsAndModules.class);
            if (plugins == null) {
                continue;
            }
            for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                table.startRow();
                table.addCell(node.getId());
                table.addCell(node.getName());
                table.addCell(pluginInfo.getName());
                table.addCell(pluginInfo.getVersion());
                table.addCell(pluginInfo.getDescription());
                table.endRow();
            }
        }

        return table;
    }
}
