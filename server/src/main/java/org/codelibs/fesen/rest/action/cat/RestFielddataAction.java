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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.GET;

import java.util.List;

import org.codelibs.fesen.action.admin.cluster.node.stats.NodeStats;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Table;
import org.codelibs.fesen.common.unit.ByteSizeValue;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.RestResponse;
import org.codelibs.fesen.rest.action.RestResponseListener;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;

/**
 * Cat API class to display information about the size of fielddata fields per node
 */
public class RestFielddataAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/fielddata"), new Route(GET, "/_cat/fielddata/{fields}")));
    }

    @Override
    public String getName() {
        return "cat_fielddata_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.clear();
        nodesStatsRequest.indices(true);
        String[] fields = request.paramAsStringArray("fields", null);
        nodesStatsRequest.indices().fieldDataFields(fields == null ? new String[] { "*" } : fields);

        return channel -> client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesStatsResponse nodeStatses) throws Exception {
                return RestTable.buildResponse(buildTable(request, nodeStatses), channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/fielddata\n");
        sb.append("/_cat/fielddata/{fields}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders().addCell("id", "desc:node id").addCell("host", "alias:h;desc:host name").addCell("ip", "desc:ip address")
                .addCell("node", "alias:n;desc:node name").addCell("field", "alias:f;desc:field name")
                .addCell("size", "text-align:right;alias:s;desc:field data usage").endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, final NodesStatsResponse nodeStatses) {
        Table table = getTableWithHeader(request);

        for (NodeStats nodeStats : nodeStatses.getNodes()) {
            if (nodeStats.getIndices().getFieldData().getFields() != null) {
                for (ObjectLongCursor<String> cursor : nodeStats.getIndices().getFieldData().getFields()) {
                    table.startRow();
                    table.addCell(nodeStats.getNode().getId());
                    table.addCell(nodeStats.getNode().getHostName());
                    table.addCell(nodeStats.getNode().getHostAddress());
                    table.addCell(nodeStats.getNode().getName());
                    table.addCell(cursor.key);
                    table.addCell(new ByteSizeValue(cursor.value));
                    table.endRow();
                }
            }
        }

        return table;
    }
}
