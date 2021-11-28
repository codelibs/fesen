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
import static org.codelibs.fesen.rest.RestRequest.Method.DELETE;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.codelibs.fesen.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

public class RestClearVotingConfigExclusionsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_cluster/voting_config_exclusions"));
    }

    @Override
    public String getName() {
        return "clear_voting_config_exclusions_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClearVotingConfigExclusionsRequest req = new ClearVotingConfigExclusionsRequest();
        if (request.hasParam("wait_for_removal")) {
            req.setWaitForRemoval(request.paramAsBoolean("wait_for_removal", true));
        }
        return channel -> client.execute(ClearVotingConfigExclusionsAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
