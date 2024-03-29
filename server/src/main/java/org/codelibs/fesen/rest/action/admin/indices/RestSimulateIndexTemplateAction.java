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

package org.codelibs.fesen.rest.action.admin.indices;

import static org.codelibs.fesen.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.post.SimulateIndexTemplateRequest;
import org.codelibs.fesen.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.cluster.metadata.ComposableIndexTemplate;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

public class RestSimulateIndexTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return org.codelibs.fesen.core.List.of(
            new Route(POST, "/_index_template/_simulate_index/{name}"));
    }

    @Override
    public String getName() {
        return "simulate_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SimulateIndexTemplateRequest simulateIndexTemplateRequest = new SimulateIndexTemplateRequest(request.param("name"));
        simulateIndexTemplateRequest.masterNodeTimeout(request.paramAsTime("master_timeout",
            simulateIndexTemplateRequest.masterNodeTimeout()));
        if (request.hasContent()) {
            PutComposableIndexTemplateAction.Request indexTemplateRequest =
                new PutComposableIndexTemplateAction.Request("simulating_template");
            indexTemplateRequest.indexTemplate(ComposableIndexTemplate.parse(request.contentParser()));
            indexTemplateRequest.create(request.paramAsBoolean("create", false));
            indexTemplateRequest.cause(request.param("cause", "api"));

            simulateIndexTemplateRequest.indexTemplateRequest(indexTemplateRequest);
        }

        return channel -> client.execute(SimulateIndexTemplateAction.INSTANCE, simulateIndexTemplateRequest,
            new RestToXContentListener<>(channel));
    }
}
