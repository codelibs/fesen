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

package org.codelibs.fesen.rest.action;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesRequest;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.GET;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;

public class RestFieldCapabilitiesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_field_caps"),
            new Route(POST, "/_field_caps"),
            new Route(GET, "/{index}/_field_caps"),
            new Route(POST, "/{index}/_field_caps")));
    }

    @Override
    public String getName() {
        return "field_capabilities_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request,
                                              final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        FieldCapabilitiesRequest fieldRequest = new FieldCapabilitiesRequest()
            .fields(Strings.splitStringByCommaToArray(request.param("fields")))
            .indices(indices);

        fieldRequest.indicesOptions(
            IndicesOptions.fromRequest(request, fieldRequest.indicesOptions()));
        fieldRequest.includeUnmapped(request.paramAsBoolean("include_unmapped", false));
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                fieldRequest.indexFilter(RestActions.getQueryContent("index_filter", parser));
            }
        });
        return channel -> client.fieldCaps(fieldRequest, new RestToXContentListener<>(channel));
    }
}
