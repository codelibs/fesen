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

package org.codelibs.fesen.rest.action.search;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.search.ClearScrollRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestStatusToXContentListener;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.DELETE;

public class RestClearScrollAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(DELETE, "/_search/scroll"),
            new Route(DELETE, "/_search/scroll/{scroll_id}")));
    }

    @Override
    public String getName() {
        return "clear_scroll_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String scrollIds = request.param("scroll_id");
        ClearScrollRequest clearRequest = new ClearScrollRequest();
        clearRequest.setScrollIds(asList(Strings.splitStringByCommaToArray(scrollIds)));
        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if (xContentParser != null) {
                // NOTE: if rest request with xcontent body has request parameters, values parsed from request body have the precedence
                try {
                    clearRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }
        }));

        return channel -> client.clearScroll(clearRequest, new RestStatusToXContentListener<>(channel));
    }

}