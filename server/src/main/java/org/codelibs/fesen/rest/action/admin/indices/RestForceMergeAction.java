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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeRequest;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

public class RestForceMergeAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestForceMergeAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_forcemerge"), new Route(POST, "/{index}/_forcemerge")));
    }

    @Override
    public String getName() {
        return "force_merge_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ForceMergeRequest mergeRequest = new ForceMergeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        mergeRequest.indicesOptions(IndicesOptions.fromRequest(request, mergeRequest.indicesOptions()));
        mergeRequest.maxNumSegments(request.paramAsInt("max_num_segments", mergeRequest.maxNumSegments()));
        mergeRequest.onlyExpungeDeletes(request.paramAsBoolean("only_expunge_deletes", mergeRequest.onlyExpungeDeletes()));
        mergeRequest.flush(request.paramAsBoolean("flush", mergeRequest.flush()));
        if (mergeRequest.onlyExpungeDeletes() && mergeRequest.maxNumSegments() != ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS) {
            deprecationLogger.deprecate("force_merge_expunge_deletes_and_max_num_segments_deprecation",
                    "setting only_expunge_deletes and max_num_segments at the same time is deprecated and will be rejected in a future version");
        }
        return channel -> client.admin().indices().forceMerge(mergeRequest, new RestToXContentListener<>(channel));
    }

}
