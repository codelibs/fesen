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

package org.codelibs.fesen.rest.action.document;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.DocWriteRequest;
import org.codelibs.fesen.action.bulk.BulkRequest;
import org.codelibs.fesen.action.bulk.BulkShardRequest;
import org.codelibs.fesen.action.support.ActiveShardCount;
import org.codelibs.fesen.client.Requests;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.mapper.MapperService;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestStatusToXContentListener;
import org.codelibs.fesen.rest.action.search.RestSearchAction;
import org.codelibs.fesen.search.fetch.subphase.FetchSourceContext;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;
import static org.codelibs.fesen.rest.RestRequest.Method.PUT;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 */
public class RestBulkAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSearchAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
    " Specifying types in bulk requests is deprecated.";

    public RestBulkAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_bulk"),
            new Route(PUT, "/_bulk"),
            new Route(POST, "/{index}/_bulk"),
            new Route(PUT, "/{index}/_bulk"),
            // Deprecated typed endpoints.
            new Route(POST, "/{index}/{type}/_bulk"),
            new Route(PUT, "/{index}/{type}/_bulk")));
    }

    @Override
    public String getName() {
        return "bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        BulkRequest bulkRequest = Requests.bulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        if (defaultType == null) {
            defaultType = MapperService.SINGLE_MAPPING_NAME;
        } else {
            deprecationLogger.deprecate("bulk_with_types", RestBulkAction.TYPES_DEPRECATION_MESSAGE);
        }
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String defaultPipeline = request.param("pipeline");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, null);
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        bulkRequest.add(request.requiredContent(), defaultIndex, defaultType, defaultRouting,
            defaultFetchSourceContext, defaultPipeline, defaultRequireAlias, allowExplicitIndex, request.getXContentType());

        return channel -> client.bulk(bulkRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}
