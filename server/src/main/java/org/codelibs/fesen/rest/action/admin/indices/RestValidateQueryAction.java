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
import static org.codelibs.fesen.rest.RestRequest.Method.GET;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;
import static org.codelibs.fesen.rest.RestStatus.OK;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.admin.indices.validate.query.QueryExplanation;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryRequest;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryResponse;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.ParsingException;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.BytesRestResponse;
import org.codelibs.fesen.rest.RestChannel;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestActions;
import org.codelibs.fesen.rest.action.RestToXContentListener;

public class RestValidateQueryAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestValidateQueryAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" + " Specifying types in validate query requests is deprecated.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_validate/query"), new Route(POST, "/_validate/query"),
                new Route(GET, "/{index}/_validate/query"), new Route(POST, "/{index}/_validate/query"),
                new Route(GET, "/{index}/{type}/_validate/query"), new Route(POST, "/{index}/{type}/_validate/query")));
    }

    @Override
    public String getName() {
        return "validate_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        validateQueryRequest.indicesOptions(IndicesOptions.fromRequest(request, validateQueryRequest.indicesOptions()));
        validateQueryRequest.explain(request.paramAsBoolean("explain", false));

        if (request.hasParam("type")) {
            deprecationLogger.deprecate("validate_query_with_types", TYPES_DEPRECATION_MESSAGE);
            validateQueryRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        }

        validateQueryRequest.rewrite(request.paramAsBoolean("rewrite", false));
        validateQueryRequest.allShards(request.paramAsBoolean("all_shards", false));

        Exception bodyParsingException = null;
        try {
            request.withContentOrSourceParamParserOrNull(parser -> {
                if (parser != null) {
                    validateQueryRequest.query(RestActions.getQueryContent(parser));
                } else if (request.hasParam("q")) {
                    validateQueryRequest.query(RestActions.urlParamsToQueryBuilder(request));
                }
            });
        } catch (Exception e) {
            bodyParsingException = e;
        }

        final Exception finalBodyParsingException = bodyParsingException;
        return channel -> {
            if (finalBodyParsingException != null) {
                if (finalBodyParsingException instanceof ParsingException) {
                    handleException(validateQueryRequest, ((ParsingException) finalBodyParsingException).getDetailedMessage(), channel);
                } else {
                    handleException(validateQueryRequest, finalBodyParsingException.getMessage(), channel);
                }
            } else {
                client.admin().indices().validateQuery(validateQueryRequest, new RestToXContentListener<>(channel));
            }
        };
    }

    private void handleException(final ValidateQueryRequest request, final String message, final RestChannel channel) throws IOException {
        channel.sendResponse(buildErrorResponse(channel.newBuilder(), message, request.explain()));
    }

    private static BytesRestResponse buildErrorResponse(XContentBuilder builder, String error, boolean explain) throws IOException {
        builder.startObject();
        builder.field(ValidateQueryResponse.VALID_FIELD, false);
        if (explain) {
            builder.field(QueryExplanation.ERROR_FIELD, error);
        }
        builder.endObject();
        return new BytesRestResponse(OK, builder);
    }
}
