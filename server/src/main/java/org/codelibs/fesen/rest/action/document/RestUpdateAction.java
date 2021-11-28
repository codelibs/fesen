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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;

import org.codelibs.fesen.action.ActionRequestValidationException;
import org.codelibs.fesen.action.DocWriteRequest;
import org.codelibs.fesen.action.index.IndexRequest;
import org.codelibs.fesen.action.support.ActiveShardCount;
import org.codelibs.fesen.action.update.UpdateRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.index.VersionType;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestActions;
import org.codelibs.fesen.rest.action.RestStatusToXContentListener;
import org.codelibs.fesen.search.fetch.subphase.FetchSourceContext;

public class RestUpdateAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestUpdateAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in "
            + "document update requests is deprecated, use the endpoint /{index}/_update/{id} instead.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_update/{id}"),
                // Deprecated typed endpoint.
                new Route(POST, "/{index}/{type}/{id}/_update")));
    }

    @Override
    public String getName() {
        return "document_update_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        UpdateRequest updateRequest;
        if (request.hasParam("type")) {
            deprecationLogger.deprecate("update_with_types", TYPES_DEPRECATION_MESSAGE);
            updateRequest = new UpdateRequest(request.param("index"), request.param("type"), request.param("id"));
        } else {
            updateRequest = new UpdateRequest(request.param("index"), request.param("id"));
        }

        updateRequest.routing(request.param("routing"));
        updateRequest.timeout(request.paramAsTime("timeout", updateRequest.timeout()));
        updateRequest.setRefreshPolicy(request.param("refresh"));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            updateRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        updateRequest.docAsUpsert(request.paramAsBoolean("doc_as_upsert", updateRequest.docAsUpsert()));
        FetchSourceContext fetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        if (fetchSourceContext != null) {
            updateRequest.fetchSource(fetchSourceContext);
        }

        updateRequest.retryOnConflict(request.paramAsInt("retry_on_conflict", updateRequest.retryOnConflict()));
        if (request.hasParam("version") || request.hasParam("version_type")) {
            final ActionRequestValidationException versioningError = new ActionRequestValidationException();
            versioningError.addValidationError("internal versioning can not be used for optimistic concurrency control. "
                    + "Please use `if_seq_no` and `if_primary_term` instead");
            throw versioningError;
        }

        updateRequest.setIfSeqNo(request.paramAsLong("if_seq_no", updateRequest.ifSeqNo()));
        updateRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", updateRequest.ifPrimaryTerm()));
        updateRequest.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, updateRequest.isRequireAlias()));

        request.applyContentParser(parser -> {
            updateRequest.fromXContent(parser);
            IndexRequest upsertRequest = updateRequest.upsertRequest();
            if (upsertRequest != null) {
                upsertRequest.routing(request.param("routing"));
                upsertRequest.version(RestActions.parseVersion(request));
                upsertRequest.versionType(VersionType.fromString(request.param("version_type"), upsertRequest.versionType()));
            }
            IndexRequest doc = updateRequest.doc();
            if (doc != null) {
                doc.routing(request.param("routing"));
                doc.version(RestActions.parseVersion(request));
                doc.versionType(VersionType.fromString(request.param("version_type"), doc.versionType()));
            }
        });

        return channel -> client.update(updateRequest,
                new RestStatusToXContentListener<>(channel, r -> r.getLocation(updateRequest.routing())));
    }

}
