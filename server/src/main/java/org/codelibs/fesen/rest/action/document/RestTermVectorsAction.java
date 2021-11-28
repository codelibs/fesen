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
import static org.codelibs.fesen.rest.RestRequest.Method.GET;
import static org.codelibs.fesen.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codelibs.fesen.action.termvectors.TermVectorsRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.index.VersionType;
import org.codelibs.fesen.index.mapper.MapperService;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestActions;
import org.codelibs.fesen.rest.action.RestToXContentListener;

/**
 * This class parses the json request and translates it into a
 * TermVectorsRequest.
 */
public class RestTermVectorsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestTermVectorsAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] " + "Specifying types in term vector requests is deprecated.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/{index}/_termvectors"), new Route(POST, "/{index}/_termvectors"),
                new Route(GET, "/{index}/_termvectors/{id}"), new Route(POST, "/{index}/_termvectors/{id}"),
                // Deprecated typed endpoints.
                new Route(GET, "/{index}/{type}/_termvectors"), new Route(POST, "/{index}/{type}/_termvectors"),
                new Route(GET, "/{index}/{type}/{id}/_termvectors"), new Route(POST, "/{index}/{type}/{id}/_termvectors")));
    }

    @Override
    public String getName() {
        return "document_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TermVectorsRequest termVectorsRequest;
        if (request.hasParam("type")) {
            deprecationLogger.deprecate("termvectors_with_types", TYPES_DEPRECATION_MESSAGE);
            termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("type"), request.param("id"));
        } else {
            termVectorsRequest = new TermVectorsRequest(request.param("index"), MapperService.SINGLE_MAPPING_NAME, request.param("id"));
        }

        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                TermVectorsRequest.parseRequest(termVectorsRequest, parser);
            }
        }
        readURIParameters(termVectorsRequest, request);

        return channel -> client.termVectors(termVectorsRequest, new RestToXContentListener<>(channel));
    }

    public static void readURIParameters(TermVectorsRequest termVectorsRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorsRequest, fields);
        termVectorsRequest.offsets(request.paramAsBoolean("offsets", termVectorsRequest.offsets()));
        termVectorsRequest.positions(request.paramAsBoolean("positions", termVectorsRequest.positions()));
        termVectorsRequest.payloads(request.paramAsBoolean("payloads", termVectorsRequest.payloads()));
        termVectorsRequest.routing(request.param("routing"));
        termVectorsRequest.realtime(request.paramAsBoolean("realtime", termVectorsRequest.realtime()));
        termVectorsRequest.version(RestActions.parseVersion(request, termVectorsRequest.version()));
        termVectorsRequest.versionType(VersionType.fromString(request.param("version_type"), termVectorsRequest.versionType()));
        termVectorsRequest.preference(request.param("preference"));
        termVectorsRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorsRequest.fieldStatistics()));
    }

    public static void addFieldStringsFromParameter(TermVectorsRequest termVectorsRequest, String fields) {
        Set<String> selectedFields = termVectorsRequest.selectedFields();
        if (fields != null) {
            String[] paramFieldStrings = Strings.commaDelimitedListToStringArray(fields);
            for (String field : paramFieldStrings) {
                if (selectedFields == null) {
                    selectedFields = new HashSet<>();
                }
                if (!selectedFields.contains(field)) {
                    field = field.replaceAll("\\s", "");
                    selectedFields.add(field);
                }
            }
        }
        if (selectedFields != null) {
            termVectorsRequest.selectedFields(selectedFields.toArray(new String[selectedFields.size()]));
        }
    }

}
