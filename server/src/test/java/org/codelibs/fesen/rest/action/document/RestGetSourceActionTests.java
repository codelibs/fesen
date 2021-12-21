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

import org.codelibs.fesen.ResourceNotFoundException;
import org.codelibs.fesen.action.get.GetResponse;
import org.codelibs.fesen.common.bytes.BytesArray;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.index.get.GetResult;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.RestResponse;
import org.codelibs.fesen.rest.RestRequest.Method;
import org.codelibs.fesen.rest.action.document.RestGetSourceAction;
import org.codelibs.fesen.rest.action.document.RestGetSourceAction.RestGetSourceResponseListener;
import org.codelibs.fesen.test.rest.FakeRestChannel;
import org.codelibs.fesen.test.rest.FakeRestRequest;
import org.codelibs.fesen.test.rest.RestActionTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.codelibs.fesen.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.codelibs.fesen.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

public class RestGetSourceActionTests extends RestActionTestCase {

    private static RestRequest request = new FakeRestRequest();
    private static FakeRestChannel channel = new FakeRestChannel(request, true, 0);
    private static RestGetSourceResponseListener listener = new RestGetSourceResponseListener(channel, request);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetSourceAction());
    }

    @AfterClass
    public static void cleanupReferences() {
        request = null;
        channel = null;
        listener = null;
    }

    /**
     * test deprecation is logged if type is used in path
     */
    public void testTypeInPath() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);
        for (Method method : Arrays.asList(Method.GET, Method.HEAD)) {
            // Ensure we have a fresh context for each request so we don't get duplicate headers
            try (ThreadContext.StoredContext ignore = verifyingClient.threadPool().getThreadContext().stashContext()) {
                RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                    .withMethod(method)
                    .withPath("/some_index/some_type/id/_source")
                    .build();

                dispatchRequest(request);
                assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
            }
        }
    }

    /**
     * test deprecation is logged if type is used as parameter
     */
    public void testTypeParameter() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");
        for (Method method : Arrays.asList(Method.GET, Method.HEAD)) {
            // Ensure we have a fresh context for each request so we don't get duplicate headers
            try (ThreadContext.StoredContext ignore = verifyingClient.threadPool().getThreadContext().stashContext()) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                    .withMethod(method)
                    .withPath("/some_index/_source/id")
                    .withParams(params)
                    .build();
            dispatchRequest(request);
            assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
            }
        }
    }

    public void testRestGetSourceAction() throws Exception {
        final BytesReference source = new BytesArray("{\"foo\": \"bar\"}");
        final GetResponse response =
            new GetResponse(new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, true, source, emptyMap(), null));

        final RestResponse restResponse = listener.buildResponse(response);

        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content(), equalTo(new BytesArray("{\"foo\": \"bar\"}")));
    }

    public void testRestGetSourceActionWithMissingDocument() {
        final GetResponse response =
            new GetResponse(new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, false, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Document not found [index1]/[_doc]/[1]"));
    }

    public void testRestGetSourceActionWithMissingDocumentSource() {
        final GetResponse response =
            new GetResponse(new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, true, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Source not found [index1]/[_doc]/[1]"));
    }
}
