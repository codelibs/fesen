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
package org.codelibs.fesen.script.mustache;

import org.codelibs.fesen.common.bytes.BytesArray;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.script.mustache.RestMultiSearchTemplateAction;
import org.codelibs.fesen.test.rest.FakeRestRequest;
import org.codelibs.fesen.test.rest.RestActionTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class RestMultiSearchTemplateActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiSearchTemplateAction(Settings.EMPTY));
    }

    public void testTypeInPath() {
        String content = "{ \"index\": \"some_index\" } \n" +
            "{\"source\": {\"query\" : {\"match_all\" :{}}}} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_msearch/template")
            .withContent(bytesContent, XContentType.JSON)
            .build();
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
        assertWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = "{ \"index\": \"some_index\", \"type\": \"some_type\" } \n" +
            "{\"source\": {\"query\" : {\"match_all\" :{}}}} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withPath("/some_index/_msearch/template")
            .withContent(bytesContent, XContentType.JSON)
            .build();
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
        assertWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }
}
