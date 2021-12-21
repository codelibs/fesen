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

import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.search.RestSearchAction;
import org.codelibs.fesen.script.mustache.RestSearchTemplateAction;
import org.codelibs.fesen.test.rest.FakeRestRequest;
import org.codelibs.fesen.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class RestSearchTemplateActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestSearchTemplateAction());
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_search/template")
            .build();

        dispatchRequest(request);
        assertWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_search/template")
            .withParams(params)
            .build();

        dispatchRequest(request);
        assertWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }
}
