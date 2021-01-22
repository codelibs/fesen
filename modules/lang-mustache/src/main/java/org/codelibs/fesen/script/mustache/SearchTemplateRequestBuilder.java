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

import java.util.Map;

import org.codelibs.fesen.action.ActionRequestBuilder;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.script.ScriptType;

public class SearchTemplateRequestBuilder
        extends ActionRequestBuilder<SearchTemplateRequest, SearchTemplateResponse> {

    SearchTemplateRequestBuilder(FesenClient client, SearchTemplateAction action) {
        super(client, action, new SearchTemplateRequest());
    }

    public SearchTemplateRequestBuilder(FesenClient client) {
        this(client, SearchTemplateAction.INSTANCE);
    }

    public SearchTemplateRequestBuilder setRequest(SearchRequest searchRequest) {
        request.setRequest(searchRequest);
        return this;
    }

    public SearchTemplateRequestBuilder setSimulate(boolean simulate) {
        request.setSimulate(simulate);
        return this;
    }

    /**
     * Enables explanation for each hit on how its score was computed. Disabled by default
     */
    public SearchTemplateRequestBuilder setExplain(boolean explain) {
        request.setExplain(explain);
        return this;
    }

    /**
     * Enables profiling of the query. Disabled by default
     */
    public SearchTemplateRequestBuilder setProfile(boolean profile) {
        request.setProfile(profile);
        return this;
    }

    public SearchTemplateRequestBuilder setScriptType(ScriptType scriptType) {
        request.setScriptType(scriptType);
        return this;
    }

    public SearchTemplateRequestBuilder setScript(String script) {
        request.setScript(script);
        return this;
    }

    public SearchTemplateRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        request.setScriptParams(scriptParams);
        return this;
    }
}
