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

package org.codelibs.fesen.action.search;

import org.codelibs.fesen.action.ActionRequestBuilder;
import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.search.Scroll;

/**
 * A search scroll action request builder.
 */
public class SearchScrollRequestBuilder extends ActionRequestBuilder<SearchScrollRequest, SearchResponse> {

    public SearchScrollRequestBuilder(FesenClient client, SearchScrollAction action) {
        super(client, action, new SearchScrollRequest());
    }

    public SearchScrollRequestBuilder(FesenClient client, SearchScrollAction action, String scrollId) {
        super(client, action, new SearchScrollRequest(scrollId));
    }

    /**
     * The scroll id to use to continue scrolling.
     */
    public SearchScrollRequestBuilder setScrollId(String scrollId) {
        request.scrollId(scrollId);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchScrollRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }
}
