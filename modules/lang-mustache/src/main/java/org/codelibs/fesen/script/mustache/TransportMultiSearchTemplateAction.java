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

import static org.codelibs.fesen.script.mustache.TransportSearchTemplateAction.convert;

import java.util.ArrayList;
import java.util.List;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.search.MultiSearchRequest;
import org.codelibs.fesen.action.search.MultiSearchResponse;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.transport.TransportService;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final NodeClient client;

    @Inject
    public TransportMultiSearchTemplateAction(TransportService transportService, ActionFilters actionFilters, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, NodeClient client) {
        super(MultiSearchTemplateAction.NAME, transportService, actionFilters, MultiSearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        List<Integer> originalSlots = new ArrayList<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(request.indicesOptions());
        if (request.maxConcurrentSearchRequests() != 0) {
            multiSearchRequest.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[request.requests().size()];
        for (int i = 0; i < items.length; i++) {
            SearchTemplateRequest searchTemplateRequest = request.requests().get(i);
            SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
            SearchRequest searchRequest;
            try {
                searchRequest = convert(searchTemplateRequest, searchTemplateResponse, scriptService, xContentRegistry);
            } catch (Exception e) {
                items[i] = new MultiSearchTemplateResponse.Item(null, e);
                continue;
            }
            items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            if (searchRequest != null) {
                multiSearchRequest.add(searchRequest);
                originalSlots.add(i);
            }
        }

        client.multiSearch(multiSearchRequest, ActionListener.wrap(r -> {
            for (int i = 0; i < r.getResponses().length; i++) {
                MultiSearchResponse.Item item = r.getResponses()[i];
                int originalSlot = originalSlots.get(i);
                if (item.isFailure()) {
                    items[originalSlot] = new MultiSearchTemplateResponse.Item(null, item.getFailure());
                } else {
                    items[originalSlot].getResponse().setResponse(item.getResponse());
                }
            }
            listener.onResponse(new MultiSearchTemplateResponse(items, r.getTook().millis()));
        }, listener::onFailure));
    }
}
