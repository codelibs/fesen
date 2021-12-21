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

package org.codelibs.fesen.action.admin.indices.template.get;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.fesen.ResourceNotFoundException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.master.TransportMasterNodeReadAction;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.metadata.ComposableIndexTemplate;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.regex.Regex;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

public class TransportGetComposableIndexTemplateAction
    extends TransportMasterNodeReadAction<GetComposableIndexTemplateAction.Request, GetComposableIndexTemplateAction.Response> {

    @Inject
    public TransportGetComposableIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                                     ThreadPool threadPool, ActionFilters actionFilters,
                                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetComposableIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetComposableIndexTemplateAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetComposableIndexTemplateAction.Response read(StreamInput in) throws IOException {
        return new GetComposableIndexTemplateAction.Response(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetComposableIndexTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(GetComposableIndexTemplateAction.Request request, ClusterState state,
                                   ActionListener<GetComposableIndexTemplateAction.Response> listener) {
        Map<String, ComposableIndexTemplate> allTemplates = state.metadata().templatesV2();

        // If we did not ask for a specific name, then we return all templates
        if (request.name() == null) {
            listener.onResponse(new GetComposableIndexTemplateAction.Response(allTemplates));
            return;
        }

        final Map<String, ComposableIndexTemplate> results = new HashMap<>();
        String name = request.name();
        if (Regex.isSimpleMatchPattern(name)) {
            for (Map.Entry<String, ComposableIndexTemplate> entry : allTemplates.entrySet()) {
                if (Regex.simpleMatch(name, entry.getKey())) {
                    results.put(entry.getKey(), entry.getValue());
                }
            }
        } else if (allTemplates.containsKey(name)) {
            results.put(name, allTemplates.get(name));
        } else {
            throw new ResourceNotFoundException("index template matching [" + request.name() + "] not found");
        }

        listener.onResponse(new GetComposableIndexTemplateAction.Response(results));
    }
}
