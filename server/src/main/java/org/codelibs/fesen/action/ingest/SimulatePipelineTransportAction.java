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

package org.codelibs.fesen.action.ingest;

import java.util.Map;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.ingest.IngestService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

public class SimulatePipelineTransportAction extends HandledTransportAction<SimulatePipelineRequest, SimulatePipelineResponse> {

    private final IngestService ingestService;
    private final SimulateExecutionService executionService;

    @Inject
    public SimulatePipelineTransportAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                           IngestService ingestService) {
        super(SimulatePipelineAction.NAME, transportService, actionFilters,
            (Writeable.Reader<SimulatePipelineRequest>) SimulatePipelineRequest::new);
        this.ingestService = ingestService;
        this.executionService = new SimulateExecutionService(threadPool);
    }

    @Override
    protected void doExecute(Task task, SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        final Map<String, Object> source = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();

        final SimulatePipelineRequest.Parsed simulateRequest;
        try {
            if (request.getId() != null) {
                simulateRequest = SimulatePipelineRequest.parseWithPipelineId(request.getId(), source, request.isVerbose(), ingestService);
            } else {
                simulateRequest = SimulatePipelineRequest.parse(source, request.isVerbose(), ingestService);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        executionService.execute(simulateRequest, listener);
    }
}
