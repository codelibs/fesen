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
package org.codelibs.fesen.action.admin.indices.mapping.put;

import static org.codelibs.fesen.action.admin.indices.mapping.put.TransportPutMappingAction.performMappingUpdate;

import java.io.IOException;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.action.support.master.TransportMasterNodeAction;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.metadata.MetadataMappingService;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

public class TransportAutoPutMappingAction extends TransportMasterNodeAction<PutMappingRequest, AcknowledgedResponse> {

    private final MetadataMappingService metadataMappingService;

    @Inject
    public TransportAutoPutMappingAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final MetadataMappingService metadataMappingService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AutoPutMappingAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutMappingRequest::new, indexNameExpressionResolver);
        this.metadataMappingService = metadataMappingService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void doExecute(Task task, PutMappingRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (request.getConcreteIndex() == null) {
            throw new IllegalArgumentException("concrete index missing");
        }

        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        String[] indices = new String[] {request.getConcreteIndex().getName()};
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(final PutMappingRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        final Index[] concreteIndices = new Index[] {request.getConcreteIndex()};
        performMappingUpdate(concreteIndices, request, listener, metadataMappingService);
    }

}
