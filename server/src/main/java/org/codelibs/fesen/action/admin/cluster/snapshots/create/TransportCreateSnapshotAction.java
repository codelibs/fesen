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

package org.codelibs.fesen.action.admin.cluster.snapshots.create;

import java.io.IOException;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.master.TransportMasterNodeAction;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.snapshots.SnapshotsService;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

/**
 * Transport action for create snapshot operation
 */
public class TransportCreateSnapshotAction extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportCreateSnapshotAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
            SnapshotsService snapshotsService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CreateSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters, CreateSnapshotRequest::new,
                indexNameExpressionResolver);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateSnapshotResponse read(StreamInput in) throws IOException {
        return new CreateSnapshotResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We only check metadata block, as we want to snapshot closed indices (which have a read block)
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        return null;
    }

    @Override
    protected void masterOperation(final CreateSnapshotRequest request, ClusterState state,
            final ActionListener<CreateSnapshotResponse> listener) {
        if (state.nodes().getMinNodeVersion().before(SnapshotsService.NO_REPO_INITIALIZE_VERSION)) {
            if (request.waitForCompletion()) {
                snapshotsService.executeSnapshotLegacy(request, ActionListener.map(listener, CreateSnapshotResponse::new));
            } else {
                snapshotsService.createSnapshotLegacy(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
            }
        } else {
            if (request.waitForCompletion()) {
                snapshotsService.executeSnapshot(request, ActionListener.map(listener, CreateSnapshotResponse::new));
            } else {
                snapshotsService.createSnapshot(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
            }
        }
    }
}
