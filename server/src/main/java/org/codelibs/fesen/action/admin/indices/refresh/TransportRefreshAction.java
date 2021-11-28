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

package org.codelibs.fesen.action.admin.indices.refresh;

import java.util.List;

import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.ActiveShardCount;
import org.codelibs.fesen.action.support.DefaultShardOperationFailedException;
import org.codelibs.fesen.action.support.replication.BasicReplicationRequest;
import org.codelibs.fesen.action.support.replication.ReplicationResponse;
import org.codelibs.fesen.action.support.replication.TransportBroadcastReplicationAction;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.transport.TransportService;

/**
 * Refresh action.
 */
public class TransportRefreshAction
        extends TransportBroadcastReplicationAction<RefreshRequest, RefreshResponse, BasicReplicationRequest, ReplicationResponse> {

    @Inject
    public TransportRefreshAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, TransportShardRefreshAction shardRefreshAction) {
        super(RefreshAction.NAME, RefreshRequest::new, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                shardRefreshAction);
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected BasicReplicationRequest newShardRequest(RefreshRequest request, ShardId shardId) {
        BasicReplicationRequest replicationRequest = new BasicReplicationRequest(shardId);
        replicationRequest.waitForActiveShards(ActiveShardCount.NONE);
        return replicationRequest;
    }

    @Override
    protected RefreshResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
            List<DefaultShardOperationFailedException> shardFailures) {
        return new RefreshResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
