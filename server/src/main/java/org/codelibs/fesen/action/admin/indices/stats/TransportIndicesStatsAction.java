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

package org.codelibs.fesen.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.DefaultShardOperationFailedException;
import org.codelibs.fesen.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.routing.ShardRouting;
import org.codelibs.fesen.cluster.routing.ShardsIterator;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.index.IndexService;
import org.codelibs.fesen.index.engine.CommitStats;
import org.codelibs.fesen.index.seqno.RetentionLeaseStats;
import org.codelibs.fesen.index.seqno.SeqNoStats;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.shard.ShardNotFoundException;
import org.codelibs.fesen.indices.IndicesService;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportIndicesStatsAction extends TransportBroadcastByNodeAction<IndicesStatsRequest, IndicesStatsResponse, ShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesStatsAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(IndicesStatsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                IndicesStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStats readShardResult(StreamInput in) throws IOException {
        return new ShardStats(in);
    }

    @Override
    protected IndicesStatsResponse newResponse(IndicesStatsRequest request, int totalShards, int successfulShards, int failedShards,
                                               List<ShardStats> responses, List<DefaultShardOperationFailedException> shardFailures,
                                               ClusterState clusterState) {
        return new IndicesStatsResponse(responses.toArray(new ShardStats[responses.size()]), totalShards, successfulShards, failedShards,
            shardFailures);
    }

    @Override
    protected IndicesStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new IndicesStatsRequest(in);
    }

    @Override
    protected ShardStats shardOperation(IndicesStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        CommonStats commonStats = new CommonStats(indicesService.getIndicesQueryCache(), indexShard, request.flags());
        CommitStats commitStats;
        SeqNoStats seqNoStats;
        RetentionLeaseStats retentionLeaseStats;
        try {
            commitStats = indexShard.commitStats();
            seqNoStats = indexShard.seqNoStats();
            retentionLeaseStats = indexShard.getRetentionLeaseStats();
        } catch (final AlreadyClosedException e) {
            // shard is closed - no stats is fine
            commitStats = null;
            seqNoStats = null;
            retentionLeaseStats = null;
        }
        return new ShardStats(
                indexShard.routingEntry(),
                indexShard.shardPath(),
                commonStats,
                commitStats,
                seqNoStats,
                retentionLeaseStats);
    }
}
