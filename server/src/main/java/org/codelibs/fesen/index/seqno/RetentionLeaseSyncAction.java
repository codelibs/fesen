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

package org.codelibs.fesen.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.codelibs.fesen.ExceptionsHelper;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.ActiveShardCount;
import org.codelibs.fesen.action.support.WriteResponse;
import org.codelibs.fesen.action.support.replication.ReplicatedWriteRequest;
import org.codelibs.fesen.action.support.replication.ReplicationResponse;
import org.codelibs.fesen.action.support.replication.ReplicationTask;
import org.codelibs.fesen.action.support.replication.TransportWriteAction;
import org.codelibs.fesen.cluster.action.shard.ShardStateAction;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.index.IndexNotFoundException;
import org.codelibs.fesen.index.IndexingPressure;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.shard.IndexShardClosedException;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.indices.IndicesService;
import org.codelibs.fesen.indices.SystemIndices;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportException;
import org.codelibs.fesen.transport.TransportResponseHandler;
import org.codelibs.fesen.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 */
public class RetentionLeaseSyncAction extends
        TransportWriteAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Response> {

    public static final String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";
    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    @Inject
    public RetentionLeaseSyncAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters,
        final IndexingPressure indexingPressure,
        final SystemIndices systemIndices) {
        super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                RetentionLeaseSyncAction.Request::new,
                RetentionLeaseSyncAction.Request::new,
                ignore -> ThreadPool.Names.MANAGEMENT, false, indexingPressure, systemIndices);
    }

    @Override
    protected void doExecute(Task parentTask, Request request, ActionListener<Response> listener) {
        assert false : "use RetentionLeaseSyncAction#sync";
    }

    final void sync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases,
                    ActionListener<ReplicationResponse> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            final Request request = new Request(shardId, retentionLeases);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "retention_lease_sync", request);
            transportService.sendChildRequest(clusterService.localNode(), transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        if (ExceptionsHelper.unwrap(e,
                                                    IndexNotFoundException.class,
                                                    AlreadyClosedException.class,
                                                    IndexShardClosedException.class) == null) {
                            getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                        }
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onFailure(e);
                    }
                });
        }
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(Request request, IndexShard primary,
            ActionListener<PrimaryResult<Request, Response>> listener) {
        ActionListener.completeWith(listener, () -> {
            assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
            Objects.requireNonNull(request);
            Objects.requireNonNull(primary);
            primary.persistRetentionLeases();
            return new WritePrimaryResult<>(request, new Response(), null, null, primary, getLogger());
        });
    }

    @Override
    protected void dispatchedShardOperationOnReplica(Request request, IndexShard replica,
            ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            Objects.requireNonNull(request);
            Objects.requireNonNull(replica);
            replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
            replica.persistRetentionLeases();
            return new WriteReplicaResult<>(request, null, null, replica, getLogger());
        });
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return null;
    }

    public static final class Request extends ReplicatedWriteRequest<Request> {

        private RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            retentionLeases = new RetentionLeases(in);
        }

        public Request(final ShardId shardId, final RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ReplicationTask(id, type, action, "retention_lease_sync shardId=" + shardId, parentTaskId, headers);
        }

        @Override
        public String toString() {
            return "RetentionLeaseSyncAction.Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    public static final class Response extends ReplicationResponse implements WriteResponse {

        public Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void setForcedRefresh(final boolean forcedRefresh) {
            // ignore
        }

    }

    @Override
    protected Response newResponseInstance(StreamInput in) throws IOException {
        return new Response(in);
    }

}
