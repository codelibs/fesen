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

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionListenerResponseHandler;
import org.codelibs.fesen.action.IndicesRequest;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.support.ChannelActionListener;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.common.util.concurrent.ConcurrentCollections;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.search.SearchPhaseResult;
import org.codelibs.fesen.search.SearchService;
import org.codelibs.fesen.search.dfs.DfsSearchResult;
import org.codelibs.fesen.search.fetch.FetchSearchResult;
import org.codelibs.fesen.search.fetch.QueryFetchSearchResult;
import org.codelibs.fesen.search.fetch.ScrollQueryFetchSearchResult;
import org.codelibs.fesen.search.fetch.ShardFetchRequest;
import org.codelibs.fesen.search.fetch.ShardFetchSearchRequest;
import org.codelibs.fesen.search.internal.InternalScrollSearchRequest;
import org.codelibs.fesen.search.internal.ShardSearchContextId;
import org.codelibs.fesen.search.internal.ShardSearchRequest;
import org.codelibs.fesen.search.query.QuerySearchRequest;
import org.codelibs.fesen.search.query.QuerySearchResult;
import org.codelibs.fesen.search.query.ScrollQuerySearchResult;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.RemoteClusterService;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportActionProxy;
import org.codelibs.fesen.transport.TransportException;
import org.codelibs.fesen.transport.TransportRequest;
import org.codelibs.fesen.transport.TransportRequestOptions;
import org.codelibs.fesen.transport.TransportResponse;
import org.codelibs.fesen.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * An encapsulation of {@link org.codelibs.fesen.search.SearchService} operations exposed through
 * transport.
 */
public class SearchTransportService {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";
    public static final String QUERY_CAN_MATCH_NAME = "indices:data/read/search[can_match]";

    private final TransportService transportService;
    private final BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper;
    private final Map<String, Long> clientConnections = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public SearchTransportService(TransportService transportService,
                                  BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper) {
        this.transportService = transportService;
        this.responseWrapper = responseWrapper;
    }

    public void sendFreeContext(Transport.Connection connection, final ShardSearchContextId contextId, OriginalIndices originalIndices) {
        transportService.sendRequest(connection, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(new ActionListener<SearchFreeContextResponse>() {
                @Override
                public void onResponse(SearchFreeContextResponse response) {
                    // no need to respond if it was freed or not
                }

                @Override
                public void onFailure(Exception e) {

                }
            }, SearchFreeContextResponse::new));
    }

    public void sendFreeContext(Transport.Connection connection, ShardSearchContextId contextId,
                                ActionListener<SearchFreeContextResponse> listener) {
        transportService.sendRequest(connection, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new));
    }

    public void sendCanMatch(Transport.Connection connection, final ShardSearchRequest request, SearchTask task, final
                            ActionListener<SearchService.CanMatchResponse> listener) {
        transportService.sendChildRequest(connection, QUERY_CAN_MATCH_NAME, request, task,
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, SearchService.CanMatchResponse::new));
    }

    public void sendClearAllScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(connection, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, (in) -> TransportResponse.Empty.INSTANCE));
    }

    public void sendExecuteDfs(Transport.Connection connection, final ShardSearchRequest request, SearchTask task,
                               final SearchActionListener<DfsSearchResult> listener) {
        transportService.sendChildRequest(connection, DFS_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, DfsSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final ShardSearchRequest request, SearchTask task,
                                 final SearchActionListener<SearchPhaseResult> listener) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exist anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        final ActionListener handler = responseWrapper.apply(connection, listener);
        transportService.sendChildRequest(connection, QUERY_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(handler, reader, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final QuerySearchRequest request, SearchTask task,
                                 final SearchActionListener<QuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_ID_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, QuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollQuery(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollFetch(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQueryFetchSearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_FETCH_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQueryFetchSearchResult::new, clientConnections,
                    connection.getNode().getId()));
    }

    public void sendExecuteFetch(Transport.Connection connection, final ShardFetchSearchRequest request, SearchTask task,
                                 final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(Transport.Connection connection, final ShardFetchRequest request, SearchTask task,
                                       final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(Transport.Connection connection, String action, final ShardFetchRequest request, SearchTask task,
                                  final SearchActionListener<FetchSearchResult> listener) {
        transportService.sendChildRequest(connection, action, request, task,
                new ConnectionCountingHandler<>(listener, FetchSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task,
                                final ActionListener<MultiSearchResponse> listener) {
        final Transport.Connection connection = transportService.getConnection(transportService.getLocalNode());
        transportService.sendChildRequest(connection, MultiSearchAction.NAME, request, task,
                new ConnectionCountingHandler<>(listener, MultiSearchResponse::new, clientConnections, connection.getNode().getId()));
    }

    public RemoteClusterService getRemoteClusterService() {
        return transportService.getRemoteClusterService();
    }

    /**
     * Return a map of nodeId to pending number of search requests.
     * This is a snapshot of the current pending search and not a live map.
     */
    public Map<String, Long> getPendingSearchRequests() {
        return new HashMap<>(clientConnections);
    }

    static class ScrollFreeContextRequest extends TransportRequest {
        private ShardSearchContextId contextId;

        ScrollFreeContextRequest(ShardSearchContextId contextId) {
            this.contextId = Objects.requireNonNull(contextId);
        }

        ScrollFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }

        public ShardSearchContextId id() {
            return this.contextId;
        }

    }

    static class SearchFreeContextRequest extends ScrollFreeContextRequest implements IndicesRequest {
        private OriginalIndices originalIndices;

        SearchFreeContextRequest(OriginalIndices originalIndices, ShardSearchContextId id) {
            super(id);
            this.originalIndices = originalIndices;
        }

        SearchFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }

        @Override
        public String[] indices() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indicesOptions();
        }

        }

    public static class SearchFreeContextResponse extends TransportResponse {

        private boolean freed;

        SearchFreeContextResponse(StreamInput in) throws IOException {
            freed = in.readBoolean();
        }

        SearchFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(freed);
        }
    }

    static boolean keepStatesInContext(Version version) {
        return version.before(Version.V_7_10_0);
    }

    public static void registerRequestHandler(TransportService transportService, SearchService searchService) {
        transportService.registerRequestHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, ScrollFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeReaderContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
        });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_SCROLL_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(FREE_CONTEXT_ACTION_NAME, ThreadPool.Names.SAME, SearchFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeReaderContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
        });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, ThreadPool.Names.SAME,
            TransportRequest.Empty::new,
            (request, channel, task) -> {
                searchService.freeAllScrollContexts();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });
        TransportActionProxy.registerProxyAction(transportService, CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            (in) -> TransportResponse.Empty.INSTANCE);

        transportService.registerRequestHandler(DFS_ACTION_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) ->
                searchService.executeDfsPhase(request, keepStatesInContext(channel.getVersion()), (SearchShardTask) task,
                    new ChannelActionListener<>(channel, DFS_ACTION_NAME, request))
        );

        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, DfsSearchResult::new);

        transportService.registerRequestHandler(QUERY_ACTION_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, keepStatesInContext(channel.getVersion()), (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyActionWithDynamicResponseType(transportService, QUERY_ACTION_NAME,
            (request) -> ((ShardSearchRequest)request).numberOfShards() == 1 ? QueryFetchSearchResult::new : QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_ID_ACTION_NAME, ThreadPool.Names.SAME, QuerySearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ID_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_ID_ACTION_NAME, QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_SCROLL_ACTION_NAME, ScrollQuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_FETCH_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_FETCH_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_SCROLL_ACTION_NAME, ScrollQueryFetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, ShardFetchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_SCROLL_ACTION_NAME, FetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_ACTION_NAME, ThreadPool.Names.SAME, true, true, ShardFetchSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_ACTION_NAME, FetchSearchResult::new);

        // this is cheap, it does not fetch during the rewrite phase, so we can let it quickly execute on a networking thread
        transportService.registerRequestHandler(QUERY_CAN_MATCH_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.canMatch(request, new ChannelActionListener<>(channel, QUERY_CAN_MATCH_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_CAN_MATCH_NAME, SearchService.CanMatchResponse::new);
    }


    /**
     * Returns a connection to the given node on the provided cluster. If the cluster alias is <code>null</code> the node will be resolved
     * against the local cluster.
     * @param clusterAlias the cluster alias the node should be resolved against
     * @param node the node to resolve
     * @return a connection to the given node belonging to the cluster with the provided alias.
     */
    public Transport.Connection getConnection(@Nullable String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }

    final class ConnectionCountingHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
        private final Map<String, Long> clientConnections;
        private final String nodeId;

        ConnectionCountingHandler(final ActionListener<? super Response> listener, final Writeable.Reader<Response> responseReader,
                                  final Map<String, Long> clientConnections, final String nodeId) {
            super(listener, responseReader);
            this.clientConnections = clientConnections;
            this.nodeId = nodeId;
            // Increment the number of connections for this node by one
            clientConnections.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
        }

        @Override
        public void handleResponse(Response response) {
            super.handleResponse(response);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        @Override
        public void handleException(TransportException e) {
            super.handleException(e);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        private boolean assertNodePresent() {
            clientConnections.compute(nodeId, (id, conns) -> {
                assert conns != null : "number of connections for " + id + " is null, but should be an integer";
                assert conns >= 1 : "number of connections for " + id + " should be >= 1 but was " + conns;
                return conns;
            });
            // Always return true, there is additional asserting here, the boolean is just so this
            // can be skipped when assertions are not enabled
            return true;
        }
    }
}
