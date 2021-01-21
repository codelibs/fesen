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

package org.codelibs.fesen.client.transport;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.node.liveness.LivenessResponse;
import org.codelibs.fesen.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.codelibs.fesen.action.admin.cluster.state.ClusterStateAction;
import org.codelibs.fesen.action.admin.cluster.state.ClusterStateResponse;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.component.Lifecycle;
import org.codelibs.fesen.common.component.LifecycleListener;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.transport.BoundTransportAddress;
import org.codelibs.fesen.common.transport.TransportAddress;
import org.codelibs.fesen.transport.CloseableConnection;
import org.codelibs.fesen.transport.ConnectTransportException;
import org.codelibs.fesen.transport.ConnectionProfile;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportException;
import org.codelibs.fesen.transport.TransportMessageListener;
import org.codelibs.fesen.transport.TransportRequest;
import org.codelibs.fesen.transport.TransportRequestOptions;
import org.codelibs.fesen.transport.TransportResponse;
import org.codelibs.fesen.transport.TransportResponseHandler;
import org.codelibs.fesen.transport.TransportService;
import org.codelibs.fesen.transport.TransportStats;

abstract class FailAndRetryMockTransport<Response extends TransportResponse> implements Transport {

    private final Random random;
    private final ClusterName clusterName;
    private final RequestHandlers requestHandlers = new RequestHandlers();
    private final Object requestHandlerMutex = new Object();
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private TransportMessageListener listener;

    private boolean connectMode = true;

    private final AtomicInteger connectTransportExceptions = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicInteger successes = new AtomicInteger();
    private final Set<DiscoveryNode> triedNodes = new CopyOnWriteArraySet<>();

    FailAndRetryMockTransport(Random random, ClusterName clusterName) {
        this.random = new Random(random.nextLong());
        this.clusterName = clusterName;
    }

    protected abstract ClusterState getMockClusterState(DiscoveryNode node);

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> connectionListener) {
        connectionListener.onResponse(new CloseableConnection() {

            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
                //we make sure that nodes get added to the connected ones when calling addTransportAddress, by returning proper nodes info
                if (connectMode) {
                    if (TransportLivenessAction.NAME.equals(action)) {
                        TransportResponseHandler transportResponseHandler = responseHandlers.onResponseReceived(requestId, listener);
                        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);
                        transportResponseHandler.handleResponse(new LivenessResponse(clusterName, node));
                    } else if (ClusterStateAction.NAME.equals(action)) {
                        TransportResponseHandler transportResponseHandler = responseHandlers.onResponseReceived(requestId, listener);
                        ClusterState clusterState = getMockClusterState(node);
                        transportResponseHandler.handleResponse(new ClusterStateResponse(clusterName, clusterState, false));
                    } else if (TransportService.HANDSHAKE_ACTION_NAME.equals(action)) {
                        TransportResponseHandler transportResponseHandler = responseHandlers.onResponseReceived(requestId, listener);
                        Version version = node.getVersion();
                        transportResponseHandler.handleResponse(new TransportService.HandshakeResponse(node, clusterName, version));

                    } else {
                        throw new UnsupportedOperationException("Mock transport does not understand action " + action);
                    }
                    return;
                }

                //once nodes are connected we'll just return errors for each sendRequest call
                triedNodes.add(node);

                if (random.nextInt(100) > 10) {
                    connectTransportExceptions.incrementAndGet();
                    throw new ConnectTransportException(node, "node not available");
                } else {
                    if (random.nextBoolean()) {
                        failures.incrementAndGet();
                        //throw whatever exception that is not a subclass of ConnectTransportException
                        throw new IllegalStateException();
                    } else {
                        TransportResponseHandler transportResponseHandler = responseHandlers.onResponseReceived(requestId, listener);
                        if (random.nextBoolean()) {
                            successes.incrementAndGet();
                            transportResponseHandler.handleResponse(newResponse());
                        } else {
                            failures.incrementAndGet();
                            transportResponseHandler.handleException(new TransportException("transport exception"));
                        }
                    }
                }
            }
        });
    }

    protected abstract Response newResponse();

    public void endConnectMode() {
        this.connectMode = false;
    }

    public int connectTransportExceptions() {
        return connectTransportExceptions.get();
    }

    public int failures() {
        return failures.get();
    }

    public int successes() {
        return successes.get();
    }

    public Set<DiscoveryNode> triedNodes() {
        return triedNodes;
    }


    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void close() {}

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return Collections.emptyMap();
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public RequestHandlers getRequestHandlers() {
        return requestHandlers;
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        this.listener = listener;
    }
}
