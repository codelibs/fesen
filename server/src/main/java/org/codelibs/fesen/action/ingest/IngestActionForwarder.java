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

import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionListenerResponseHandler;
import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.cluster.ClusterChangedEvent;
import org.codelibs.fesen.cluster.ClusterStateApplier;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.Randomness;
import org.codelibs.fesen.transport.TransportService;

/**
 * A utility for forwarding ingest requests to ingest nodes in a round-robin fashion.
 *
 * TODO: move this into IngestService and make index/bulk actions call that
 */
public final class IngestActionForwarder implements ClusterStateApplier {

    private final TransportService transportService;
    private final AtomicInteger ingestNodeGenerator = new AtomicInteger(Randomness.get().nextInt());
    private DiscoveryNode[] ingestNodes;

    public IngestActionForwarder(TransportService transportService) {
        this.transportService = transportService;
        ingestNodes = new DiscoveryNode[0];
    }

    public void forwardIngestRequest(ActionType<?> action, ActionRequest request, ActionListener<?> listener) {
        transportService.sendRequest(randomIngestNode(), action.name(), request,
            new ActionListenerResponseHandler(listener, action.getResponseReader()));
    }

    private DiscoveryNode randomIngestNode() {
        final DiscoveryNode[] nodes = ingestNodes;
        if (nodes.length == 0) {
            throw new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.");
        }

        return nodes[Math.floorMod(ingestNodeGenerator.incrementAndGet(), nodes.length)];
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ingestNodes = event.state().getNodes().getIngestNodes().values().toArray(DiscoveryNode.class);
    }
}
