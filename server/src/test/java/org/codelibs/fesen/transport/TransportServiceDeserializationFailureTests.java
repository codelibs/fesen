/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.transport;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.support.PlainActionFuture;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.coordination.DeterministicTaskQueue;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskAwareRequest;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.transport.MockTransport;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportException;
import org.codelibs.fesen.transport.TransportRequest;
import org.codelibs.fesen.transport.TransportRequestOptions;
import org.codelibs.fesen.transport.TransportResponse;
import org.codelibs.fesen.transport.TransportResponseHandler;
import org.codelibs.fesen.transport.TransportService;

import java.util.Collections;
import java.util.List;

import static org.codelibs.fesen.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class TransportServiceDeserializationFailureTests extends ESTestCase {

    public void testDeserializationFailureLogIdentifiesListener() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "local").build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());

        final String testActionName = "internal:test-action";

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(otherNode, new ClusterName(""), Version.CURRENT));
                }
            }
        };
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, deterministicTaskQueue.getThreadPool(),
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, ignored -> localNode, null, Collections.emptySet());

        transportService.registerRequestHandler(testActionName, ThreadPool.Names.SAME, TransportRequest.Empty::new,
                (request, channel, task) -> channel.sendResponse(TransportResponse.Empty.INSTANCE));

        transportService.start();
        transportService.acceptIncomingRequests();

        final PlainActionFuture<Void> connectionFuture = new PlainActionFuture<>();
        transportService.connectToNode(otherNode, connectionFuture);
        assertTrue(connectionFuture.isDone());

        {
            // requests without a parent task are recorded directly in the response context

            transportService.sendRequest(otherNode, testActionName, TransportRequest.Empty.INSTANCE, TransportRequestOptions.EMPTY,
                    new TransportResponseHandler<TransportResponse.Empty>() {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            fail("should not be called");
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            fail("should not be called");
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            throw new AssertionError("should not be called");
                        }

                        @Override
                        public String toString() {
                            return "test handler without parent";
                        }
                    });

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts =
                    transport.getResponseHandlers().prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(containsString("test handler without parent")));
        }

        {
            // requests with a parent task get wrapped up by the transport service, including the action name

            final Task parentTask = transportService.getTaskManager().register("test", "test-action", new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {
                    fail("should not be called");
                }

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }
            });

            transportService.sendChildRequest(otherNode, testActionName, TransportRequest.Empty.INSTANCE, parentTask,
                    TransportRequestOptions.EMPTY, new TransportResponseHandler<TransportResponse.Empty>() {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            fail("should not be called");
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            fail("should not be called");
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            throw new AssertionError("should not be called");
                        }

                        @Override
                        public String toString() {
                            return "test handler with parent";
                        }
                    });

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts =
                    transport.getResponseHandlers().prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(allOf(containsString("test handler with parent"), containsString(testActionName))));
        }
    }

}
