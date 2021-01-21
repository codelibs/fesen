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
package org.codelibs.fesen.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListenerResponseHandler;
import org.codelibs.fesen.action.support.PlainActionFuture;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.NotMasterException;
import org.codelibs.fesen.cluster.coordination.CoordinationStateRejectedException;
import org.codelibs.fesen.cluster.coordination.FailedToCommitClusterStateException;
import org.codelibs.fesen.cluster.coordination.Join;
import org.codelibs.fesen.cluster.coordination.JoinHelper;
import org.codelibs.fesen.cluster.coordination.ValidateJoinRequest;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.discovery.zen.MembershipAction;
import org.codelibs.fesen.monitor.StatusInfo;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.transport.CapturingTransport;
import org.codelibs.fesen.test.transport.MockTransport;
import org.codelibs.fesen.test.transport.CapturingTransport.CapturedRequest;
import org.codelibs.fesen.transport.RemoteTransportException;
import org.codelibs.fesen.transport.TransportException;
import org.codelibs.fesen.transport.TransportResponse;
import org.codelibs.fesen.transport.TransportService;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.codelibs.fesen.monitor.StatusInfo.Status.HEALTHY;
import static org.codelibs.fesen.monitor.StatusInfo.Status.UNHEALTHY;
import static org.codelibs.fesen.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class JoinHelperTests extends ESTestCase {

    public void testJoinDeduplication() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(), random());
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = capturingTransport.createTransportService(Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode, null, Collections.emptySet());
        JoinHelper joinHelper = new JoinHelper(Settings.EMPTY, null, null, transportService, () -> 0L, () -> null,
            (joinRequest, joinCallback) -> { throw new AssertionError(); }, startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(), (s, p, r) -> {},
            () -> new StatusInfo(HEALTHY, "info"));
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 works
        Optional<Join> optionalJoin1 = randomBoolean() ? Optional.empty() :
            Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(1));
        CapturedRequest capturedRequest1 = capturedRequests1[0];
        assertEquals(node1, capturedRequest1.node);

        assertTrue(joinHelper.isJoinPending());

        // check that sending a join to node2 works
        Optional<Join> optionalJoin2 = randomBoolean() ? Optional.empty() :
            Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2);
        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(1));
        CapturedRequest capturedRequest2 = capturedRequests2[0];
        assertEquals(node2, capturedRequest2.node);

        // check that sending another join to node1 is a noop as the previous join is still in progress
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        assertThat(capturingTransport.getCapturedRequestsAndClear().length, equalTo(0));

        // complete the previous join to node1
        if (randomBoolean()) {
            capturingTransport.handleResponse(capturedRequest1.requestId, TransportResponse.Empty.INSTANCE);
        } else {
            capturingTransport.handleRemoteError(capturedRequest1.requestId, new CoordinationStateRejectedException("dummy"));
        }

        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node);

        // check that sending another join to node2 works if the optionalJoin is different
        Optional<Join> optionalJoin2a = optionalJoin2.isPresent() && randomBoolean() ? Optional.empty() :
            Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2a);
        CapturedRequest[] capturedRequests2a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2a.length, equalTo(1));
        CapturedRequest capturedRequest2a = capturedRequests2a[0];
        assertEquals(node2, capturedRequest2a.node);

        // complete all the joins and check that isJoinPending is updated
        assertTrue(joinHelper.isJoinPending());
        capturingTransport.handleRemoteError(capturedRequest2.requestId, new CoordinationStateRejectedException("dummy"));
        capturingTransport.handleRemoteError(capturedRequest1a.requestId, new CoordinationStateRejectedException("dummy"));
        capturingTransport.handleRemoteError(capturedRequest2a.requestId, new CoordinationStateRejectedException("dummy"));
        assertFalse(joinHelper.isJoinPending());
    }

    public void testFailedJoinAttemptLogLevel() {
        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(new TransportException("generic transport exception")), is(Level.INFO));

        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("remote transport exception with generic cause", new Exception())), is(Level.INFO));

        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by CoordinationStateRejectedException",
                        new CoordinationStateRejectedException("test"))), is(Level.DEBUG));

        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by FailedToCommitClusterStateException",
                        new FailedToCommitClusterStateException("test"))), is(Level.DEBUG));

        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by NotMasterException",
                        new NotMasterException("test"))), is(Level.DEBUG));
    }

    public void testZen1JoinValidationRejectsMismatchedClusterUUID() {
        assertJoinValidationRejectsMismatchedClusterUUID(MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            "mixed-version cluster join validation on cluster state with a different cluster uuid");
    }

    public void testJoinValidationRejectsMismatchedClusterUUID() {
        assertJoinValidationRejectsMismatchedClusterUUID(JoinHelper.VALIDATE_JOIN_ACTION_NAME,
            "join validation on cluster state with a different cluster uuid");
    }

    private void assertJoinValidationRejectsMismatchedClusterUUID(String actionName, String expectedMessage) {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(), random());
        MockTransport mockTransport = new MockTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .generateClusterUuidIfNeeded().clusterUUIDCommitted(true)).build();

        TransportService transportService = mockTransport.createTransportService(Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode, null, Collections.emptySet());
        new JoinHelper(Settings.EMPTY, null, null, transportService, () -> 0L, () -> localClusterState,
            (joinRequest, joinCallback) -> { throw new AssertionError(); }, startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(), (s, p, r) -> {}, null); // registers request handler
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterState otherClusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .generateClusterUuidIfNeeded()).build();

        final PlainActionFuture<TransportResponse.Empty> future = new PlainActionFuture<>();
        transportService.sendRequest(localNode, actionName,
            new ValidateJoinRequest(otherClusterState),
            new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE));
        deterministicTaskQueue.runAllTasks();

        final CoordinationStateRejectedException coordinationStateRejectedException
            = expectThrows(CoordinationStateRejectedException.class, future::actionGet);
        assertThat(coordinationStateRejectedException.getMessage(), containsString(expectedMessage));
        assertThat(coordinationStateRejectedException.getMessage(), containsString(localClusterState.metadata().clusterUUID()));
        assertThat(coordinationStateRejectedException.getMessage(), containsString(otherClusterState.metadata().clusterUUID()));
    }

    public void testJoinFailureOnUnhealthyNodes() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(), random());
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = capturingTransport.createTransportService(Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode, null, Collections.emptySet());
        AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>
            (new StatusInfo(UNHEALTHY, "unhealthy-info"));
        JoinHelper joinHelper = new JoinHelper(Settings.EMPTY, null, null, transportService, () -> 0L, () -> null,
            (joinRequest, joinCallback) -> { throw new AssertionError(); }, startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(), (s, p, r) -> {}, () -> nodeHealthServiceStatus.get());
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 doesn't work
        Optional<Join> optionalJoin1 = randomBoolean() ? Optional.empty() :
            Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, randomNonNegativeLong(), optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node2 doesn't work
        Optional<Join> optionalJoin2 = randomBoolean() ? Optional.empty() :
            Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));

        transportService.start();
        joinHelper.sendJoinRequest(node2, randomNonNegativeLong(), optionalJoin2);

        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        nodeHealthServiceStatus.getAndSet(new StatusInfo(HEALTHY, "healthy-info"));
        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node);
    }
}
