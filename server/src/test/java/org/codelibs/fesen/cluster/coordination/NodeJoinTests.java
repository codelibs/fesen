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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.ESAllocationTestCase;
import org.codelibs.fesen.cluster.block.ClusterBlocks;
import org.codelibs.fesen.cluster.coordination.CoordinationMetadata;
import org.codelibs.fesen.cluster.coordination.CoordinationStateRejectedException;
import org.codelibs.fesen.cluster.coordination.Coordinator;
import org.codelibs.fesen.cluster.coordination.ElectionStrategy;
import org.codelibs.fesen.cluster.coordination.FollowersChecker;
import org.codelibs.fesen.cluster.coordination.InMemoryPersistedState;
import org.codelibs.fesen.cluster.coordination.Join;
import org.codelibs.fesen.cluster.coordination.JoinHelper;
import org.codelibs.fesen.cluster.coordination.JoinRequest;
import org.codelibs.fesen.cluster.coordination.PublishRequest;
import org.codelibs.fesen.cluster.coordination.StartJoinRequest;
import org.codelibs.fesen.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.node.DiscoveryNodeRole;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.cluster.service.FakeThreadPoolMasterService;
import org.codelibs.fesen.cluster.service.MasterService;
import org.codelibs.fesen.cluster.service.MasterServiceTests;
import org.codelibs.fesen.common.Randomness;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.concurrent.BaseFuture;
import org.codelibs.fesen.common.util.concurrent.FutureUtils;
import org.codelibs.fesen.monitor.NodeHealthService;
import org.codelibs.fesen.monitor.StatusInfo;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.test.ClusterServiceUtils;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.transport.CapturingTransport;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.RequestHandlerRegistry;
import org.codelibs.fesen.transport.TestTransportChannel;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportRequest;
import org.codelibs.fesen.transport.TransportResponse;
import org.codelibs.fesen.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.codelibs.fesen.monitor.StatusInfo.Status.HEALTHY;
import static org.codelibs.fesen.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NodeJoinTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private Coordinator coordinator;
    private DeterministicTaskQueue deterministicTaskQueue;
    private Transport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(NodeJoinTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterService.close();
    }

    private static ClusterState initialState(DiscoveryNode localNode, long term, long version,
                                             VotingConfiguration config) {
        return ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId()))
            .metadata(Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                        .term(term)
                        .lastAcceptedConfiguration(config)
                        .lastCommittedConfiguration(config)
                    .build()))
            .version(version)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
    }

    private void setupFakeMasterServiceAndCoordinator(long term, ClusterState initialState, NodeHealthService nodeHealthService) {
        deterministicTaskQueue
            = new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        final ThreadPool fakeThreadPool = deterministicTaskQueue.getThreadPool();
        FakeThreadPoolMasterService fakeMasterService = new FakeThreadPoolMasterService("test_node","test",
            fakeThreadPool, deterministicTaskQueue::scheduleNow);
        setupMasterServiceAndCoordinator(term, initialState, fakeMasterService, fakeThreadPool, Randomness.get(), nodeHealthService);
        fakeMasterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            coordinator.handlePublishRequest(new PublishRequest(event.state()));
            publishListener.onResponse(null);
        });
        fakeMasterService.start();
    }

    private void setupRealMasterServiceAndCoordinator(long term, ClusterState initialState) {
        MasterService masterService = new MasterService(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool);
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialState);
        masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        setupMasterServiceAndCoordinator(term, initialState, masterService, threadPool, new Random(Randomness.get().nextLong()),
            () -> new StatusInfo(HEALTHY, "healthy-info"));
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
    }

    private void setupMasterServiceAndCoordinator(long term, ClusterState initialState, MasterService masterService,
                                                  ThreadPool threadPool, Random random, NodeHealthService nodeHealthService) {
        if (this.masterService != null || coordinator != null) {
            throw new IllegalStateException("method setupMasterServiceAndCoordinator can only be called once");
        }
        this.masterService = masterService;
        CapturingTransport capturingTransport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode destination) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(destination, initialState.getClusterName(),
                        destination.getVersion()));
                } else if (action.equals(JoinHelper.VALIDATE_JOIN_ACTION_NAME)) {
                    handleResponse(requestId, new TransportResponse.Empty());
                } else {
                    super.onSendRequest(requestId, action, request, destination);
                }
            }
        };
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TransportService transportService = capturingTransport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> initialState.nodes().getLocalNode(),
            clusterSettings, Collections.emptySet());
        coordinator = new Coordinator("test_node", Settings.EMPTY, clusterSettings,
            transportService, writableRegistry(),
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new InMemoryPersistedState(term, initialState), r -> emptyList(),
            new NoOpClusterApplier(),
            Collections.emptyList(),
            random, (s, p, r) -> {}, ElectionStrategy.DEFAULT_INSTANCE, nodeHealthService);
        transportService.start();
        transportService.acceptIncomingRequests();
        transport = capturingTransport;
        coordinator.start();
        coordinator.startInitialJoin();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean master) {
        final Set<DiscoveryNodeRole> roles;
        if (master) {
            roles = singleton(DiscoveryNodeRole.MASTER_ROLE);
        } else {
            roles = Collections.emptySet();
        }
        final String prefix = master ? "master_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    static class SimpleFuture extends BaseFuture<Void> {
        final String description;

        SimpleFuture(String description) {
            this.description = description;
        }

        public void markAsDone() {
            set(null);
        }

        public void markAsFailed(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return "future [" + description + "]";
        }
    }

    private SimpleFuture joinNodeAsync(final JoinRequest joinRequest) {
        final SimpleFuture future = new SimpleFuture("join of " + joinRequest + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        try {
            final RequestHandlerRegistry<JoinRequest> joinHandler = transport.getRequestHandlers().getHandler(JoinHelper.JOIN_ACTION_NAME);
            final ActionListener<TransportResponse> listener = new ActionListener<TransportResponse>() {

                @Override
                public void onResponse(TransportResponse transportResponse) {
                    logger.debug("{} completed", future);
                    future.markAsDone();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                    future.markAsFailed(e);
                }
            };

            joinHandler.processMessageReceived(joinRequest, new TestTransportChannel(listener));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
            future.markAsFailed(e);
        }
        return future;
    }

    private void joinNode(final JoinRequest joinRequest) {
        FutureUtils.get(joinNodeAsync(joinRequest));
    }

    private void joinNodeAndRun(final JoinRequest joinRequest) {
        SimpleFuture fut = joinNodeAsync(joinRequest);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        FutureUtils.get(fut);
    }

    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(randomFrom(node0, node1))),
            () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertFalse(isLocalNodeElectedMaster());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node1, newTerm,
            Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertEquals(Coordinator.Mode.LEADER, coordinator.getMode());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(coordinator.getStateForMasterService().nodes().isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node1)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, newTerm,
                Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion)))));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateStillElectsMasterThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinElectedLeaderWithHigherTerm() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());

        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm, Optional.empty()));
        assertThat(coordinator.getCurrentTerm(), greaterThanOrEqualTo(newerTerm));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        DiscoveryNode node2 = newNode(2, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node2)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture futNode0 = joinNodeAsync(new JoinRequest(node0, newTerm, Optional.of(
            new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode0.isDone());
        assertFalse(isLocalNodeElectedMaster());
        SimpleFuture futNode1 = joinNodeAsync(new JoinRequest(node1, newTerm, Optional.of(
            new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode1.isDone());
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node2, newTerm, Optional.of(new Join(node2, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        assertTrue(clusterStateHasNode(node2));
        FutureUtils.get(futNode0);
        FutureUtils.get(futNode1);
    }

    public void testJoinFollowerWithHigherTerm() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm,
            Optional.of(new Join(node1, node0, newerTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinUpdateVotingConfigExclusion() throws Exception {
        DiscoveryNode initialNode = newNode(0, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);

        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion = new CoordinationMetadata.VotingConfigExclusion(
                                                        CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER, "knownNodeName");

        setupFakeMasterServiceAndCoordinator(initialTerm, buildStateWithVotingConfigExclusion(initialNode, initialTerm,
            initialVersion, votingConfigExclusion),
            () -> new StatusInfo(HEALTHY, "healthy-info"));

        DiscoveryNode knownJoiningNode = new DiscoveryNode("knownNodeName", "newNodeId", buildNewFakeTransportAddress(),
                                                            emptyMap(), singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long newerTerm = newTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(knownJoiningNode, initialTerm,
            Optional.of(new Join(knownJoiningNode, initialNode, newerTerm, initialTerm, initialVersion))));

        assertTrue(MasterServiceTests.discoveryState(masterService).getVotingConfigExclusions().stream().anyMatch(exclusion -> {
            return "knownNodeName".equals(exclusion.getNodeName()) && "newNodeId".equals(exclusion.getNodeId());
        }));
    }

    private ClusterState buildStateWithVotingConfigExclusion(DiscoveryNode initialNode,
                                                             long initialTerm,
                                                             long initialVersion,
                                                             CoordinationMetadata.VotingConfigExclusion votingConfigExclusion) {
        ClusterState initialState = initialState(initialNode, initialTerm, initialVersion,
                                                    new VotingConfiguration(singleton(initialNode.getId())));
        Metadata newMetadata = Metadata.builder(initialState.metadata())
                                        .coordinationMetadata(CoordinationMetadata.builder(initialState.coordinationMetadata())
                                                                                    .addVotingConfigExclusion(votingConfigExclusion)
                                                                                    .build())
                                        .build();

        return ClusterState.builder(initialState).metadata(newMetadata).build();
    }

    private void handleStartJoinFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<StartJoinRequest> startJoinHandler = transport.getRequestHandlers()
            .getHandler(JoinHelper.START_JOIN_ACTION_NAME);
        startJoinHandler.processMessageReceived(new StartJoinRequest(node, term), new TestTransportChannel(
            new ActionListener<TransportResponse>() {
                @Override
                public void onResponse(TransportResponse transportResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                fail();
            }
        }));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.CANDIDATE));
    }

    private void handleFollowerCheckFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest> followerCheckHandler = transport.getRequestHandlers()
            .getHandler(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);
        final TestTransportChannel channel = new TestTransportChannel(new ActionListener<TransportResponse>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        followerCheckHandler.processMessageReceived(new FollowersChecker.FollowerCheckRequest(term, node), channel);
        // Will throw exception if failed
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.FOLLOWER));
    }

    public void testJoinFollowerFails() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.empty()))).getMessage(),
            containsString("join target is a follower"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testBecomeFollowerFailsPendingJoin() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node1)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node0, newTerm,
            Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        handleFollowerCheckFrom(node1, newTerm);
        assertFalse(isLocalNodeElectedMaster());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> FutureUtils.get(fut)).getMessage(),
            containsString("became follower"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testConcurrentJoining() {
        List<DiscoveryNode> masterNodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true)).collect(Collectors.toList());
        List<DiscoveryNode> otherNodes = IntStream.rangeClosed(masterNodes.size() + 1, masterNodes.size() + 1 + randomIntBetween(0, 5))
            .mapToObj(nodeId -> newNode(nodeId, false)).collect(Collectors.toList());
        List<DiscoveryNode> allNodes = Stream.concat(masterNodes.stream(), otherNodes.stream()).collect(Collectors.toList());

        DiscoveryNode localNode = masterNodes.get(0);
        VotingConfiguration votingConfiguration = new VotingConfiguration(randomValueOtherThan(singletonList(localNode),
            () -> randomSubsetOf(randomIntBetween(1, masterNodes.size()), masterNodes)).stream()
            .map(DiscoveryNode::getId).collect(Collectors.toSet()));

        logger.info("Voting configuration: {}", votingConfiguration);

        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupRealMasterServiceAndCoordinator(initialTerm, initialState(localNode, initialTerm, initialVersion, votingConfiguration));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(allNodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList()))
            == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream().map(
            node -> new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))))
            .collect(Collectors.toList());

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(allNodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
                    newTerm, initialTerm, initialVersion)));
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
                    randomLongBetween(0, initialTerm), initialTerm, initialVersion)));
            } else {
                // better state
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
                    newTerm, initialTerm, initialVersion + randomLongBetween(1, 10))));
            }
        }).collect(Collectors.toList());

        // duplicate some requests, which will be unsuccessful
        possiblyFailingJoinRequests.addAll(randomSubsetOf(possiblyFailingJoinRequests));

        CyclicBarrier barrier = new CyclicBarrier(correctJoinRequests.size() + possiblyFailingJoinRequests.size() + 1);
        final Runnable awaitBarrier = () -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        };

        final AtomicBoolean stopAsserting = new AtomicBoolean();
        final Thread assertionThread = new Thread(() -> {
            awaitBarrier.run();
            while (stopAsserting.get() == false) {
                coordinator.invariant();
            }
        }, "assert invariants");

        final List<Thread> joinThreads = Stream.concat(correctJoinRequests.stream().map(joinRequest ->
            new Thread(() -> {
                awaitBarrier.run();
                joinNode(joinRequest);
            }, "process " + joinRequest)), possiblyFailingJoinRequests.stream().map(joinRequest ->
            new Thread(() -> {
                awaitBarrier.run();
                try {
                    joinNode(joinRequest);
                } catch (CoordinationStateRejectedException e) {
                    // ignore - these requests are expected to fail
                }
            }, "process " + joinRequest))).collect(Collectors.toList());

        assertionThread.start();
        joinThreads.forEach(Thread::start);
        joinThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        stopAsserting.set(true);
        try {
            assertionThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster());
        for (DiscoveryNode successfulNode : successfulNodes) {
            assertTrue(successfulNode + " joined cluster", clusterStateHasNode(successfulNode));
            assertFalse(successfulNode + " voted for master", coordinator.missingJoinVoteFrom(successfulNode));
        }
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(masterService).nodes().get(node.getId()));
    }
}
