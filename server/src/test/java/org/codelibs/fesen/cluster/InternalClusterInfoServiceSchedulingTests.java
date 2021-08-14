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

package org.codelibs.fesen.cluster;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.action.admin.indices.stats.IndicesStatsRequest;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.InternalClusterInfoService;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.coordination.DeterministicTaskQueue;
import org.codelibs.fesen.cluster.coordination.MockSinglePrioritizingExecutor;
import org.codelibs.fesen.cluster.coordination.NoMasterBlockService;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.cluster.service.ClusterApplier;
import org.codelibs.fesen.cluster.service.ClusterApplierService;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.cluster.service.FakeThreadPoolMasterService;
import org.codelibs.fesen.cluster.service.MasterService;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.codelibs.fesen.node.Node;
import org.codelibs.fesen.test.ClusterServiceUtils;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.client.NoOpClient;
import org.codelibs.fesen.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.codelibs.fesen.cluster.InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class InternalClusterInfoServiceSchedulingTests extends ESTestCase {

    public void testScheduling() {
        final DiscoveryNode discoveryNode = new DiscoveryNode("test", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNodes noMaster = DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build();
        final DiscoveryNodes localMaster = DiscoveryNodes.builder(noMaster).masterNodeId(discoveryNode.getId()).build();

        final Settings.Builder settingsBuilder = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), discoveryNode.getName());
        if (randomBoolean()) {
            settingsBuilder.put(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), randomIntBetween(10000, 60000) + "ms");
        }
        final Settings settings = settingsBuilder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ClusterApplierService clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return new MockSinglePrioritizingExecutor("mock-executor", deterministicTaskQueue, threadPool);
            }
        };

        final MasterService masterService = new FakeThreadPoolMasterService("test", "masterService", threadPool, r -> {
            fail("master service should not run any tasks");
        });

        final ClusterService clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);

        final FakeClusterInfoServiceClient client = new FakeClusterInfoServiceClient(threadPool);
        final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        clusterService.addListener(clusterInfoService);
        clusterInfoService.addListener(ignored -> {});

        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterApplierService.setInitialState(ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build());
        masterService.setClusterStatePublisher((clusterChangedEvent, publishListener, ackListener) -> fail("should not publish"));
        masterService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();

        final AtomicBoolean becameMaster1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState("become master 1",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(localMaster).build(), setFlagOnSuccess(becameMaster1));
        runUntilFlag(deterministicTaskQueue, becameMaster1);

        final AtomicBoolean failMaster1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState("fail master 1",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build(), setFlagOnSuccess(failMaster1));
        runUntilFlag(deterministicTaskQueue, failMaster1);

        final AtomicBoolean becameMaster2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState("become master 2",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(localMaster).build(), setFlagOnSuccess(becameMaster2));
        runUntilFlag(deterministicTaskQueue, becameMaster2);

        for (int i = 0; i < 3; i++) {
            final int initialRequestCount = client.requestCount;
            final long duration = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis();
            runFor(deterministicTaskQueue, duration);
            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(client.requestCount, equalTo(initialRequestCount + 2)); // should have run two client requests per interval
        }

        final AtomicBoolean failMaster2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState("fail master 2",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build(), setFlagOnSuccess(failMaster2));
        runUntilFlag(deterministicTaskQueue, failMaster2);

        runFor(deterministicTaskQueue, INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis());
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }

    private static void runFor(DeterministicTaskQueue deterministicTaskQueue, long duration) {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + duration;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime
                && (deterministicTaskQueue.hasRunnableTasks() || deterministicTaskQueue.hasDeferredTasks())) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static void runUntilFlag(DeterministicTaskQueue deterministicTaskQueue, AtomicBoolean flag) {
        while (flag.get() == false) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static ClusterApplier.ClusterApplyListener setFlagOnSuccess(AtomicBoolean flag) {
        return new ClusterApplier.ClusterApplyListener() {

            @Override
            public void onSuccess(String source) {
                assertTrue(flag.compareAndSet(false, true));
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError("unexpected", e);
            }
        };
    }

    private static class FakeClusterInfoServiceClient extends NoOpClient {

        int requestCount;

        FakeClusterInfoServiceClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (request instanceof NodesStatsRequest || request instanceof IndicesStatsRequest) {
                requestCount++;
                // ClusterInfoService handles ClusterBlockExceptions quietly, so we invent such an exception to avoid excess logging
                listener.onFailure(new ClusterBlockException(
                        org.codelibs.fesen.core.Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL)));
            } else {
                fail("unexpected action: " + action.name());
            }
        }
    }

}
