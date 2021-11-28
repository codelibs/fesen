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

package org.codelibs.fesen.gateway;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.ClusterStateUpdateTask;
import org.codelibs.fesen.cluster.EmptyClusterInfoService;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.block.ClusterBlocks;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.cluster.routing.allocation.AllocationService;
import org.codelibs.fesen.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.decider.AllocationDeciders;
import org.codelibs.fesen.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.transport.TransportAddress;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.gateway.GatewayService;
import org.codelibs.fesen.snapshots.EmptySnapshotsInfoService;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.gateway.TestGatewayAllocator;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.HashSet;

import static org.codelibs.fesen.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.codelibs.fesen.test.NodeRoles.masterNode;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;

public class GatewayServiceTests extends ESTestCase {

    private GatewayService createService(final Settings.Builder settings) {
        final ClusterService clusterService = new ClusterService(Settings.builder().put("cluster.name", "GatewayServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);
        final AllocationService allocationService = new AllocationService(
                new AllocationDeciders(new HashSet<>(Arrays.asList(
                        new SameShardAllocationDecider(Settings.EMPTY,
                                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                        new ReplicaAfterPrimaryActiveAllocationDecider()))),
                new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE);
        return new GatewayService(settings.build(), allocationService, clusterService, null, null, null);
    }

    public void testDefaultRecoverAfterTime() {
        // check that the default is not set
        GatewayService service = createService(Settings.builder());
        assertNull(service.recoverAfterTime());

        // ensure default is set when setting expected_data_nodes
        service = createService(Settings.builder().put("gateway.expected_data_nodes", 1));
        assertThat(service.recoverAfterTime(), Matchers.equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure settings override default
        final TimeValue timeValue = TimeValue.timeValueHours(3);
        // ensure default is set when setting expected_nodes
        service = createService(Settings.builder().put("gateway.recover_after_time", timeValue.toString()));
        assertThat(service.recoverAfterTime().millis(), Matchers.equalTo(timeValue.millis()));
    }

    public void testDeprecatedSettings() {
        GatewayService service = createService(Settings.builder());

        service = createService(Settings.builder().put("gateway.expected_nodes", 1));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { GatewayService.EXPECTED_NODES_SETTING });

        service = createService(Settings.builder().put("gateway.expected_master_nodes", 1));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { GatewayService.EXPECTED_MASTER_NODES_SETTING });

        service = createService(Settings.builder().put("gateway.recover_after_nodes", 1));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { GatewayService.RECOVER_AFTER_NODES_SETTING });

        service = createService(Settings.builder().put("gateway.recover_after_master_nodes", 1));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING });
    }

    public void testRecoverStateUpdateTask() throws Exception {
        GatewayService service = createService(Settings.builder());
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask();
        String nodeId = randomAlphaOfLength(10);
        DiscoveryNode masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT).put(masterNode()).build(),
                new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        ClusterState stateWithBlock = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build()).build();

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertNotEquals(recoveredState, stateWithBlock);
        assertThat(recoveredState.blocks().global(ClusterBlockLevel.METADATA_WRITE), not(hasItem(STATE_NOT_RECOVERED_BLOCK)));

        ClusterState clusterState = clusterStateUpdateTask.execute(recoveredState);
        assertSame(recoveredState, clusterState);
    }

}
