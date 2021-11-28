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

package org.codelibs.fesen.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.support.replication.ClusterStateCreationUtils;
import org.codelibs.fesen.cluster.ClusterInfo;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.ESAllocationTestCase;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.cluster.routing.RoutingNode;
import org.codelibs.fesen.cluster.routing.RoutingNodes;
import org.codelibs.fesen.cluster.routing.RoutingTable;
import org.codelibs.fesen.cluster.routing.ShardRouting;
import org.codelibs.fesen.cluster.routing.ShardRoutingState;
import org.codelibs.fesen.cluster.routing.TestShardRouting;
import org.codelibs.fesen.cluster.routing.allocation.AllocationService;
import org.codelibs.fesen.cluster.routing.allocation.RoutingAllocation;
import org.codelibs.fesen.cluster.routing.allocation.decider.AllocationDeciders;
import org.codelibs.fesen.cluster.routing.allocation.decider.Decision;
import org.codelibs.fesen.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.codelibs.fesen.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.codelibs.fesen.cluster.routing.allocation.RoutingNodesUtils.numberOfShardsOfType;
import static org.hamcrest.Matchers.equalTo;

public class SameShardRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(SameShardRoutingTests.class);

    public void testSameHost() {
        AllocationService strategy = createAllocationService(
                Settings.builder().put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true).build());

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metadata(metadata)
                .routingTable(routingTable).build();

        logger.info("--> adding two nodes with the same host");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("node1", "node1", "node1", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
                                MASTER_DATA_ROLES, Version.CURRENT))
                        .add(new DiscoveryNode("node2", "node2", "node2", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
                                MASTER_DATA_ROLES, Version.CURRENT)))
                .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards, no replica will be started since its on the same host");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(0));

        logger.info("--> add another node, with a different host, replicas will be allocating");
        clusterState =
                ClusterState
                        .builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(new DiscoveryNode("node3", "node3",
                                "node3", "test2", "test2", buildNewFakeTransportAddress(), emptyMap(), MASTER_DATA_ROLES, Version.CURRENT)))
                        .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().shardsWithState(INITIALIZING)) {
            assertThat(shardRouting.currentNodeId(), equalTo("node3"));
        }
    }

    public void testForceAllocatePrimaryOnSameNodeNotAllowed() {
        SameShardAllocationDecider decider = new SameShardAllocationDecider(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomIntBetween(2, 4), 1);
        Index index = clusterState.getMetadata().index("idx").getIndex();
        ShardRouting primaryShard = clusterState.routingTable().index(index).shard(0).primaryShard();
        RoutingNode routingNode = clusterState.getRoutingNodes().node(primaryShard.currentNodeId());
        RoutingAllocation routingAllocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
                new RoutingNodes(clusterState, false), clusterState, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, System.nanoTime());

        // can't force allocate same shard copy to the same node
        ShardRouting newPrimary = TestShardRouting.newShardRouting(primaryShard.shardId(), null, true, ShardRoutingState.UNASSIGNED);
        Decision decision = decider.canForceAllocatePrimary(newPrimary, routingNode, routingAllocation);
        assertEquals(Decision.Type.NO, decision.type());

        // can force allocate to a different node
        RoutingNode unassignedNode = null;
        for (RoutingNode node : clusterState.getRoutingNodes()) {
            if (node.isEmpty()) {
                unassignedNode = node;
                break;
            }
        }
        decision = decider.canForceAllocatePrimary(newPrimary, unassignedNode, routingAllocation);
        assertEquals(Decision.Type.YES, decision.type());
    }
}
