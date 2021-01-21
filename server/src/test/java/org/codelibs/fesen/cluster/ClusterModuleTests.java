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

package org.codelibs.fesen.cluster;

import org.codelibs.fesen.cluster.ClusterInfoService;
import org.codelibs.fesen.cluster.ClusterModule;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.EmptyClusterInfoService;
import org.codelibs.fesen.cluster.RestoreInProgress;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.metadata.RepositoriesMetadata;
import org.codelibs.fesen.cluster.routing.ShardRouting;
import org.codelibs.fesen.cluster.routing.allocation.ExistingShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.RoutingAllocation;
import org.codelibs.fesen.cluster.routing.allocation.ShardAllocationDecision;
import org.codelibs.fesen.cluster.routing.allocation.allocator.ShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.decider.AllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.ModuleTestCase;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.IndexScopedSettings;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsModule;
import org.codelibs.fesen.common.settings.Setting.Property;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.gateway.GatewayAllocator;
import org.codelibs.fesen.plugins.ClusterPlugin;
import org.codelibs.fesen.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ClusterModuleTests extends ModuleTestCase {
    private ClusterInfoService clusterInfoService = EmptyClusterInfoService.INSTANCE;
    private ClusterService clusterService;
    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
        clusterService = new ClusterService(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    static class FakeAllocationDecider extends AllocationDecider {
        protected FakeAllocationDecider() {
        }
    }

    static class FakeShardsAllocator implements ShardsAllocator {
        @Override
        public void allocate(RoutingAllocation allocation) {
            // noop
        }
        @Override
        public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
            throw new UnsupportedOperationException("explain API not supported on FakeShardsAllocator");
        }
    }

    public void testRegisterClusterDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterClusterDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY,
            Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope));
        assertInstanceBinding(module, ClusterSettings.class, service -> service.isDynamicSetting("foo.bar"));
    }

    public void testRegisterIndexDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                "Cannot register setting [" + EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterIndexDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY,
            Setting.boolSetting("index.foo.bar", false, Property.Dynamic, Property.IndexScope));
        assertInstanceBinding(module, IndexScopedSettings.class, service -> service.isDynamicSetting("index.foo.bar"));
    }

    public void testRegisterAllocationDeciderDuplicate() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ClusterModule(Settings.EMPTY, clusterService,
                Collections.<ClusterPlugin>singletonList(new ClusterPlugin() {
                    @Override
                    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                        return Collections.singletonList(new EnableAllocationDecider(settings, clusterSettings));
                    }
                }), clusterInfoService, null, threadContext));
        assertEquals(e.getMessage(),
            "Cannot specify allocation decider [" + EnableAllocationDecider.class.getName() + "] twice");
    }

    public void testRegisterAllocationDecider() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService,
            Collections.singletonList(new ClusterPlugin() {
                @Override
                public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                    return Collections.singletonList(new FakeAllocationDecider());
                }
            }), clusterInfoService, null, threadContext);
        assertTrue(module.deciderList.stream().anyMatch(d -> d.getClass().equals(FakeAllocationDecider.class)));
    }

    private ClusterModule newClusterModuleWithShardsAllocator(Settings settings, String name, Supplier<ShardsAllocator> supplier) {
        return new ClusterModule(settings, clusterService, Collections.singletonList(
            new ClusterPlugin() {
                @Override
                public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
                    return Collections.singletonMap(name, supplier);
                }
            }
        ), clusterInfoService, null, threadContext);
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "custom").build();
        ClusterModule module = newClusterModuleWithShardsAllocator(settings, "custom", FakeShardsAllocator::new);
        assertEquals(FakeShardsAllocator.class, module.shardsAllocator.getClass());
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            newClusterModuleWithShardsAllocator(Settings.EMPTY, ClusterModule.BALANCED_ALLOCATOR, FakeShardsAllocator::new));
        assertEquals("ShardsAllocator [" + ClusterModule.BALANCED_ALLOCATOR + "] already defined", e.getMessage());
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ClusterModule(settings, clusterService, Collections.emptyList(), clusterInfoService, null, threadContext));
        assertEquals("Unknown ShardsAllocator [dne]", e.getMessage());
    }

    public void testShardsAllocatorFactoryNull() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "bad").build();
        expectThrows(NullPointerException.class, () -> newClusterModuleWithShardsAllocator(settings, "bad", () -> null));
    }

    // makes sure that the allocation deciders are setup in the correct order, such that the
    // slower allocation deciders come last and we can exit early if there is a NO decision without
    // running them. If the order of the deciders is changed for a valid reason, the order should be
    // changed in the test too.
    public void testAllocationDeciderOrder() {
        List<Class<? extends AllocationDecider>> expectedDeciders = Arrays.asList(
            MaxRetryAllocationDecider.class,
            ResizeAllocationDecider.class,
            ReplicaAfterPrimaryActiveAllocationDecider.class,
            RebalanceOnlyWhenActiveAllocationDecider.class,
            ClusterRebalanceAllocationDecider.class,
            ConcurrentRebalanceAllocationDecider.class,
            EnableAllocationDecider.class,
            NodeVersionAllocationDecider.class,
            SnapshotInProgressAllocationDecider.class,
            RestoreInProgressAllocationDecider.class,
            FilterAllocationDecider.class,
            SameShardAllocationDecider.class,
            DiskThresholdDecider.class,
            ThrottlingAllocationDecider.class,
            ShardsLimitAllocationDecider.class,
            AwarenessAllocationDecider.class);
        Collection<AllocationDecider> deciders = ClusterModule.createAllocationDeciders(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), Collections.emptyList());
        Iterator<AllocationDecider> iter = deciders.iterator();
        int idx = 0;
        while (iter.hasNext()) {
            AllocationDecider decider = iter.next();
            assertSame(decider.getClass(), expectedDeciders.get(idx++));
        }
    }

    public void testPre63CustomsFiltering() {
        final String whiteListedClusterCustom = randomFrom(ClusterModule.PRE_6_3_CLUSTER_CUSTOMS_WHITE_LIST);
        final String whiteListedMetadataCustom = randomFrom(ClusterModule.PRE_6_3_METADATA_CUSTOMS_WHITE_LIST);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(whiteListedClusterCustom, new RestoreInProgress.Builder().build())
            .putCustom("other", new RestoreInProgress.Builder().build())
            .metadata(Metadata.builder()
                .putCustom(whiteListedMetadataCustom, new RepositoriesMetadata(Collections.emptyList()))
                .putCustom("other", new RepositoriesMetadata(Collections.emptyList()))
                .build())
            .build();

        assertNotNull(clusterState.custom(whiteListedClusterCustom));
        assertNotNull(clusterState.custom("other"));
        assertNotNull(clusterState.metadata().custom(whiteListedMetadataCustom));
        assertNotNull(clusterState.metadata().custom("other"));

        final ClusterState fixedClusterState = ClusterModule.filterCustomsForPre63Clients(clusterState);

        assertNotNull(fixedClusterState.custom(whiteListedClusterCustom));
        assertNull(fixedClusterState.custom("other"));
        assertNotNull(fixedClusterState.metadata().custom(whiteListedMetadataCustom));
        assertNull(fixedClusterState.metadata().custom("other"));
    }

    public void testRejectsReservedExistingShardsAllocatorName() {
        final ClusterModule clusterModule = new ClusterModule(Settings.EMPTY, clusterService,
            Collections.singletonList(existingShardsAllocatorPlugin(GatewayAllocator.ALLOCATOR_NAME)), clusterInfoService, null,
            threadContext);
        expectThrows(IllegalArgumentException.class, () -> clusterModule.setExistingShardsAllocators(new TestGatewayAllocator()));
    }

    public void testRejectsDuplicateExistingShardsAllocatorName() {
        final ClusterModule clusterModule = new ClusterModule(Settings.EMPTY, clusterService,
            Arrays.asList(existingShardsAllocatorPlugin("duplicate"), existingShardsAllocatorPlugin("duplicate")), clusterInfoService, null,
            threadContext);
        expectThrows(IllegalArgumentException.class, () -> clusterModule.setExistingShardsAllocators(new TestGatewayAllocator()));
    }

    private static ClusterPlugin existingShardsAllocatorPlugin(final String allocatorName) {
        return new ClusterPlugin() {
            @Override
            public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
                return Collections.singletonMap(allocatorName, new TestGatewayAllocator());
            }
        };
    }

}
