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

import org.codelibs.fesen.cluster.action.index.MappingUpdatedAction;
import org.codelibs.fesen.cluster.action.index.NodeMappingRefreshAction;
import org.codelibs.fesen.cluster.action.shard.ShardStateAction;
import org.codelibs.fesen.cluster.metadata.ComponentTemplateMetadata;
import org.codelibs.fesen.cluster.metadata.ComposableIndexTemplateMetadata;
import org.codelibs.fesen.cluster.metadata.DataStreamMetadata;
import org.codelibs.fesen.cluster.metadata.IndexGraveyard;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.metadata.MetadataDeleteIndexService;
import org.codelibs.fesen.cluster.metadata.MetadataIndexAliasesService;
import org.codelibs.fesen.cluster.metadata.MetadataIndexStateService;
import org.codelibs.fesen.cluster.metadata.MetadataIndexTemplateService;
import org.codelibs.fesen.cluster.metadata.MetadataMappingService;
import org.codelibs.fesen.cluster.metadata.MetadataUpdateSettingsService;
import org.codelibs.fesen.cluster.metadata.RepositoriesMetadata;
import org.codelibs.fesen.cluster.routing.DelayedAllocationService;
import org.codelibs.fesen.cluster.routing.allocation.AllocationService;
import org.codelibs.fesen.cluster.routing.allocation.ExistingShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.allocator.ShardsAllocator;
import org.codelibs.fesen.cluster.routing.allocation.decider.AllocationDecider;
import org.codelibs.fesen.cluster.routing.allocation.decider.AllocationDeciders;
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
import org.codelibs.fesen.common.ParseField;
import org.codelibs.fesen.common.inject.AbstractModule;
import org.codelibs.fesen.common.io.stream.NamedWriteable;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry.Entry;
import org.codelibs.fesen.common.io.stream.Writeable.Reader;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.Setting.Property;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.common.util.set.Sets;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.gateway.GatewayAllocator;
import org.codelibs.fesen.ingest.IngestMetadata;
import org.codelibs.fesen.persistent.PersistentTasksCustomMetadata;
import org.codelibs.fesen.persistent.PersistentTasksNodeService;
import org.codelibs.fesen.plugins.ClusterPlugin;
import org.codelibs.fesen.script.ScriptMetadata;
import org.codelibs.fesen.snapshots.SnapshotsInfoService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskResultsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Configures classes and services that affect the entire cluster.
 */
public class ClusterModule extends AbstractModule {

    public static final String BALANCED_ALLOCATOR = "balanced"; // default
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING =
        new Setting<>("cluster.routing.allocation.type", BALANCED_ALLOCATOR, Function.identity(), Property.NodeScope);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final List<ClusterPlugin> clusterPlugins;
    // pkg private for tests
    final Collection<AllocationDecider> deciderList;
    final ShardsAllocator shardsAllocator;

    public ClusterModule(Settings settings, ClusterService clusterService, List<ClusterPlugin> clusterPlugins,
                         ClusterInfoService clusterInfoService, SnapshotsInfoService snapshotsInfoService, ThreadContext threadContext) {
        this.clusterPlugins = clusterPlugins;
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(deciderList);
        this.shardsAllocator = createShardsAllocator(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext);
        this.allocationService = new AllocationService(allocationDeciders, shardsAllocator, clusterInfoService, snapshotsInfoService);
    }

    public static List<Entry> getNamedWriteables() {
        List<Entry> entries = new ArrayList<>();
        // Cluster State
        registerClusterCustom(entries, SnapshotsInProgress.TYPE, SnapshotsInProgress::new, SnapshotsInProgress::readDiffFrom);
        registerClusterCustom(entries, RestoreInProgress.TYPE, RestoreInProgress::new, RestoreInProgress::readDiffFrom);
        registerClusterCustom(entries, SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress::new,
            SnapshotDeletionsInProgress::readDiffFrom);
        registerClusterCustom(entries, RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress::new,
            RepositoryCleanupInProgress::readDiffFrom);
        // Metadata
        registerMetadataCustom(entries, RepositoriesMetadata.TYPE, RepositoriesMetadata::new, RepositoriesMetadata::readDiffFrom);
        registerMetadataCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerMetadataCustom(entries, ScriptMetadata.TYPE, ScriptMetadata::new, ScriptMetadata::readDiffFrom);
        registerMetadataCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        registerMetadataCustom(entries, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new,
            PersistentTasksCustomMetadata::readDiffFrom);
        registerMetadataCustom(entries, ComponentTemplateMetadata.TYPE, ComponentTemplateMetadata::new,
            ComponentTemplateMetadata::readDiffFrom);
        registerMetadataCustom(entries, ComposableIndexTemplateMetadata.TYPE, ComposableIndexTemplateMetadata::new,
            ComposableIndexTemplateMetadata::readDiffFrom);
        registerMetadataCustom(entries, DataStreamMetadata.TYPE, DataStreamMetadata::new, DataStreamMetadata::readDiffFrom);
        // Task Status (not Diffable)
        entries.add(new Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME, PersistentTasksNodeService.Status::new));
        return entries;
    }

    static final Set<String> PRE_6_3_METADATA_CUSTOMS_WHITE_LIST = Collections.unmodifiableSet(Sets.newHashSet(
        IndexGraveyard.TYPE, IngestMetadata.TYPE, RepositoriesMetadata.TYPE, ScriptMetadata.TYPE));

    static final Set<String> PRE_6_3_CLUSTER_CUSTOMS_WHITE_LIST = Collections.unmodifiableSet(Sets.newHashSet(
        RestoreInProgress.TYPE, SnapshotDeletionsInProgress.TYPE, SnapshotsInProgress.TYPE));

    /**
     * For interoperability with transport clients older than 6.3, we need to strip customs
     * from the cluster state that the client might not be able to deserialize
     *
     * @param clusterState the cluster state to filter the customs from
     * @return the adapted cluster state
     */
    public static ClusterState filterCustomsForPre63Clients(ClusterState clusterState) {
        final ClusterState.Builder builder = ClusterState.builder(clusterState);
        clusterState.customs().keysIt().forEachRemaining(name -> {
            if (PRE_6_3_CLUSTER_CUSTOMS_WHITE_LIST.contains(name) == false) {
                builder.removeCustom(name);
            }
        });
        final Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        clusterState.metadata().customs().keysIt().forEachRemaining(name -> {
            if (PRE_6_3_METADATA_CUSTOMS_WHITE_LIST.contains(name) == false) {
                metaBuilder.removeCustom(name);
            }
        });
        return builder.metadata(metaBuilder).build();
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(RepositoriesMetadata.TYPE),
            RepositoriesMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IngestMetadata.TYPE),
            IngestMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ScriptMetadata.TYPE),
            ScriptMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE),
            IndexGraveyard::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(PersistentTasksCustomMetadata.TYPE),
            PersistentTasksCustomMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ComponentTemplateMetadata.TYPE),
            ComponentTemplateMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ComposableIndexTemplateMetadata.TYPE),
            ComposableIndexTemplateMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(DataStreamMetadata.TYPE),
            DataStreamMetadata::fromXContent));
        return entries;
    }

    private static <T extends ClusterState.Custom> void registerClusterCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                       Reader<NamedDiff> diffReader) {
        registerCustom(entries, ClusterState.Custom.class, name, reader, diffReader);
    }

    private static <T extends Metadata.Custom> void registerMetadataCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                       Reader<NamedDiff> diffReader) {
        registerCustom(entries, Metadata.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(List<Entry> entries, Class<T> category, String name,
                                                                  Reader<? extends T> reader, Reader<NamedDiff> diffReader) {
        entries.add(new Entry(category, name, reader));
        entries.add(new Entry(NamedDiff.class, name, diffReader));
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    // TODO: this is public so allocation benchmark can access the default deciders...can we do that in another way?
    /** Return a new {@link AllocationDecider} instance with builtin deciders as well as those from plugins. */
    public static Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings,
                                                                         List<ClusterPlugin> clusterPlugins) {
        // collect deciders by class so that we can detect duplicates
        Map<Class, AllocationDecider> deciders = new LinkedHashMap<>();
        addAllocationDecider(deciders, new MaxRetryAllocationDecider());
        addAllocationDecider(deciders, new ResizeAllocationDecider());
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider());
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider());
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeVersionAllocationDecider());
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider());
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider());
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new SameShardAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new AwarenessAllocationDecider(settings, clusterSettings));

        clusterPlugins.stream()
            .flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream())
            .forEach(d -> addAllocationDecider(deciders, d));

        return deciders.values();
    }

    /** Add the given allocation decider to the given deciders collection, erroring if the class name is already used. */
    private static void addAllocationDecider(Map<Class, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    private static ShardsAllocator createShardsAllocator(Settings settings, ClusterSettings clusterSettings,
                                                         List<ClusterPlugin> clusterPlugins) {
        Map<String, Supplier<ShardsAllocator>> allocators = new HashMap<>();
        allocators.put(BALANCED_ALLOCATOR, () -> new BalancedShardsAllocator(settings, clusterSettings));

        for (ClusterPlugin plugin : clusterPlugins) {
            plugin.getShardsAllocators(settings, clusterSettings).forEach((k, v) -> {
                if (allocators.put(k, v) != null) {
                    throw new IllegalArgumentException("ShardsAllocator [" + k + "] already defined");
                }
            });
        }
        String allocatorName = SHARDS_ALLOCATOR_TYPE_SETTING.get(settings);
        Supplier<ShardsAllocator> allocatorSupplier = allocators.get(allocatorName);
        if (allocatorSupplier == null) {
            throw new IllegalArgumentException("Unknown ShardsAllocator [" + allocatorName + "]");
        }
        return Objects.requireNonNull(allocatorSupplier.get(),
            "ShardsAllocator factory for [" + allocatorName + "] returned null");
    }

    public AllocationService getAllocationService() {
        return allocationService;
    }

    @Override
    protected void configure() {
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).toInstance(allocationService);
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetadataDeleteIndexService.class).asEagerSingleton();
        bind(MetadataIndexStateService.class).asEagerSingleton();
        bind(MetadataMappingService.class).asEagerSingleton();
        bind(MetadataIndexAliasesService.class).asEagerSingleton();
        bind(MetadataUpdateSettingsService.class).asEagerSingleton();
        bind(MetadataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(allocationDeciders);
        bind(ShardsAllocator.class).toInstance(shardsAllocator);
    }

    public void setExistingShardsAllocators(GatewayAllocator gatewayAllocator) {
        final Map<String, ExistingShardsAllocator> existingShardsAllocators = new HashMap<>();
        existingShardsAllocators.put(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator);

        for (ClusterPlugin clusterPlugin : clusterPlugins) {
            for (Map.Entry<String, ExistingShardsAllocator> existingShardsAllocatorEntry
                : clusterPlugin.getExistingShardsAllocators().entrySet()) {
                final String allocatorName = existingShardsAllocatorEntry.getKey();
                if (existingShardsAllocators.put(allocatorName, existingShardsAllocatorEntry.getValue()) != null) {
                    throw new IllegalArgumentException("ExistingShardsAllocator [" + allocatorName + "] from [" +
                        clusterPlugin.getClass().getName() + "] was already defined");
                }
            }
        }
        allocationService.setExistingShardsAllocators(existingShardsAllocators);
    }

}