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

package org.codelibs.fesen.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.codelibs.fesen.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.codelibs.fesen.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.codelibs.fesen.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.codelibs.fesen.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.codelibs.fesen.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.action.admin.cluster.health.TransportClusterHealthAction;
import org.codelibs.fesen.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.codelibs.fesen.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.codelibs.fesen.action.admin.cluster.node.info.NodesInfoAction;
import org.codelibs.fesen.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.codelibs.fesen.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.codelibs.fesen.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.codelibs.fesen.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsAction;
import org.codelibs.fesen.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.usage.NodesUsageAction;
import org.codelibs.fesen.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.codelibs.fesen.action.admin.cluster.remote.RemoteInfoAction;
import org.codelibs.fesen.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.codelibs.fesen.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.codelibs.fesen.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.codelibs.fesen.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteAction;
import org.codelibs.fesen.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.codelibs.fesen.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.codelibs.fesen.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.codelibs.fesen.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.codelibs.fesen.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.codelibs.fesen.action.admin.cluster.state.ClusterStateAction;
import org.codelibs.fesen.action.admin.cluster.state.TransportClusterStateAction;
import org.codelibs.fesen.action.admin.cluster.stats.ClusterStatsAction;
import org.codelibs.fesen.action.admin.cluster.stats.TransportClusterStatsAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.codelibs.fesen.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistAction;
import org.codelibs.fesen.action.admin.indices.alias.exists.TransportAliasesExistAction;
import org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.codelibs.fesen.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.action.admin.indices.analyze.TransportAnalyzeAction;
import org.codelibs.fesen.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.codelibs.fesen.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.codelibs.fesen.action.admin.indices.close.CloseIndexAction;
import org.codelibs.fesen.action.admin.indices.close.TransportCloseIndexAction;
import org.codelibs.fesen.action.admin.indices.create.AutoCreateAction;
import org.codelibs.fesen.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.action.admin.indices.create.TransportCreateIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.codelibs.fesen.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.codelibs.fesen.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.codelibs.fesen.action.admin.indices.delete.DeleteIndexAction;
import org.codelibs.fesen.action.admin.indices.delete.TransportDeleteIndexAction;
import org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsAction;
import org.codelibs.fesen.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.codelibs.fesen.action.admin.indices.exists.types.TransportTypesExistsAction;
import org.codelibs.fesen.action.admin.indices.exists.types.TypesExistsAction;
import org.codelibs.fesen.action.admin.indices.flush.FlushAction;
import org.codelibs.fesen.action.admin.indices.flush.SyncedFlushAction;
import org.codelibs.fesen.action.admin.indices.flush.TransportFlushAction;
import org.codelibs.fesen.action.admin.indices.flush.TransportSyncedFlushAction;
import org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeAction;
import org.codelibs.fesen.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.codelibs.fesen.action.admin.indices.get.GetIndexAction;
import org.codelibs.fesen.action.admin.indices.get.TransportGetIndexAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingAction;
import org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.codelibs.fesen.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.codelibs.fesen.action.admin.indices.open.OpenIndexAction;
import org.codelibs.fesen.action.admin.indices.open.TransportOpenIndexAction;
import org.codelibs.fesen.action.admin.indices.readonly.AddIndexBlockAction;
import org.codelibs.fesen.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.codelibs.fesen.action.admin.indices.recovery.RecoveryAction;
import org.codelibs.fesen.action.admin.indices.recovery.TransportRecoveryAction;
import org.codelibs.fesen.action.admin.indices.refresh.RefreshAction;
import org.codelibs.fesen.action.admin.indices.refresh.TransportRefreshAction;
import org.codelibs.fesen.action.admin.indices.resolve.ResolveIndexAction;
import org.codelibs.fesen.action.admin.indices.rollover.RolloverAction;
import org.codelibs.fesen.action.admin.indices.rollover.TransportRolloverAction;
import org.codelibs.fesen.action.admin.indices.segments.IndicesSegmentsAction;
import org.codelibs.fesen.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsAction;
import org.codelibs.fesen.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.codelibs.fesen.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.codelibs.fesen.action.admin.indices.settings.put.UpdateSettingsAction;
import org.codelibs.fesen.action.admin.indices.shards.IndicesShardStoresAction;
import org.codelibs.fesen.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.codelibs.fesen.action.admin.indices.shrink.ResizeAction;
import org.codelibs.fesen.action.admin.indices.shrink.TransportResizeAction;
import org.codelibs.fesen.action.admin.indices.stats.IndicesStatsAction;
import org.codelibs.fesen.action.admin.indices.stats.TransportIndicesStatsAction;
import org.codelibs.fesen.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.get.GetComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.codelibs.fesen.action.admin.indices.template.get.TransportGetComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.get.TransportGetComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.codelibs.fesen.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.post.SimulateTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.post.TransportSimulateTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.PutComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.PutIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.upgrade.get.TransportUpgradeStatusAction;
import org.codelibs.fesen.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.codelibs.fesen.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.codelibs.fesen.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import org.codelibs.fesen.action.admin.indices.upgrade.post.UpgradeAction;
import org.codelibs.fesen.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.codelibs.fesen.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryAction;
import org.codelibs.fesen.action.bulk.BulkAction;
import org.codelibs.fesen.action.bulk.TransportBulkAction;
import org.codelibs.fesen.action.bulk.TransportShardBulkAction;
import org.codelibs.fesen.action.delete.DeleteAction;
import org.codelibs.fesen.action.delete.TransportDeleteAction;
import org.codelibs.fesen.action.explain.ExplainAction;
import org.codelibs.fesen.action.explain.TransportExplainAction;
import org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesAction;
import org.codelibs.fesen.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.codelibs.fesen.action.fieldcaps.TransportFieldCapabilitiesIndexAction;
import org.codelibs.fesen.action.get.GetAction;
import org.codelibs.fesen.action.get.MultiGetAction;
import org.codelibs.fesen.action.get.TransportGetAction;
import org.codelibs.fesen.action.get.TransportMultiGetAction;
import org.codelibs.fesen.action.get.TransportShardMultiGetAction;
import org.codelibs.fesen.action.index.IndexAction;
import org.codelibs.fesen.action.index.TransportIndexAction;
import org.codelibs.fesen.action.ingest.DeletePipelineAction;
import org.codelibs.fesen.action.ingest.DeletePipelineTransportAction;
import org.codelibs.fesen.action.ingest.GetPipelineAction;
import org.codelibs.fesen.action.ingest.GetPipelineTransportAction;
import org.codelibs.fesen.action.ingest.PutPipelineAction;
import org.codelibs.fesen.action.ingest.PutPipelineTransportAction;
import org.codelibs.fesen.action.ingest.SimulatePipelineAction;
import org.codelibs.fesen.action.ingest.SimulatePipelineTransportAction;
import org.codelibs.fesen.action.main.MainAction;
import org.codelibs.fesen.action.main.TransportMainAction;
import org.codelibs.fesen.action.search.ClearScrollAction;
import org.codelibs.fesen.action.search.MultiSearchAction;
import org.codelibs.fesen.action.search.SearchAction;
import org.codelibs.fesen.action.search.SearchScrollAction;
import org.codelibs.fesen.action.search.TransportClearScrollAction;
import org.codelibs.fesen.action.search.TransportMultiSearchAction;
import org.codelibs.fesen.action.search.TransportSearchAction;
import org.codelibs.fesen.action.search.TransportSearchScrollAction;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.AutoCreateIndex;
import org.codelibs.fesen.action.support.DestructiveOperations;
import org.codelibs.fesen.action.support.TransportAction;
import org.codelibs.fesen.action.termvectors.MultiTermVectorsAction;
import org.codelibs.fesen.action.termvectors.TermVectorsAction;
import org.codelibs.fesen.action.termvectors.TransportMultiTermVectorsAction;
import org.codelibs.fesen.action.termvectors.TransportShardMultiTermsVectorAction;
import org.codelibs.fesen.action.termvectors.TransportTermVectorsAction;
import org.codelibs.fesen.action.update.TransportUpdateAction;
import org.codelibs.fesen.action.update.UpdateAction;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.common.NamedRegistry;
import org.codelibs.fesen.common.inject.AbstractModule;
import org.codelibs.fesen.common.inject.TypeLiteral;
import org.codelibs.fesen.common.inject.multibindings.MapBinder;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.IndexScopedSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsFilter;
import org.codelibs.fesen.index.seqno.RetentionLeaseActions;
import org.codelibs.fesen.indices.SystemIndices;
import org.codelibs.fesen.indices.breaker.CircuitBreakerService;
import org.codelibs.fesen.persistent.CompletionPersistentTaskAction;
import org.codelibs.fesen.persistent.RemovePersistentTaskAction;
import org.codelibs.fesen.persistent.StartPersistentTaskAction;
import org.codelibs.fesen.persistent.UpdatePersistentTaskStatusAction;
import org.codelibs.fesen.plugins.ActionPlugin;
import org.codelibs.fesen.plugins.ActionPlugin.ActionHandler;
import org.codelibs.fesen.rest.RestController;
import org.codelibs.fesen.rest.RestHandler;
import org.codelibs.fesen.rest.RestHeaderDefinition;
import org.codelibs.fesen.rest.action.RestFieldCapabilitiesAction;
import org.codelibs.fesen.rest.action.RestMainAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestCancelTasksAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterAllocationExplainAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterHealthAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterRerouteAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterSearchShardsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterStateAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterStatsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetScriptContextAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestGetTaskAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestListTasksAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestNodesInfoAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestNodesStatsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestNodesUsageAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestPutRepositoryAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.codelibs.fesen.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.codelibs.fesen.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.codelibs.fesen.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.codelibs.fesen.rest.action.admin.cluster.dangling.RestListDanglingIndicesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestAddIndexBlockAction;
import org.codelibs.fesen.rest.action.admin.indices.RestAnalyzeAction;
import org.codelibs.fesen.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.codelibs.fesen.rest.action.admin.indices.RestCloseIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestCreateIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestDeleteIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestFlushAction;
import org.codelibs.fesen.rest.action.admin.indices.RestForceMergeAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetAliasesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetFieldMappingAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetIndicesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetMappingAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetSettingsAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndexPutAliasAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndicesAliasesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndicesSegmentsAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndicesShardStoresAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndicesStatsAction;
import org.codelibs.fesen.rest.action.admin.indices.RestOpenIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestPutMappingAction;
import org.codelibs.fesen.rest.action.admin.indices.RestRecoveryAction;
import org.codelibs.fesen.rest.action.admin.indices.RestRefreshAction;
import org.codelibs.fesen.rest.action.admin.indices.RestResizeHandler;
import org.codelibs.fesen.rest.action.admin.indices.RestResolveIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestRolloverIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestSimulateTemplateAction;
import org.codelibs.fesen.rest.action.admin.indices.RestSyncedFlushAction;
import org.codelibs.fesen.rest.action.admin.indices.RestUpdateSettingsAction;
import org.codelibs.fesen.rest.action.admin.indices.RestUpgradeAction;
import org.codelibs.fesen.rest.action.admin.indices.RestUpgradeStatusAction;
import org.codelibs.fesen.rest.action.admin.indices.RestValidateQueryAction;
import org.codelibs.fesen.rest.action.cat.AbstractCatAction;
import org.codelibs.fesen.rest.action.cat.RestAliasAction;
import org.codelibs.fesen.rest.action.cat.RestAllocationAction;
import org.codelibs.fesen.rest.action.cat.RestCatAction;
import org.codelibs.fesen.rest.action.cat.RestCatRecoveryAction;
import org.codelibs.fesen.rest.action.cat.RestFielddataAction;
import org.codelibs.fesen.rest.action.cat.RestHealthAction;
import org.codelibs.fesen.rest.action.cat.RestIndicesAction;
import org.codelibs.fesen.rest.action.cat.RestMasterAction;
import org.codelibs.fesen.rest.action.cat.RestNodeAttrsAction;
import org.codelibs.fesen.rest.action.cat.RestNodesAction;
import org.codelibs.fesen.rest.action.cat.RestPluginsAction;
import org.codelibs.fesen.rest.action.cat.RestRepositoriesAction;
import org.codelibs.fesen.rest.action.cat.RestSegmentsAction;
import org.codelibs.fesen.rest.action.cat.RestShardsAction;
import org.codelibs.fesen.rest.action.cat.RestSnapshotAction;
import org.codelibs.fesen.rest.action.cat.RestTasksAction;
import org.codelibs.fesen.rest.action.cat.RestTemplatesAction;
import org.codelibs.fesen.rest.action.cat.RestThreadPoolAction;
import org.codelibs.fesen.rest.action.document.RestBulkAction;
import org.codelibs.fesen.rest.action.document.RestDeleteAction;
import org.codelibs.fesen.rest.action.document.RestGetAction;
import org.codelibs.fesen.rest.action.document.RestGetSourceAction;
import org.codelibs.fesen.rest.action.document.RestIndexAction;
import org.codelibs.fesen.rest.action.document.RestMultiGetAction;
import org.codelibs.fesen.rest.action.document.RestMultiTermVectorsAction;
import org.codelibs.fesen.rest.action.document.RestTermVectorsAction;
import org.codelibs.fesen.rest.action.document.RestUpdateAction;
import org.codelibs.fesen.rest.action.document.RestIndexAction.AutoIdHandler;
import org.codelibs.fesen.rest.action.document.RestIndexAction.CreateHandler;
import org.codelibs.fesen.rest.action.ingest.RestDeletePipelineAction;
import org.codelibs.fesen.rest.action.ingest.RestGetPipelineAction;
import org.codelibs.fesen.rest.action.ingest.RestPutPipelineAction;
import org.codelibs.fesen.rest.action.ingest.RestSimulatePipelineAction;
import org.codelibs.fesen.rest.action.search.RestClearScrollAction;
import org.codelibs.fesen.rest.action.search.RestCountAction;
import org.codelibs.fesen.rest.action.search.RestExplainAction;
import org.codelibs.fesen.rest.action.search.RestMultiSearchAction;
import org.codelibs.fesen.rest.action.search.RestSearchAction;
import org.codelibs.fesen.rest.action.search.RestSearchScrollAction;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.usage.UsageService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final boolean transportClient;
    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    private final Map<String, ActionHandler<?, ?>> actions;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;

    public ActionModule(boolean transportClient, Settings settings, IndexNameExpressionResolver indexNameExpressionResolver,
                        IndexScopedSettings indexScopedSettings, ClusterSettings clusterSettings, SettingsFilter settingsFilter,
                        ThreadPool threadPool, List<ActionPlugin> actionPlugins, NodeClient nodeClient,
                        CircuitBreakerService circuitBreakerService, UsageService usageService, SystemIndices systemIndices) {
        this.transportClient = transportClient;
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        autoCreateIndex = transportClient
            ? null
            : new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(new RestHeaderDefinition(Task.X_OPAQUE_ID, false))
        ).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext());
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList()));
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
                actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList()));

        if (transportClient) {
            restController = null;
        } else {
            restController = new RestController(headers, restWrapper, nodeClient, circuitBreakerService, usageService);
        }
    }


    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action, Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(RemoteInfoAction.INSTANCE, TransportRemoteInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(NodesUsageAction.INSTANCE, TransportNodesUsageAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class);
        actions.register(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(CleanupRepositoryAction.INSTANCE, TransportCleanupRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(CloneSnapshotAction.INSTANCE, TransportCloneSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(TypesExistsAction.INSTANCE, TransportTypesExistsAction.class);
        actions.register(AddIndexBlockAction.INSTANCE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(GetFieldMappingsAction.INSTANCE, TransportGetFieldMappingsAction.class,
                TransportGetFieldMappingsIndexAction.class);
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(AutoPutMappingAction.INSTANCE, TransportAutoPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(DeleteComponentTemplateAction.INSTANCE, TransportDeleteComponentTemplateAction.class);
        actions.register(PutComposableIndexTemplateAction.INSTANCE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(DeleteComposableIndexTemplateAction.INSTANCE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(SyncedFlushAction.INSTANCE, TransportSyncedFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(AliasesExistAction.INSTANCE, TransportAliasesExistAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(MultiTermVectorsAction.INSTANCE, TransportMultiTermVectorsAction.class,
                TransportShardMultiTermsVectorAction.class);
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class,
                TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class,
                TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);

        //Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(FieldCapabilitiesAction.INSTANCE, TransportFieldCapabilitiesAction.class,
            TransportFieldCapabilitiesIndexAction.class);

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);

        // Dangling indices
        actions.register(ListDanglingIndicesAction.INSTANCE, TransportListDanglingIndicesAction.class);
        actions.register(ImportDanglingIndexAction.INSTANCE, TransportImportDanglingIndexAction.class);
        actions.register(DeleteDanglingIndexAction.INSTANCE, TransportDeleteDanglingIndexAction.class);
        actions.register(FindDanglingIndexAction.INSTANCE, TransportFindDanglingIndexAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet())));
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = handler -> {
            if (handler instanceof AbstractCatAction) {
                catActions.add((AbstractCatAction) handler);
            }
            restController.registerHandler(handler);
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestMainAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesStatsAction());
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction());
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestGetIndicesAction());
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction());
        registerHandler.accept(new RestIndicesShardStoresAction());
        registerHandler.accept(new RestGetAliasesAction());
        registerHandler.accept(new RestIndexDeleteAliasesAction());
        registerHandler.accept(new RestIndexPutAliasAction());
        registerHandler.accept(new RestIndicesAliasesAction());
        registerHandler.accept(new RestCreateIndexAction());
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction());
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction());
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction());
        registerHandler.accept(new RestRolloverIndexAction());
        registerHandler.accept(new RestDeleteIndexAction());
        registerHandler.accept(new RestCloseIndexAction());
        registerHandler.accept(new RestOpenIndexAction());
        registerHandler.accept(new RestAddIndexBlockAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestGetIndexTemplateAction());
        registerHandler.accept(new RestPutIndexTemplateAction());
        registerHandler.accept(new RestDeleteIndexTemplateAction());
        registerHandler.accept(new RestPutComponentTemplateAction());
        registerHandler.accept(new RestGetComponentTemplateAction());
        registerHandler.accept(new RestDeleteComponentTemplateAction());
        registerHandler.accept(new RestPutComposableIndexTemplateAction());
        registerHandler.accept(new RestGetComposableIndexTemplateAction());
        registerHandler.accept(new RestDeleteComposableIndexTemplateAction());
        registerHandler.accept(new RestSimulateIndexTemplateAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction(threadPool));
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestSyncedFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestUpgradeAction());
        registerHandler.accept(new RestUpgradeStatusAction());
        registerHandler.accept(new RestClearIndicesCacheAction());
        registerHandler.accept(new RestResolveIndexAction());

        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler(nodesInCluster));
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction());
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestMultiSearchAction(settings));

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction());
        registerHandler.accept(new RestPutStoredScriptAction());
        registerHandler.accept(new RestDeleteStoredScriptAction());
        registerHandler.accept(new RestGetScriptContextAction());
        registerHandler.accept(new RestGetScriptLanguageAction());

        registerHandler.accept(new RestFieldCapabilitiesAction());

        // Tasks API
        registerHandler.accept(new RestListTasksAction(nodesInCluster));
        registerHandler.accept(new RestGetTaskAction());
        registerHandler.accept(new RestCancelTasksAction(nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction());
        registerHandler.accept(new RestGetPipelineAction());
        registerHandler.accept(new RestDeletePipelineAction());
        registerHandler.accept(new RestSimulatePipelineAction());

        // Dangling indices API
        registerHandler.accept(new RestListDanglingIndicesAction());
        registerHandler.accept(new RestImportDanglingIndexAction());
        registerHandler.accept(new RestDeleteDanglingIndexAction());

        // CAT API
        registerHandler.accept(new RestAllocationAction());
        registerHandler.accept(new RestShardsAction());
        registerHandler.accept(new RestMasterAction());
        registerHandler.accept(new RestNodesAction());
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction());
        registerHandler.accept(new RestSegmentsAction());
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.codelibs.fesen.rest.action.cat.RestCountAction());
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction());
        registerHandler.accept(new RestHealthAction());
        registerHandler.accept(new org.codelibs.fesen.rest.action.cat.RestPendingClusterTasksAction());
        registerHandler.accept(new RestAliasAction());
        registerHandler.accept(new RestThreadPoolAction());
        registerHandler.accept(new RestPluginsAction());
        registerHandler.accept(new RestFielddataAction());
        registerHandler.accept(new RestNodeAttrsAction());
        registerHandler.accept(new RestRepositoriesAction());
        registerHandler.accept(new RestSnapshotAction());
        registerHandler.accept(new RestTemplatesAction());
        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(settings, restController, clusterSettings, indexScopedSettings,
                    settingsFilter, indexNameExpressionResolver, nodesInCluster)) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {}).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {}).toInstance(indicesAliasesRequestRequestValidators);

        if (false == transportClient) {
            // Supporting classes only used when not a transport client
            bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
            bind(TransportLivenessAction.class).asEagerSingleton();

            // register ActionType -> transportAction Map used by NodeClient
            @SuppressWarnings("rawtypes")
            MapBinder<ActionType, TransportAction> transportActionsBinder
                    = MapBinder.newMapBinder(binder(), ActionType.class, TransportAction.class);
            for (ActionHandler<?, ?> action : actions.values()) {
                // bind the action as eager singleton, so the map binder one will reuse it
                bind(action.getTransportAction()).asEagerSingleton();
                transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
                for (Class<?> supportAction : action.getSupportTransportActions()) {
                    bind(supportAction).asEagerSingleton();
                }
            }
        }
    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public RestController getRestController() {
        return restController;
    }
}
