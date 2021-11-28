/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.kibana;

import static java.util.Collections.unmodifiableList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.IndexScopedSettings;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Setting.Property;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsFilter;
import org.codelibs.fesen.index.reindex.RestDeleteByQueryAction;
import org.codelibs.fesen.indices.SystemIndexDescriptor;
import org.codelibs.fesen.plugins.Plugin;
import org.codelibs.fesen.plugins.SystemIndexPlugin;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestController;
import org.codelibs.fesen.rest.RestHandler;
import org.codelibs.fesen.rest.action.admin.indices.RestCreateIndexAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetAliasesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestGetIndicesAction;
import org.codelibs.fesen.rest.action.admin.indices.RestIndexPutAliasAction;
import org.codelibs.fesen.rest.action.admin.indices.RestRefreshAction;
import org.codelibs.fesen.rest.action.admin.indices.RestUpdateSettingsAction;
import org.codelibs.fesen.rest.action.document.RestBulkAction;
import org.codelibs.fesen.rest.action.document.RestDeleteAction;
import org.codelibs.fesen.rest.action.document.RestGetAction;
import org.codelibs.fesen.rest.action.document.RestIndexAction;
import org.codelibs.fesen.rest.action.document.RestIndexAction.AutoIdHandler;
import org.codelibs.fesen.rest.action.document.RestIndexAction.CreateHandler;
import org.codelibs.fesen.rest.action.document.RestMultiGetAction;
import org.codelibs.fesen.rest.action.document.RestUpdateAction;
import org.codelibs.fesen.rest.action.search.RestClearScrollAction;
import org.codelibs.fesen.rest.action.search.RestSearchAction;
import org.codelibs.fesen.rest.action.search.RestSearchScrollAction;

public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    public static final Setting<List<String>> KIBANA_INDEX_NAMES_SETTING = Setting.listSetting("kibana.system_indices",
            unmodifiableList(Arrays.asList(".kibana", ".kibana_*", ".reporting-*", ".apm-agent-configuration", ".apm-custom-link")),
            Function.identity(), Property.NodeScope);

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return unmodifiableList(KIBANA_INDEX_NAMES_SETTING.get(settings).stream()
                .map(pattern -> new SystemIndexDescriptor(pattern, "System index used by kibana")).collect(Collectors.toList()));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        // TODO need to figure out what subset of system indices Kibana should have access to via these APIs
        return unmodifiableList(Arrays.asList(
                // Based on https://github.com/elastic/kibana/issues/49764
                // apis needed to perform migrations... ideally these will go away
                new KibanaWrappedRestHandler(new RestCreateIndexAction()), new KibanaWrappedRestHandler(new RestGetAliasesAction()),
                new KibanaWrappedRestHandler(new RestIndexPutAliasAction()), new KibanaWrappedRestHandler(new RestRefreshAction()),

                // apis needed to access saved objects
                new KibanaWrappedRestHandler(new RestGetAction()), new KibanaWrappedRestHandler(new RestMultiGetAction(settings)),
                new KibanaWrappedRestHandler(new RestSearchAction()), new KibanaWrappedRestHandler(new RestBulkAction(settings)),
                new KibanaWrappedRestHandler(new RestDeleteAction()), new KibanaWrappedRestHandler(new RestDeleteByQueryAction()),

                // api used for testing
                new KibanaWrappedRestHandler(new RestUpdateSettingsAction()),

                // apis used specifically by reporting
                new KibanaWrappedRestHandler(new RestGetIndicesAction()), new KibanaWrappedRestHandler(new RestIndexAction()),
                new KibanaWrappedRestHandler(new CreateHandler()), new KibanaWrappedRestHandler(new AutoIdHandler(nodesInCluster)),
                new KibanaWrappedRestHandler(new RestUpdateAction()), new KibanaWrappedRestHandler(new RestSearchScrollAction()),
                new KibanaWrappedRestHandler(new RestClearScrollAction())));

    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(KIBANA_INDEX_NAMES_SETTING);
    }

    static class KibanaWrappedRestHandler extends BaseRestHandler.Wrapper {

        KibanaWrappedRestHandler(BaseRestHandler delegate) {
            super(delegate);
        }

        @Override
        public String getName() {
            return "kibana_" + super.getName();
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return true;
        }

        @Override
        public List<Route> routes() {
            return unmodifiableList(super.routes().stream().map(route -> new Route(route.getMethod(), "/_kibana" + route.getPath()))
                    .collect(Collectors.toList()));
        }
    }
}
