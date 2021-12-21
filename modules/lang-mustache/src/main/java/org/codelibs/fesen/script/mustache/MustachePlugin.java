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

package org.codelibs.fesen.script.mustache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.IndexScopedSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsFilter;
import org.codelibs.fesen.plugins.ActionPlugin;
import org.codelibs.fesen.plugins.Plugin;
import org.codelibs.fesen.plugins.ScriptPlugin;
import org.codelibs.fesen.plugins.SearchPlugin;
import org.codelibs.fesen.rest.RestController;
import org.codelibs.fesen.rest.RestHandler;
import org.codelibs.fesen.script.ScriptContext;
import org.codelibs.fesen.script.ScriptEngine;

public class MustachePlugin extends Plugin implements ScriptPlugin, ActionPlugin, SearchPlugin {

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>>contexts) {
        return new MustacheScriptEngine();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SearchTemplateAction.INSTANCE, TransportSearchTemplateAction.class),
                new ActionHandler<>(MultiSearchTemplateAction.INSTANCE, TransportMultiSearchTemplateAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestSearchTemplateAction(),
                new RestMultiSearchTemplateAction(settings),
                new RestRenderSearchTemplateAction());
    }
}
