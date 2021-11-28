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

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionModule;
import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionRequestValidationException;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.action.main.MainAction;
import org.codelibs.fesen.action.main.TransportMainAction;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.TransportAction;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.IndexScopedSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsFilter;
import org.codelibs.fesen.common.settings.SettingsModule;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.plugins.ActionPlugin;
import org.codelibs.fesen.plugins.ActionPlugin.ActionHandler;
import org.codelibs.fesen.rest.RestChannel;
import org.codelibs.fesen.rest.RestController;
import org.codelibs.fesen.rest.RestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.RestRequest.Method;
import org.codelibs.fesen.rest.action.RestMainAction;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskManager;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.startsWith;

public class ActionModuleTests extends ESTestCase {
    public void testSetupActionsContainsKnownBuiltin() {
        assertThat(ActionModule.setupActions(emptyList()),
                hasEntry(MainAction.INSTANCE.name(), new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class)));
    }

    public void testPluginCantOverwriteBuiltinAction() {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(MainAction.INSTANCE, TransportMainAction.class));
            }
        };
        Exception e = expectThrows(IllegalArgumentException.class, () -> ActionModule.setupActions(singletonList(dupsMainAction)));
        assertEquals("action for name [" + MainAction.NAME + "] already registered", e.getMessage());
    }

    public void testPluginCanRegisterAction() {
        class FakeRequest extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
        class FakeTransportAction extends TransportAction<FakeRequest, ActionResponse> {
            protected FakeTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
                super(actionName, actionFilters, taskManager);
            }

            @Override
            protected void doExecute(Task task, FakeRequest request, ActionListener<ActionResponse> listener) {
            }
        }
        class FakeAction extends ActionType<ActionResponse> {
            protected FakeAction() {
                super("fake", null);
            }
        }
        FakeAction action = new FakeAction();
        ActionPlugin registersFakeAction = new ActionPlugin() {
            @Override
            public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
                return singletonList(new ActionHandler<>(action, FakeTransportAction.class));
            }
        };
        assertThat(ActionModule.setupActions(singletonList(registersFakeAction)),
                hasEntry("fake", new ActionHandler<>(action, FakeTransportAction.class)));
    }

    public void testSetupRestHandlerContainsKnownBuiltin() {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        UsageService usageService = new UsageService();
        ActionModule actionModule = new ActionModule(false, settings.getSettings(),
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)), settings.getIndexScopedSettings(),
                settings.getClusterSettings(), settings.getSettingsFilter(), null, emptyList(), null, null, usageService, null);
        actionModule.initRestHandlers(null);
        // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
        Exception e =
                expectThrows(IllegalArgumentException.class, () -> actionModule.getRestController().registerHandler(new RestHandler() {
                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                    }

                    @Override
                    public List<Route> routes() {
                        return singletonList(new Route(Method.GET, "/"));
                    }
                }));
        assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
    }

    public void testPluginCantOverwriteBuiltinRestHandler() throws IOException {
        ActionPlugin dupsMainAction = new ActionPlugin() {
            @Override
            public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                    IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                    IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
                return singletonList(new RestMainAction() {

                    @Override
                    public String getName() {
                        return "duplicated_" + super.getName();
                    }

                });
            }
        };
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule =
                    new ActionModule(false, settings.getSettings(), new IndexNameExpressionResolver(threadPool.getThreadContext()),
                            settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                            singletonList(dupsMainAction), null, null, usageService, null);
            Exception e = expectThrows(IllegalArgumentException.class, () -> actionModule.initRestHandlers(null));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testPluginCanRegisterRestHandler() {
        class FakeHandler implements RestHandler {
            @Override
            public List<Route> routes() {
                return singletonList(new Route(Method.GET, "/_dummy"));
            }

            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            }
        }
        ActionPlugin registersFakeHandler = new ActionPlugin() {
            @Override
            public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                    IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                    IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
                return singletonList(new FakeHandler());
            }
        };

        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            UsageService usageService = new UsageService();
            ActionModule actionModule =
                    new ActionModule(false, settings.getSettings(), new IndexNameExpressionResolver(threadPool.getThreadContext()),
                            settings.getIndexScopedSettings(), settings.getClusterSettings(), settings.getSettingsFilter(), threadPool,
                            singletonList(registersFakeHandler), null, null, usageService, null);
            actionModule.initRestHandlers(null);
            // At this point the easiest way to confirm that a handler is loaded is to try to register another one on top of it and to fail
            Exception e =
                    expectThrows(IllegalArgumentException.class, () -> actionModule.getRestController().registerHandler(new RestHandler() {
                        @Override
                        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                        }

                        @Override
                        public List<Route> routes() {
                            return singletonList(new Route(Method.GET, "/_dummy"));
                        }
                    }));
            assertThat(e.getMessage(), startsWith("Cannot replace existing handler for [/_dummy] for method: GET"));
        } finally {
            threadPool.shutdown();
        }
    }
}
