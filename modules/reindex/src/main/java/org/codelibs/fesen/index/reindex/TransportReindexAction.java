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

package org.codelibs.fesen.index.reindex;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.function.Function;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.AutoCreateIndex;
import org.codelibs.fesen.action.support.HandledTransportAction;
import org.codelibs.fesen.client.Client;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Setting.Property;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_WHITELIST =
            Setting.listSetting("reindex.remote.whitelist", emptyList(), Function.identity(), Property.NodeScope);

    private final ReindexValidator reindexValidator;
    private final Reindexer reindexer;

    @Inject
    public TransportReindexAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ScriptService scriptService,
            AutoCreateIndex autoCreateIndex, Client client, TransportService transportService, ReindexSslConfig sslConfig) {
        super(ReindexAction.NAME, transportService, actionFilters, ReindexRequest::new);
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.reindexer = new Reindexer(clusterService, client, threadPool, scriptService, sslConfig);
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        reindexValidator.initialValidation(request);
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        reindexer.initTask(bulkByScrollTask, request, new ActionListener<Void>() {
            @Override
            public void onResponse(Void v) {
                reindexer.execute(bulkByScrollTask, request, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
