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

package org.codelibs.fesen.rest.action.admin.cluster;

import org.codelibs.fesen.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;
import org.codelibs.fesen.tasks.TaskId;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.rest.RestRequest.Method.GET;

public class RestGetTaskAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_tasks/{task_id}"));
    }

    @Override
    public String getName() {
        return "get_task_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TaskId taskId = new TaskId(request.param("task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = request.paramAsTime("timeout", null);

        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(taskId);
        getTaskRequest.setWaitForCompletion(waitForCompletion);
        getTaskRequest.setTimeout(timeout);
        return channel -> client.admin().cluster().getTask(getTaskRequest, new RestToXContentListener<>(channel));
    }
}
