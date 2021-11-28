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

import java.util.Objects;

import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.core.TimeValue;

public abstract class ActionRequestBuilder<Request extends ActionRequest, Response extends ActionResponse> {

    protected final ActionType<Response> action;
    protected final Request request;
    protected final FesenClient client;

    protected ActionRequestBuilder(FesenClient client, ActionType<Response> action, Request request) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.request = request;
        this.client = client;
    }

    public Request request() {
        return this.request;
    }

    public ActionFuture<Response> execute() {
        return client.execute(action, request);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get() {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(TimeValue timeout) {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
     */
    public Response get(String timeout) {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }
}
