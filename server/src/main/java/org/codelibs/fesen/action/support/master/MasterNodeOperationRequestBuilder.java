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

package org.codelibs.fesen.action.support.master;

import org.codelibs.fesen.action.ActionRequestBuilder;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.core.TimeValue;

/**
 * Base request builder for master node operations
 */
public abstract class MasterNodeOperationRequestBuilder<Request extends MasterNodeRequest<Request>, Response extends ActionResponse,
        RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response> {

    protected MasterNodeOperationRequestBuilder(FesenClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setMasterNodeTimeout(TimeValue timeout) {
        request.masterNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setMasterNodeTimeout(String timeout) {
        request.masterNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

}
