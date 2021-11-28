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

package org.codelibs.fesen.index.rankeval;

import org.codelibs.fesen.action.ActionRequestBuilder;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.client.FesenClient;

public class RankEvalRequestBuilder extends ActionRequestBuilder<RankEvalRequest, RankEvalResponse> {

    public RankEvalRequestBuilder(FesenClient client, ActionType<RankEvalResponse> action, RankEvalRequest request) {
        super(client, action, request);
    }

    @Override
    public RankEvalRequest request() {
        return request;
    }

    public void setRankEvalSpec(RankEvalSpec spec) {
        this.request.setRankEvalSpec(spec);
    }

    public RankEvalSpec getRankEvalSpec() {
        return this.request.getRankEvalSpec();
    }
}
