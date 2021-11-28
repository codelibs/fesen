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

package org.codelibs.fesen.action.admin.cluster.reroute;

import java.io.IOException;

import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.routing.allocation.RoutingExplanations;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.ToXContentObject;
import org.codelibs.fesen.common.xcontent.XContentBuilder;

/**
 * Response returned after a cluster reroute request
 */
public class ClusterRerouteResponse extends AcknowledgedResponse implements ToXContentObject {

    private final ClusterState state;
    private final RoutingExplanations explanations;

    ClusterRerouteResponse(StreamInput in) throws IOException {
        super(in, true);
        state = ClusterState.readFrom(in, null);
        explanations = RoutingExplanations.readFrom(in);
    }

    ClusterRerouteResponse(boolean acknowledged, ClusterState state, RoutingExplanations explanations) {
        super(acknowledged);
        this.state = state;
        this.explanations = explanations;
    }

    /**
     * Returns the cluster state resulted from the cluster reroute request execution
     */
    public ClusterState getState() {
        return this.state;
    }

    public RoutingExplanations getExplanations() {
        return this.explanations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        state.writeTo(out);
        RoutingExplanations.writeTo(explanations, out);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("state");
        state.toXContent(builder, params);
        builder.endObject();
        if (params.paramAsBoolean("explain", false)) {
            explanations.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }
}
