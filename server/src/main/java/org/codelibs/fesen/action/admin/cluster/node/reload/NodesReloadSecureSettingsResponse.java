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

package org.codelibs.fesen.action.admin.cluster.node.reload;

import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.FailedNodeException;
import org.codelibs.fesen.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.action.support.nodes.BaseNodesResponse;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ToXContentFragment;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

/**
 * The response for the reload secure settings action
 */
public class NodesReloadSecureSettingsResponse extends BaseNodesResponse<NodesReloadSecureSettingsResponse.NodeResponse>
        implements ToXContentFragment {

    public NodesReloadSecureSettingsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesReloadSecureSettingsResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodesReloadSecureSettingsResponse.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodesReloadSecureSettingsResponse.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (final NodesReloadSecureSettingsResponse.NodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            builder.field("name", node.getNode().getName());
            final Exception e = node.reloadException();
            if (e != null) {
                builder.startObject("reload_exception");
                FesenException.generateThrowableXContent(builder, params, e);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (final IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private Exception reloadException = null;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                reloadException = in.readException();
            }
        }

        public NodeResponse(DiscoveryNode node, Exception reloadException) {
            super(node);
            this.reloadException = reloadException;
        }

        public Exception reloadException() {
            return this.reloadException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (reloadException != null) {
                out.writeBoolean(true);
                out.writeException(reloadException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final NodesReloadSecureSettingsResponse.NodeResponse that = (NodesReloadSecureSettingsResponse.NodeResponse) o;
            return reloadException != null ? reloadException.equals(that.reloadException) : that.reloadException == null;
        }

        @Override
        public int hashCode() {
            return reloadException != null ? reloadException.hashCode() : 0;
        }
    }
}
