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

package org.codelibs.fesen.action.admin.cluster.node.info;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codelibs.fesen.action.FailedNodeException;
import org.codelibs.fesen.action.support.nodes.BaseNodesResponse;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.node.DiscoveryNodeRole;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.ToXContentFragment;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.http.HttpInfo;
import org.codelibs.fesen.ingest.IngestInfo;
import org.codelibs.fesen.monitor.jvm.JvmInfo;
import org.codelibs.fesen.monitor.os.OsInfo;
import org.codelibs.fesen.monitor.process.ProcessInfo;
import org.codelibs.fesen.search.aggregations.support.AggregationInfo;
import org.codelibs.fesen.threadpool.ThreadPoolInfo;
import org.codelibs.fesen.transport.TransportInfo;

public class NodesInfoResponse extends BaseNodesResponse<NodeInfo> implements ToXContentFragment {

    public NodesInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesInfoResponse(ClusterName clusterName, List<NodeInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeInfo::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeInfo> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (NodeInfo nodeInfo : getNodes()) {
            builder.startObject(nodeInfo.getNode().getId());

            builder.field("name", nodeInfo.getNode().getName());
            builder.field("transport_address", nodeInfo.getNode().getAddress().toString());
            builder.field("host", nodeInfo.getNode().getHostName());
            builder.field("ip", nodeInfo.getNode().getHostAddress());

            builder.field("version", nodeInfo.getVersion());
            builder.field("build_flavor", nodeInfo.getBuild().flavor().displayName());
            builder.field("build_type", nodeInfo.getBuild().type().displayName());
            builder.field("build_hash", nodeInfo.getBuild().hash());
            if (nodeInfo.getTotalIndexingBuffer() != null) {
                builder.humanReadableField("total_indexing_buffer", "total_indexing_buffer_in_bytes", nodeInfo.getTotalIndexingBuffer());
            }

            builder.startArray("roles");
            for (DiscoveryNodeRole role : nodeInfo.getNode().getRoles()) {
                builder.value(role.roleName());
            }
            builder.endArray();

            if (!nodeInfo.getNode().getAttributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> entry : nodeInfo.getNode().getAttributes().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            if (nodeInfo.getSettings() != null) {
                builder.startObject("settings");
                Settings settings = nodeInfo.getSettings();
                settings.toXContent(builder, params);
                builder.endObject();
            }

            if (nodeInfo.getInfo(OsInfo.class) != null) {
                nodeInfo.getInfo(OsInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProcessInfo.class) != null) {
                nodeInfo.getInfo(ProcessInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(JvmInfo.class) != null) {
                nodeInfo.getInfo(JvmInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ThreadPoolInfo.class) != null) {
                nodeInfo.getInfo(ThreadPoolInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(TransportInfo.class) != null) {
                nodeInfo.getInfo(TransportInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(HttpInfo.class) != null) {
                nodeInfo.getInfo(HttpInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(PluginsAndModules.class) != null) {
                nodeInfo.getInfo(PluginsAndModules.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(IngestInfo.class) != null) {
                nodeInfo.getInfo(IngestInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(AggregationInfo.class) != null) {
                nodeInfo.getInfo(AggregationInfo.class).toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
