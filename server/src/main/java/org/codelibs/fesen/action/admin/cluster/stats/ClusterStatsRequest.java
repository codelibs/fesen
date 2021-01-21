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

package org.codelibs.fesen.action.admin.cluster.stats;

import java.io.IOException;

import org.codelibs.fesen.action.support.nodes.BaseNodesRequest;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;

/**
 * A request to get cluster level stats.
 */
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

}
