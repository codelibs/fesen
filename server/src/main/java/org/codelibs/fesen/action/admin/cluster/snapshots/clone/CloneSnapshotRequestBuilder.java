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

package org.codelibs.fesen.action.admin.cluster.snapshots.clone;

import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.action.support.master.MasterNodeOperationRequestBuilder;
import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.common.Strings;

public class CloneSnapshotRequestBuilder extends MasterNodeOperationRequestBuilder<CloneSnapshotRequest, AcknowledgedResponse,
                                                                                   CloneSnapshotRequestBuilder> {

    protected CloneSnapshotRequestBuilder(FesenClient client, ActionType<AcknowledgedResponse> action,
                                          CloneSnapshotRequest request) {
        super(client, action, request);
    }

    public CloneSnapshotRequestBuilder(FesenClient client, ActionType<AcknowledgedResponse> action,
                                       String repository, String source, String target) {
        this(client, action, new CloneSnapshotRequest(repository, source, target, Strings.EMPTY_ARRAY));
    }

    /**
     * Sets a list of indices that should be cloned from the source to the target snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will clone all indices with
     * prefix "test" except index "test42".
     *
     * @return this builder
     */
    public CloneSnapshotRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CloneSnapshotRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
