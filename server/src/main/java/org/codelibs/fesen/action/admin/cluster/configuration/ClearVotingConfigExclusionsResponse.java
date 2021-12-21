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
package org.codelibs.fesen.action.admin.cluster.configuration;

import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ToXContentObject;
import org.codelibs.fesen.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A response to {@link ClearVotingConfigExclusionsRequest} indicating that voting config exclusions have been cleared from the
 * cluster state.
 */
public class ClearVotingConfigExclusionsResponse extends ActionResponse implements ToXContentObject {
    public ClearVotingConfigExclusionsResponse() {
    }

    public ClearVotingConfigExclusionsResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return builder;
    }
}
