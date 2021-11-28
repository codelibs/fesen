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

package org.codelibs.fesen.action.admin.cluster.repositories.get;

import static org.codelibs.fesen.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.cluster.metadata.RepositoriesMetadata;
import org.codelibs.fesen.cluster.metadata.RepositoryMetadata;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ToXContentObject;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;

/**
 * Get repositories response
 */
public class GetRepositoriesResponse extends ActionResponse implements ToXContentObject {

    private RepositoriesMetadata repositories;

    GetRepositoriesResponse(RepositoriesMetadata repositories) {
        this.repositories = repositories;
    }

    GetRepositoriesResponse(StreamInput in) throws IOException {
        repositories = new RepositoriesMetadata(in);
    }

    /**
     * List of repositories to return
     *
     * @return list or repositories
     */
    public List<RepositoryMetadata> repositories() {
        return repositories.repositories();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositories.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositories.toXContent(builder,
                new DelegatingMapParams(Collections.singletonMap(RepositoriesMetadata.HIDE_GENERATIONS_PARAM, "true"), params));
        builder.endObject();
        return builder;
    }

    public static GetRepositoriesResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new GetRepositoriesResponse(RepositoriesMetadata.fromXContent(parser));
    }
}
