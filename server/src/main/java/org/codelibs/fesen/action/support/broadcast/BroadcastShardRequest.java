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

package org.codelibs.fesen.action.support.broadcast;

import java.io.IOException;

import org.codelibs.fesen.action.IndicesRequest;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.transport.TransportRequest;

public abstract class BroadcastShardRequest extends TransportRequest implements IndicesRequest {

    private ShardId shardId;

    protected OriginalIndices originalIndices;

    protected BroadcastShardRequest() {
    }

    public BroadcastShardRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    protected BroadcastShardRequest(ShardId shardId, BroadcastRequest<? extends BroadcastRequest<?>> request) {
        this.shardId = shardId;
        this.originalIndices = new OriginalIndices(request);
    }

    protected BroadcastShardRequest(ShardId shardId, OriginalIndices originalIndices) {
        this.shardId = shardId;
        this.originalIndices = originalIndices;
    }

    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }
}
