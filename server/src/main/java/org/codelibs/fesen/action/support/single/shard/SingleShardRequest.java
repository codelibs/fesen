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

package org.codelibs.fesen.action.support.single.shard;

import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionRequestValidationException;
import org.codelibs.fesen.action.IndicesRequest;
import org.codelibs.fesen.action.ValidateActions;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.index.shard.ShardId;

import java.io.IOException;

public abstract class SingleShardRequest<Request extends SingleShardRequest<Request>> extends ActionRequest implements IndicesRequest {

    public static final IndicesOptions INDICES_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    /**
     * The concrete index name
     *
     * Whether index property is optional depends on the concrete implementation. If index property is required the
     * concrete implementation should use {@link #validateNonNullIndex()} to check if the index property has been set
     */
    @Nullable
    protected String index;
    ShardId internalShardId;

    public SingleShardRequest() {
    }

    public SingleShardRequest(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            internalShardId = new ShardId(in);
        }
        index = in.readOptionalString();
        // no need to pass threading over the network, they are always false when coming throw a thread pool
    }

    protected SingleShardRequest(String index) {
        this.index = index;
    }

    /**
     * @return a validation exception if the index property hasn't been set
     */
    protected ActionRequestValidationException validateNonNullIndex() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = ValidateActions.addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * @return The concrete index this request is targeted for or <code>null</code> if index is optional.
     *         Whether index property is optional depends on the concrete implementation. If index property
     *         is required the concrete implementation should use {@link #validateNonNullIndex()} to check
     *         if the index property has been set
     */
    @Nullable
    public String index() {
        return index;
    }

    /**
     * Sets the index.
     */
    @SuppressWarnings("unchecked")
    public final Request index(String index) {
        this.index = index;
        return (Request) this;
    }

    @Override
    public String[] indices() {
        return new String[]{index};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return INDICES_OPTIONS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(internalShardId);
        out.writeOptionalString(index);
    }
}

