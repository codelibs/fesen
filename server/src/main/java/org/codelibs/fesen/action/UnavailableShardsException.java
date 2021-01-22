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

package org.codelibs.fesen.action;

import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.common.Nullable;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.rest.RestStatus;

import java.io.IOException;

public class UnavailableShardsException extends FesenException {

    public UnavailableShardsException(@Nullable ShardId shardId, String message, Object... args) {
        super(buildMessage(shardId, message), args);
    }

    public UnavailableShardsException(String index, int shardId, String message, Object... args) {
        super(buildMessage(index, shardId, message), args);
    }

    private static String buildMessage(ShardId shardId, String message) {
        if (shardId == null) {
            return message;
        }
        return buildMessage(shardId.getIndexName(), shardId.id(), message);
    }

    private static String buildMessage(String index, int shardId, String message) {return "[" + index + "][" + shardId + "] " + message;}

    public UnavailableShardsException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
