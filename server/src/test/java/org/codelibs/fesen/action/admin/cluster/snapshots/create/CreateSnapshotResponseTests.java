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

package org.codelibs.fesen.action.admin.cluster.snapshots.create;

import org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.snapshots.SnapshotId;
import org.codelibs.fesen.snapshots.SnapshotInfo;
import org.codelibs.fesen.snapshots.SnapshotInfoTests;
import org.codelibs.fesen.snapshots.SnapshotShardFailure;
import org.codelibs.fesen.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

public class CreateSnapshotResponseTests extends AbstractXContentTestCase<CreateSnapshotResponse> {

    @Override
    protected CreateSnapshotResponse doParseInstance(XContentParser parser) throws IOException {
        return CreateSnapshotResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected CreateSnapshotResponse createTestInstance() {
        SnapshotId snapshotId = new SnapshotId("test", UUID.randomUUID().toString());
        List<String> indices = new ArrayList<>();
        indices.add("test0");
        indices.add("test1");

        List<String> dataStreams = new ArrayList<>();
        dataStreams.add("test0");
        dataStreams.add("test1");
        String reason = "reason";
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 10000;
        int totalShards = randomIntBetween(1, 3);
        int successfulShards = randomIntBetween(0, totalShards);
        List<SnapshotShardFailure> shardFailures = new ArrayList<>();

        for (int count = successfulShards; count < totalShards; ++count) {
            shardFailures.add(new SnapshotShardFailure(
                "node-id", new ShardId("index-" + count, UUID.randomUUID().toString(), randomInt()), "reason"));
        }

        boolean globalState = randomBoolean();

        return new CreateSnapshotResponse(
            new SnapshotInfo(snapshotId, indices, dataStreams, startTime, reason, endTime, totalShards, shardFailures,
                globalState, SnapshotInfoTests.randomUserMetadata()));
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Don't inject random fields into the custom snapshot metadata, because the metadata map is equality-checked after doing a
        // round-trip through xContent serialization/deserialization. Even though the rest of the object ignores unknown fields,
        // `metadata` doesn't ignore unknown fields (it just includes them in the parsed object, because the keys are arbitrary), so any
        // new fields added to the metadata before it gets deserialized that weren't in the serialized version will cause the equality
        // check to fail.
        return field -> field.startsWith("snapshot.metadata");
    }
}
