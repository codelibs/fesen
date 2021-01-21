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

package org.codelibs.fesen.action.admin.cluster.snapshots.get;

import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.codelibs.fesen.common.UUIDs;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.snapshots.SnapshotId;
import org.codelibs.fesen.snapshots.SnapshotInfo;
import org.codelibs.fesen.snapshots.SnapshotInfoTests;
import org.codelibs.fesen.snapshots.SnapshotShardFailure;
import org.codelibs.fesen.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class GetSnapshotsResponseTests extends AbstractSerializingTestCase<GetSnapshotsResponse> {

    @Override
    protected GetSnapshotsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSnapshotsResponse.fromXContent(parser);
    }

    @Override
    protected GetSnapshotsResponse createTestInstance() {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(5, 10); ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            snapshots.add(new SnapshotInfo(snapshotId, Arrays.asList("index1", "index2"), Collections.singletonList("ds"),
                System.currentTimeMillis(), reason, System.currentTimeMillis(), randomIntBetween(2, 3), shardFailures, randomBoolean(),
                SnapshotInfoTests.randomUserMetadata()));
        }
        return new GetSnapshotsResponse(snapshots);
    }

    @Override
    protected Writeable.Reader<GetSnapshotsResponse> instanceReader() {
        return GetSnapshotsResponse::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Don't inject random fields into the custom snapshot metadata, because the metadata map is equality-checked after doing a
        // round-trip through xContent serialization/deserialization. Even though the rest of the object ignores unknown fields,
        // `metadata` doesn't ignore unknown fields (it just includes them in the parsed object, because the keys are arbitrary), so any
        // new fields added to the metadata before it gets deserialized that weren't in the serialized version will cause the equality
        // check to fail.

        // The actual fields are nested in an array, so this regex matches fields with names of the form `snapshots.3.metadata`
        return Pattern.compile("snapshots\\.\\d+\\.metadata.*").asPredicate();
    }
}
