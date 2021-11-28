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

package org.codelibs.fesen.index.shard;

import static org.hamcrest.Matchers.equalTo;

import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.index.shard.DocsStats;
import org.codelibs.fesen.test.ESTestCase;

public class DocsStatsTests extends ESTestCase {

    public void testCalculateAverageDocSize() throws Exception {
        DocsStats stats = new DocsStats(10, 2, 120);
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        stats.add(new DocsStats(0, 0, 0));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        stats.add(new DocsStats(8, 30, 480));
        assertThat(stats.getCount(), equalTo(18L));
        assertThat(stats.getDeleted(), equalTo(32L));
        assertThat(stats.getTotalSizeInBytes(), equalTo(600L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(12L));
    }

    public void testUninitialisedShards() {
        DocsStats stats = new DocsStats(0, 0, -1);
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(0L));
        stats.add(new DocsStats(0, 0, -1));
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(0L));
        stats.add(new DocsStats(1, 0, 10));
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats(0, 0, -1));
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats(1, 0, 20));
        assertThat(stats.getTotalSizeInBytes(), equalTo(30L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(15L));
    }

    public void testSerialize() throws Exception {
        DocsStats originalStats = new DocsStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                DocsStats cloneStats = new DocsStats(in);
                assertThat(cloneStats.getCount(), equalTo(originalStats.getCount()));
                assertThat(cloneStats.getDeleted(), equalTo(originalStats.getDeleted()));
                assertThat(cloneStats.getAverageSizeInBytes(), equalTo(originalStats.getAverageSizeInBytes()));
            }
        }
    }
}
