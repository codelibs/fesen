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
package org.codelibs.fesen.index.suggest.stats;

import org.codelibs.fesen.common.FieldMemoryStats;
import org.codelibs.fesen.common.FieldMemoryStatsTests;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.search.suggest.completion.CompletionStats;
import org.codelibs.fesen.test.ESTestCase;

import java.io.IOException;

public class CompletionsStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        CompletionStats stats = new CompletionStats(randomNonNegativeLong(), map);
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        CompletionStats read = new CompletionStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getSizeInBytes(), read.getSizeInBytes());
        assertEquals(stats.getFields(), read.getFields());
    }
}
