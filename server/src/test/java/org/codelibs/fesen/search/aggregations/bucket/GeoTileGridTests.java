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

package org.codelibs.fesen.search.aggregations.bucket;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.common.geo.GeoBoundingBox;
import org.codelibs.fesen.common.geo.GeoBoundingBoxTests;
import org.codelibs.fesen.common.geo.GeoPoint;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.io.stream.NamedWriteableAwareStreamInput;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.search.aggregations.BaseAggregationTestCase;
import org.codelibs.fesen.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.codelibs.fesen.test.VersionUtils;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder> {

    @Override
    protected GeoTileGridAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        GeoTileGridAggregationBuilder factory = new GeoTileGridAggregationBuilder(name);
        factory.field("foo");
        if (randomBoolean()) {
            factory.precision(randomIntBetween(0, GeoTileUtils.MAX_ZOOM));
        }
        if (randomBoolean()) {
            factory.size(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.setGeoBoundingBox(GeoBoundingBoxTests.randomBBox());
        }
        return factory;
    }

    public void testSerializationPreBounds() throws Exception {
        Version noBoundsSupportVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_0);
        GeoTileGridAggregationBuilder builder = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_6_0);
            builder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                new NamedWriteableRegistry(Collections.emptyList()))) {
                in.setVersion(noBoundsSupportVersion);
                GeoTileGridAggregationBuilder readBuilder = new GeoTileGridAggregationBuilder(in);
                assertThat(readBuilder.geoBoundingBox(), equalTo(new GeoBoundingBox(
                    new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN))));
            }
        }
    }
}
