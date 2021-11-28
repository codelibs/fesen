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

package org.codelibs.fesen.common.geo;

import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.geo.GeoJson;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.ToXContentObject;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.geo.GeometryTestUtils;
import org.codelibs.fesen.geometry.Geometry;
import org.codelibs.fesen.geometry.utils.GeographyValidator;
import org.codelibs.fesen.test.AbstractXContentTestCase;
import org.codelibs.fesen.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.codelibs.fesen.geo.GeometryTestUtils.randomCircle;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomGeometryCollection;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomLine;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomMultiLine;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomMultiPoint;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomMultiPolygon;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomPoint;
import static org.codelibs.fesen.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;

public class GeoJsonSerializationTests extends ESTestCase {

    private static class GeometryWrapper implements ToXContentObject {

        private final Geometry geometry;
        private static final GeoJson PARSER = new GeoJson(true, false, new GeographyValidator(true));

        GeometryWrapper(Geometry geometry) {
            this.geometry = geometry;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return GeoJson.toXContent(geometry, builder, params);
        }

        public static GeometryWrapper fromXContent(XContentParser parser) throws IOException {
            parser.nextToken();
            return new GeometryWrapper(PARSER.fromXContent(parser));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            GeometryWrapper that = (GeometryWrapper) o;
            return Objects.equals(geometry, that.geometry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geometry);
        }
    }

    private void xContentTest(Supplier<Geometry> instanceSupplier) throws IOException {
        AbstractXContentTestCase.xContentTester(this::createParser, () -> new GeometryWrapper(instanceSupplier.get()),
                (geometryWrapper, xContentBuilder) -> {
                    geometryWrapper.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                }, GeometryWrapper::fromXContent).supportsUnknownFields(true).test();
    }

    public void testPoint() throws IOException {
        xContentTest(() -> randomPoint(randomBoolean()));
    }

    public void testMultiPoint() throws IOException {
        xContentTest(() -> randomMultiPoint(randomBoolean()));
    }

    public void testLineString() throws IOException {
        xContentTest(() -> randomLine(randomBoolean()));
    }

    public void testMultiLineString() throws IOException {
        xContentTest(() -> randomMultiLine(randomBoolean()));
    }

    public void testPolygon() throws IOException {
        xContentTest(() -> randomPolygon(randomBoolean()));
    }

    public void testMultiPolygon() throws IOException {
        xContentTest(() -> randomMultiPolygon(randomBoolean()));
    }

    public void testEnvelope() throws IOException {
        xContentTest(GeometryTestUtils::randomRectangle);
    }

    public void testGeometryCollection() throws IOException {
        xContentTest(() -> randomGeometryCollection(randomBoolean()));
    }

    public void testCircle() throws IOException {
        xContentTest(() -> randomCircle(randomBoolean()));
    }

    public void testToMap() throws IOException {
        for (int i = 0; i < 10; i++) {
            Geometry geometry = GeometryTestUtils.randomGeometry(randomBoolean());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
            StreamInput input = BytesReference.bytes(builder).streamInput();

            try (XContentParser parser =
                    XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, input)) {
                Map<String, Object> map = GeoJson.toMap(geometry);
                assertThat(parser.map(), equalTo(map));
            }
        }
    }
}
