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

package org.codelibs.fesen.search.geo;

import static org.hamcrest.Matchers.containsString;

import org.codelibs.fesen.action.search.SearchAction;
import org.codelibs.fesen.action.search.SearchPhaseExecutionException;
import org.codelibs.fesen.action.search.SearchRequestBuilder;
import org.codelibs.fesen.common.geo.GeoShapeType;
import org.codelibs.fesen.common.geo.ShapeRelation;
import org.codelibs.fesen.common.geo.builders.CoordinatesBuilder;
import org.codelibs.fesen.common.geo.builders.LineStringBuilder;
import org.codelibs.fesen.common.geo.builders.MultiLineStringBuilder;
import org.codelibs.fesen.common.geo.builders.MultiPointBuilder;
import org.codelibs.fesen.common.geo.builders.PointBuilder;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.geometry.Line;
import org.codelibs.fesen.geometry.LinearRing;
import org.codelibs.fesen.geometry.MultiLine;
import org.codelibs.fesen.geometry.MultiPoint;
import org.codelibs.fesen.geometry.Point;
import org.codelibs.fesen.geometry.Rectangle;
import org.codelibs.fesen.index.query.GeoShapeQueryBuilder;
import org.codelibs.fesen.index.query.QueryBuilders;

public class GeoPointShapeQueryTests extends GeoQueryTests {

    @Override
    protected XContentBuilder createTypedMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("location").field("type", "geo_point").endObject().endObject().endObject().endObject();

        return xcb;
    }

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject(defaultGeoFieldName)
                .field("type", "geo_point").endObject().endObject().endObject();

        return xcb;
    }

    public void testProcessRelationSupport() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        Rectangle rectangle = new Rectangle(-35, -25, -25, -35);

        for (ShapeRelation shapeRelation : ShapeRelation.values()) {
            if (shapeRelation.equals(ShapeRelation.INTERSECTS) == false) {
                try {
                    client().prepareSearch("test")
                            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, rectangle).relation(shapeRelation)).get();
                } catch (SearchPhaseExecutionException e) {
                    assertThat(e.getCause().getMessage(),
                            containsString(shapeRelation + " query relation not supported for Field [" + defaultGeoFieldName + "]"));
                }
            }
        }
    }

    public void testQueryLine() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        Line line = new Line(new double[] { -25, -25 }, new double[] { -35, -35 });

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, line)).get();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.LINESTRING + " queries"));
        }
    }

    public void testQueryLinearRing() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        LinearRing linearRing = new LinearRing(new double[] { -25, -35, -25 }, new double[] { -25, -35, -25 });

        try {
            // LinearRing extends Line implements Geometry: expose the build process
            GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder(defaultGeoFieldName, linearRing);
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
            searchRequestBuilder.setQuery(queryBuilder);
            searchRequestBuilder.setIndices("test");
            searchRequestBuilder.get();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getMessage(),
                    containsString("Field [" + defaultGeoFieldName + "] does not support LINEARRING queries"));
        }
    }

    public void testQueryMultiLine() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        CoordinatesBuilder coords1 = new CoordinatesBuilder().coordinate(-35, -35).coordinate(-25, -25);
        CoordinatesBuilder coords2 = new CoordinatesBuilder().coordinate(-15, -15).coordinate(-5, -5);
        LineStringBuilder lsb1 = new LineStringBuilder(coords1);
        LineStringBuilder lsb2 = new LineStringBuilder(coords2);
        MultiLineStringBuilder mlb = new MultiLineStringBuilder().linestring(lsb1).linestring(lsb2);
        MultiLine multiline = (MultiLine) mlb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, multiline)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.MULTILINESTRING + " queries"));
        }
    }

    public void testQueryMultiPoint() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        MultiPointBuilder mpb = new MultiPointBuilder().coordinate(-35, -25).coordinate(-15, -5);
        MultiPoint multiPoint = mpb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, multiPoint)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.MULTIPOINT + " queries"));
        }
    }

    public void testQueryPoint() throws Exception {
        XContentBuilder xcb = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc", xcb).get();
        ensureGreen();

        PointBuilder pb = new PointBuilder().coordinate(-35, -25);
        Point point = pb.buildGeometry();

        try {
            client().prepareSearch("test").setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, point)).get();
        } catch (Exception e) {
            assertThat(e.getCause().getMessage(), containsString("does not support " + GeoShapeType.POINT + " queries"));
        }
    }
}
