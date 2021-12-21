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

package org.codelibs.fesen.geometry.utils;

import org.codelibs.fesen.geometry.Circle;
import org.codelibs.fesen.geometry.Geometry;
import org.codelibs.fesen.geometry.GeometryCollection;
import org.codelibs.fesen.geometry.GeometryVisitor;
import org.codelibs.fesen.geometry.Line;
import org.codelibs.fesen.geometry.LinearRing;
import org.codelibs.fesen.geometry.MultiLine;
import org.codelibs.fesen.geometry.MultiPoint;
import org.codelibs.fesen.geometry.MultiPolygon;
import org.codelibs.fesen.geometry.Point;
import org.codelibs.fesen.geometry.Polygon;
import org.codelibs.fesen.geometry.Rectangle;

/**
 * Validator that only checks that altitude only shows up if ignoreZValue is set to true.
 */
public class StandardValidator implements GeometryValidator {

    private final boolean ignoreZValue;

    public StandardValidator(boolean ignoreZValue) {
       this.ignoreZValue = ignoreZValue;
    }

    protected void checkZ(double zValue) {
        if (ignoreZValue == false && Double.isNaN(zValue) == false) {
            throw new IllegalArgumentException("found Z value [" + zValue + "] but [ignore_z_value] "
                + "parameter is [" + ignoreZValue + "]");
        }
    }

    @Override
    public void validate(Geometry geometry) {
        if (ignoreZValue == false) {
            geometry.visit(new GeometryVisitor<Void, RuntimeException>() {

                @Override
                public Void visit(Circle circle) throws RuntimeException {
                    checkZ(circle.getZ());
                    return null;
                }

                @Override
                public Void visit(GeometryCollection<?> collection) throws RuntimeException {
                    for (Geometry g : collection) {
                        g.visit(this);
                    }
                    return null;
                }

                @Override
                public Void visit(Line line) throws RuntimeException {
                    for (int i = 0; i < line.length(); i++) {
                        checkZ(line.getZ(i));
                    }
                    return null;
                }

                @Override
                public Void visit(LinearRing ring) throws RuntimeException {
                    for (int i = 0; i < ring.length(); i++) {
                        checkZ(ring.getZ(i));
                    }
                    return null;
                }

                @Override
                public Void visit(MultiLine multiLine) throws RuntimeException {
                    return visit((GeometryCollection<?>) multiLine);
                }

                @Override
                public Void visit(MultiPoint multiPoint) throws RuntimeException {
                    return visit((GeometryCollection<?>) multiPoint);
                }

                @Override
                public Void visit(MultiPolygon multiPolygon) throws RuntimeException {
                    return visit((GeometryCollection<?>) multiPolygon);
                }

                @Override
                public Void visit(Point point) throws RuntimeException {
                    checkZ(point.getZ());
                    return null;
                }

                @Override
                public Void visit(Polygon polygon) throws RuntimeException {
                    polygon.getPolygon().visit(this);
                    for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                        polygon.getHole(i).visit(this);
                    }
                    return null;
                }

                @Override
                public Void visit(Rectangle rectangle) throws RuntimeException {
                    checkZ(rectangle.getMinZ());
                    checkZ(rectangle.getMaxZ());
                    return null;
                }
            });
        }
    }
}

