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

package org.codelibs.fesen.search.aggregations.metrics;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ObjectParser;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.support.CoreValuesSourceType;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceConfig;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceRegistry;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceType;

public class GeoBoundsAggregationBuilder extends ValuesSourceAggregationBuilder<GeoBoundsAggregationBuilder> {
    public static final String NAME = "geo_bounds";
    public static final ValuesSourceRegistry.RegistryKey<GeoBoundsAggregatorSupplier> REGISTRY_KEY =
            new ValuesSourceRegistry.RegistryKey<>(NAME, GeoBoundsAggregatorSupplier.class);

    public static final ObjectParser<GeoBoundsAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, GeoBoundsAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareBoolean(GeoBoundsAggregationBuilder::wrapLongitude, GeoBoundsAggregator.WRAP_LONGITUDE_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        GeoBoundsAggregatorFactory.registerAggregators(builder);
    }

    private boolean wrapLongitude = true;

    public GeoBoundsAggregationBuilder(String name) {
        super(name);
    }

    protected GeoBoundsAggregationBuilder(GeoBoundsAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder,
            Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.wrapLongitude = clone.wrapLongitude;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GeoBoundsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public GeoBoundsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(wrapLongitude);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.GEOPOINT;
    }

    /**
     * Set whether to wrap longitudes. Defaults to true.
     */
    public GeoBoundsAggregationBuilder wrapLongitude(boolean wrapLongitude) {
        this.wrapLongitude = wrapLongitude;
        return this;
    }

    /**
     * Get whether to wrap longitudes.
     */
    public boolean wrapLongitude() {
        return wrapLongitude;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected GeoBoundsAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
            AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new GeoBoundsAggregatorFactory(name, config, wrapLongitude, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoBoundsAggregator.WRAP_LONGITUDE_FIELD.getPreferredName(), wrapLongitude);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), wrapLongitude);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        if (super.equals(obj) == false)
            return false;
        GeoBoundsAggregationBuilder other = (GeoBoundsAggregationBuilder) obj;
        return Objects.equals(wrapLongitude, other.wrapLongitude);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
