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

import org.codelibs.fesen.Version;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ObjectParser;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.support.CoreValuesSourceType;
import org.codelibs.fesen.search.aggregations.support.ValuesSource;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceConfig;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceRegistry;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class ValueCountAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, ValueCountAggregationBuilder> {
    public static final String NAME = "value_count";
    public static final ValuesSourceRegistry.RegistryKey<MetricAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        MetricAggregatorSupplier.class
    );

    public static final ObjectParser<ValueCountAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, ValueCountAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        ValueCountAggregatorFactory.registerAggregators(builder);
    }

    public ValueCountAggregationBuilder(String name) {
        super(name);
    }

    protected ValueCountAggregationBuilder(ValueCountAggregationBuilder clone,
                                           AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ValueCountAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ValueCountAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    protected ValueCountAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                        ValuesSourceConfig config,
                                                        AggregatorFactory parent,
                                                        AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new ValueCountAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
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
