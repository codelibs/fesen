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
package org.codelibs.fesen.search.aggregations.matrix.stats;

import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.MultiValueMode;
import org.codelibs.fesen.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class MatrixStatsAggregationBuilder extends ArrayValuesSourceAggregationBuilder.LeafOnly<MatrixStatsAggregationBuilder> {
    public static final String NAME = "matrix_stats";

    private MultiValueMode multiValueMode = MultiValueMode.AVG;

    public MatrixStatsAggregationBuilder(String name) {
        super(name);
    }

    protected MatrixStatsAggregationBuilder(MatrixStatsAggregationBuilder clone,
                                            AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.multiValueMode = clone.multiValueMode;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MatrixStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public MatrixStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    public MatrixStatsAggregationBuilder multiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }

    public MultiValueMode multiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected MatrixStatsAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                        Map<String, ValuesSourceConfig> configs,
                                                        AggregatorFactory parent,
                                                        AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new MatrixStatsAggregatorFactory(name, configs, multiValueMode, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(MULTIVALUE_MODE_FIELD.getPreferredName(), multiValueMode);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
