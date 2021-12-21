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

import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.MultiValueMode;
import org.codelibs.fesen.search.aggregations.AggregationExecutionException;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.search.aggregations.support.ArrayValuesSourceAggregatorFactory;
import org.codelibs.fesen.search.aggregations.support.ValuesSource;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceConfig;
import org.codelibs.fesen.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class MatrixStatsAggregatorFactory extends ArrayValuesSourceAggregatorFactory {

    private final MultiValueMode multiValueMode;

    MatrixStatsAggregatorFactory(String name,
                                    Map<String, ValuesSourceConfig> configs,
                                    MultiValueMode multiValueMode,
                                    QueryShardContext queryShardContext,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metadata) throws IOException {
        super(name, configs, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.multiValueMode = multiValueMode;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata)
        throws IOException {
        return new MatrixStatsAggregator(name, null, searchContext, parent, multiValueMode, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSource> valuesSources,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            CardinalityUpperBound cardinality,
                                            Map<String, Object> metadata) throws IOException {
        Map<String, ValuesSource.Numeric> typedValuesSources = new HashMap<>(valuesSources.size());
        for (Map.Entry<String, ValuesSource> entry : valuesSources.entrySet()) {
            if (entry.getValue() instanceof ValuesSource.Numeric == false) {
                throw new AggregationExecutionException("ValuesSource type " + entry.getValue().toString() +
                    "is not supported for aggregation " + this.name());
            }
            // TODO: There must be a better option than this.
            typedValuesSources.put(entry.getKey(), (ValuesSource.Numeric) entry.getValue());
        }
        return new MatrixStatsAggregator(name, typedValuesSources, searchContext, parent, multiValueMode, metadata);
    }
}
