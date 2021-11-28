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

package org.codelibs.fesen.search.aggregations.bucket.global;

import java.io.IOException;
import java.util.Map;

import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.aggregations.AggregationExecutionException;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.search.internal.SearchContext;

public class GlobalAggregatorFactory extends AggregatorFactory {

    public GlobalAggregatorFactory(String name, QueryShardContext queryShardContext, AggregatorFactory parent,
            AggregatorFactories.Builder subFactories, Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality,
            Map<String, Object> metadata) throws IOException {
        if (parent != null) {
            throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a global " + "sub-aggregation [" + name
                    + "]. Global aggregations can only be defined as top level aggregations");
        }
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new AggregationExecutionException("Aggregation [" + name() + "] must have cardinality 1 but was [" + cardinality + "]");
        }
        return new GlobalAggregator(name, factories, searchContext, metadata);
    }
}
