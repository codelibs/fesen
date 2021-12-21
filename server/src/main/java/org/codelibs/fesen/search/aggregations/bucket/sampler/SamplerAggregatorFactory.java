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

package org.codelibs.fesen.search.aggregations.bucket.sampler;

import java.io.IOException;
import java.util.Map;

import org.codelibs.fesen.index.query.QueryShardContext;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.AggregatorFactory;
import org.codelibs.fesen.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.search.internal.SearchContext;

public class SamplerAggregatorFactory extends AggregatorFactory {

    private final int shardSize;

    SamplerAggregatorFactory(String name, int shardSize, QueryShardContext queryShardContext, AggregatorFactory parent,
                             AggregatorFactories.Builder subFactories, Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        this.shardSize = shardSize;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        CardinalityUpperBound cardinality,
                                        Map<String, Object> metadata) throws IOException {
        return new SamplerAggregator(name, shardSize, factories, searchContext, parent, metadata);
    }

}
