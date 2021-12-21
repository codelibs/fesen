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
package org.codelibs.fesen.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.codelibs.fesen.common.lucene.Lucene;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.search.aggregations.InternalAggregation;
import org.codelibs.fesen.search.aggregations.LeafBucketCollector;
import org.codelibs.fesen.search.aggregations.LeafBucketCollectorBase;
import org.codelibs.fesen.search.aggregations.bucket.BucketsAggregator;
import org.codelibs.fesen.search.aggregations.bucket.SingleBucketAggregator;
import org.codelibs.fesen.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Aggregate all docs that match a filter.
 */
public class FilterAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final Supplier<Weight> filter;

    public FilterAggregator(String name,
                            Supplier<Weight> filter,
                            AggregatorFactories factories,
                            SearchContext context,
                            Aggregator parent,
                            CardinalityUpperBound cardinality,
                            Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.filter = filter;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        // no need to provide deleted docs to the filter
        final Bits bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filter.get().scorerSupplier(ctx));
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bits.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalFilter(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations(), metadata());
    }
}


