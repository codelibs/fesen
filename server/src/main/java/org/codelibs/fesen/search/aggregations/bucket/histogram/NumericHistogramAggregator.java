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

package org.codelibs.fesen.search.aggregations.bucket.histogram;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.codelibs.fesen.index.fielddata.SortedNumericDoubleValues;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.AggregatorFactories;
import org.codelibs.fesen.search.aggregations.BucketOrder;
import org.codelibs.fesen.search.aggregations.CardinalityUpperBound;
import org.codelibs.fesen.search.aggregations.LeafBucketCollector;
import org.codelibs.fesen.search.aggregations.LeafBucketCollectorBase;
import org.codelibs.fesen.search.aggregations.support.ValuesSource;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceConfig;
import org.codelibs.fesen.search.internal.SearchContext;

/**
 * An aggregator for numeric values. For a given {@code interval},
 * {@code offset} and {@code value}, it returns the highest number that can be
 * written as {@code interval * x + offset} and yet is less than or equal to
 * {@code value}.
 */
public class NumericHistogramAggregator extends AbstractHistogramAggregator {
    private final ValuesSource.Numeric valuesSource;

    public NumericHistogramAggregator(String name, AggregatorFactories factories, double interval, double offset, BucketOrder order,
            boolean keyed, long minDocCount, DoubleBounds extendedBounds, DoubleBounds hardBounds, ValuesSourceConfig valuesSourceConfig,
            SearchContext context, Aggregator parent, CardinalityUpperBound cardinalityUpperBound, Map<String, Object> metadata)
            throws IOException {
        super(name, factories, interval, offset, order, keyed, minDocCount, extendedBounds, hardBounds, valuesSourceConfig.format(),
                context, parent, cardinalityUpperBound, metadata);
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    double previousKey = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < valuesCount; ++i) {
                        double value = values.nextValue();
                        double key = Math.floor((value - offset) / interval);
                        assert key >= previousKey;
                        if (key == previousKey) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(key * interval)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, Double.doubleToLongBits(key));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousKey = key;
                    }
                }
            }
        };
    }
}
