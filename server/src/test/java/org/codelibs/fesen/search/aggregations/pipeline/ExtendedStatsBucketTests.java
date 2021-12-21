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

package org.codelibs.fesen.search.aggregations.pipeline;

import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.PipelineAggregator;
import org.codelibs.fesen.search.aggregations.support.ValueType;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExtendedStatsBucketTests extends AbstractBucketMetricsTestCase<ExtendedStatsBucketPipelineAggregationBuilder> {

    @Override
    protected ExtendedStatsBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        ExtendedStatsBucketPipelineAggregationBuilder factory = new ExtendedStatsBucketPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            factory.sigma(randomDoubleBetween(0.0, 10.0, false));
        }
        return factory;
    }

    public void testSigmaFromInt() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("name")
                    .startObject("extended_stats_bucket")
                        .field("sigma", 5)
                        .field("buckets_path", "test")
                    .endObject()
                .endObject()
            .endObject();

        ExtendedStatsBucketPipelineAggregationBuilder builder = (ExtendedStatsBucketPipelineAggregationBuilder) parse(
                createParser(content));

        assertThat(builder.sigma(), equalTo(5.0));
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        assertThat(validate(aggBuilders, new ExtendedStatsBucketPipelineAggregationBuilder("name", "invalid_agg>metric")), equalTo(
                "Validation Failed: 1: " + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " aggregation does not exist for aggregation [name]: invalid_agg>metric;"));

        // Now try to point to a single bucket agg
        assertThat(validate(aggBuilders, new ExtendedStatsBucketPipelineAggregationBuilder("name", "global>metric")), equalTo(
                "Validation Failed: 1: The first aggregation in " + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " must be a multi-bucket aggregation for aggregation [name] found :" + GlobalAggregationBuilder.class.getName()
                + " for buckets path: global>metric;"));

        // Now try to point to a valid multi-bucket agg
        assertThat(validate(aggBuilders, new ExtendedStatsBucketPipelineAggregationBuilder("name", "terms>metric")), nullValue());
    }
}
