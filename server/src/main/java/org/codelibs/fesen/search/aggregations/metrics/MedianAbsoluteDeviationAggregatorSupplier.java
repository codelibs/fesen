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

import org.codelibs.fesen.search.DocValueFormat;
import org.codelibs.fesen.search.aggregations.Aggregator;
import org.codelibs.fesen.search.aggregations.support.ValuesSource;
import org.codelibs.fesen.search.internal.SearchContext;

public interface MedianAbsoluteDeviationAggregatorSupplier {
    Aggregator build(String name,
                     ValuesSource valuesSource,
                     DocValueFormat format,
                     SearchContext context,
                     Aggregator parent,
                     Map<String, Object> metadata,
                     double compression) throws IOException;
}
