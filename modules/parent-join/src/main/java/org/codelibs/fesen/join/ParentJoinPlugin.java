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

package org.codelibs.fesen.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codelibs.fesen.index.mapper.Mapper;
import org.codelibs.fesen.join.aggregations.ChildrenAggregationBuilder;
import org.codelibs.fesen.join.aggregations.InternalChildren;
import org.codelibs.fesen.join.aggregations.InternalParent;
import org.codelibs.fesen.join.aggregations.ParentAggregationBuilder;
import org.codelibs.fesen.join.mapper.ParentJoinFieldMapper;
import org.codelibs.fesen.join.query.HasChildQueryBuilder;
import org.codelibs.fesen.join.query.HasParentQueryBuilder;
import org.codelibs.fesen.join.query.ParentIdQueryBuilder;
import org.codelibs.fesen.plugins.MapperPlugin;
import org.codelibs.fesen.plugins.Plugin;
import org.codelibs.fesen.plugins.SearchPlugin;

public class ParentJoinPlugin extends Plugin implements SearchPlugin, MapperPlugin {

    public ParentJoinPlugin() {
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(new QuerySpec<>(HasChildQueryBuilder.NAME, HasChildQueryBuilder::new, HasChildQueryBuilder::fromXContent),
                new QuerySpec<>(HasParentQueryBuilder.NAME, HasParentQueryBuilder::new, HasParentQueryBuilder::fromXContent),
                new QuerySpec<>(ParentIdQueryBuilder.NAME, ParentIdQueryBuilder::new, ParentIdQueryBuilder::fromXContent));
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
                new AggregationSpec(ChildrenAggregationBuilder.NAME, ChildrenAggregationBuilder::new, ChildrenAggregationBuilder::parse)
                        .addResultReader(InternalChildren::new),
                new AggregationSpec(ParentAggregationBuilder.NAME, ParentAggregationBuilder::new, ParentAggregationBuilder::parse)
                        .addResultReader(InternalParent::new));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(ParentJoinFieldMapper.CONTENT_TYPE, new ParentJoinFieldMapper.TypeParser());
    }
}
