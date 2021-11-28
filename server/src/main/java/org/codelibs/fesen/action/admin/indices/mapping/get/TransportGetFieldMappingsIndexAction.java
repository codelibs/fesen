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

package org.codelibs.fesen.action.admin.indices.mapping.get;

import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.single.shard.TransportSingleShardAction;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.routing.ShardsIterator;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.inject.Inject;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.common.regex.Regex;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.index.IndexService;
import org.codelibs.fesen.index.mapper.DocumentMapper;
import org.codelibs.fesen.index.mapper.Mapper;
import org.codelibs.fesen.index.mapper.MappingLookup;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.indices.IndicesService;
import org.codelibs.fesen.indices.TypeMissingException;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 */
public class TransportGetFieldMappingsIndexAction
        extends TransportSingleShardAction<GetFieldMappingsIndexRequest, GetFieldMappingsResponse> {

    private static final String ACTION_NAME = GetFieldMappingsAction.NAME + "[index]";

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportGetFieldMappingsIndexAction(ClusterService clusterService, TransportService transportService,
            IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                GetFieldMappingsIndexRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldMappingsIndexRequest request) {
        //internal action, index already resolved
        return false;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected GetFieldMappingsResponse shardOperation(final GetFieldMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        Version indexCreatedVersion = indexService.mapperService().getIndexSettings().getIndexVersionCreated();
        Predicate<String> metadataFieldPredicate = (f) -> indicesService.isMetadataField(indexCreatedVersion, f);
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        DocumentMapper mapper = indexService.mapperService().documentMapper();
        Collection<String> typeIntersection;
        if (request.types().length == 0) {
            typeIntersection = mapper == null ? Collections.emptySet() : Collections.singleton(mapper.type());
        } else {
            typeIntersection = mapper != null && Regex.simpleMatch(request.types(), mapper.type()) ? Collections.singleton(mapper.type())
                    : Collections.emptySet();
            if (typeIntersection.isEmpty()) {
                throw new TypeMissingException(shardId.getIndex(), request.types());
            }
        }

        Map<String, Map<String, FieldMappingMetadata>> typeMappings = new HashMap<>();
        for (String type : typeIntersection) {
            DocumentMapper documentMapper = indexService.mapperService().documentMapper(type);
            Map<String, FieldMappingMetadata> fieldMapping = findFieldMappingsByType(fieldPredicate, documentMapper, request);
            if (!fieldMapping.isEmpty()) {
                typeMappings.put(type, fieldMapping);
            }
        }
        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), Collections.unmodifiableMap(typeMappings)));
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> getResponseReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        static final String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }
    };

    private static Map<String, FieldMappingMetadata> findFieldMappingsByType(Predicate<String> fieldPredicate,
            DocumentMapper documentMapper, GetFieldMappingsIndexRequest request) {
        Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
        final MappingLookup allFieldMappers = documentMapper.mappers();
        for (String field : request.fields()) {
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    addFieldMapper(fieldPredicate, fieldMapper.name(), fieldMapper, fieldMappings, request.includeDefaults());
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    if (Regex.simpleMatch(field, fieldMapper.name())) {
                        addFieldMapper(fieldPredicate, fieldMapper.name(), fieldMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = allFieldMappers.getMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(fieldPredicate, field, fieldMapper, fieldMappings, request.includeDefaults());
                } else if (request.probablySingleFieldRequest()) {
                    fieldMappings.put(field, FieldMappingMetadata.NULL);
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    private static void addFieldMapper(Predicate<String> fieldPredicate, String field, Mapper fieldMapper,
            Map<String, FieldMappingMetadata> fieldMappings, boolean includeDefaults) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        if (fieldPredicate.test(field)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(fieldMapper, XContentType.JSON,
                        includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS, false);
                fieldMappings.put(field, new FieldMappingMetadata(fieldMapper.name(), bytes));
            } catch (IOException e) {
                throw new FesenException("failed to serialize XContent of field [" + field + "]", e);
            }
        }
    }
}
