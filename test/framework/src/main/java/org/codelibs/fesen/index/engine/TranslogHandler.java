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

package org.codelibs.fesen.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.VersionType;
import org.codelibs.fesen.index.analysis.AnalysisRegistry;
import org.codelibs.fesen.index.analysis.AnalyzerScope;
import org.codelibs.fesen.index.analysis.IndexAnalyzers;
import org.codelibs.fesen.index.analysis.NamedAnalyzer;
import org.codelibs.fesen.index.engine.Engine;
import org.codelibs.fesen.index.mapper.DocumentMapper;
import org.codelibs.fesen.index.mapper.DocumentMapperForType;
import org.codelibs.fesen.index.mapper.MapperService;
import org.codelibs.fesen.index.mapper.Mapping;
import org.codelibs.fesen.index.mapper.RootObjectMapper;
import org.codelibs.fesen.index.mapper.SourceToParse;
import org.codelibs.fesen.index.seqno.SequenceNumbers;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.similarity.SimilarityService;
import org.codelibs.fesen.index.translog.Translog;
import org.codelibs.fesen.indices.IndicesModule;
import org.codelibs.fesen.indices.mapper.MapperRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TranslogHandler implements Engine.TranslogRecoveryRunner {

    private final MapperService mapperService;
    public Mapping mappingUpdate = null;
    private final Map<String, Mapping> recoveredTypes = new HashMap<>();

    private final AtomicLong appliedOperations = new AtomicLong();

    long appliedOperations() {
        return appliedOperations.get();
    }

    public TranslogHandler(NamedXContentRegistry xContentRegistry, IndexSettings indexSettings) {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()));
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, emptyMap(), emptyMap());
        SimilarityService similarityService = new SimilarityService(indexSettings, null, emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(emptyList()).getMapperRegistry();
        mapperService = new MapperService(indexSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                () -> null, () -> false, null);
    }

    private DocumentMapperForType docMapper(String type) {
        RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
        DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
        return new DocumentMapperForType(b.build(mapperService), mappingUpdate);
    }

    private void applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        switch (operation.operationType()) {
            case INDEX:
                Engine.Index engineIndex = (Engine.Index) operation;
                Mapping update = engineIndex.parsedDoc().dynamicMappingsUpdate();
                if (engineIndex.parsedDoc().dynamicMappingsUpdate() != null) {
                    recoveredTypes.compute(engineIndex.type(), (k, mapping) ->
                        mapping == null ? update : mapping.merge(update, MapperService.MergeReason.MAPPING_RECOVERY));
                }
                engine.index(engineIndex);
                break;
            case DELETE:
                engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

    /**
     * Returns the recovered types modifying the mapping during the recovery
     */
    public Map<String, Mapping> getRecoveredTypes() {
        return recoveredTypes;
    }

    @Override
    public int run(Engine engine, Translog.Snapshot snapshot) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            applyOperation(engine, convertToEngineOp(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY));
            opsRecovered++;
            appliedOperations.incrementAndGet();
        }
        engine.syncTranslog();
        return opsRecovered;
    }

    public Engine.Operation convertToEngineOp(Translog.Operation operation, Engine.Operation.Origin origin) {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                final String indexName = mapperService.index().getName();
                final Engine.Index engineIndex = IndexShard.prepareIndex(docMapper(index.type()),
                    new SourceToParse(indexName, index.type(), index.id(), index.source(), XContentHelper.xContentType(index.source()),
                        index.routing()), index.seqNo(), index.primaryTerm(), index.version(), versionType, origin,
                    index.getAutoGeneratedIdTimestamp(), true, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
                return engineIndex;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                final Engine.Delete engineDelete = new Engine.Delete(delete.type(), delete.id(), delete.uid(), delete.seqNo(),
                    delete.primaryTerm(), delete.version(), versionType, origin, System.nanoTime(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
                return engineDelete;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                final Engine.NoOp engineNoOp =
                        new Engine.NoOp(noOp.seqNo(), noOp.primaryTerm(), origin, System.nanoTime(), noOp.reason());
                return engineNoOp;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
    }

}
