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
package org.codelibs.fesen.search.fetch.subphase;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.codelibs.fesen.search.fetch.FetchContext;
import org.codelibs.fesen.search.fetch.FetchSubPhase;
import org.codelibs.fesen.search.fetch.FetchSubPhaseProcessor;
import org.codelibs.fesen.search.rescore.RescoreContext;

/**
 * Explains the scoring calculations for the top hits.
 */
public final class ExplainPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.explain() == false) {
            return null;
        }
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                final int topLevelDocId = hitContext.hit().docId();
                Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);

                for (RescoreContext rescore : context.rescore()) {
                    explanation = rescore.rescorer().explain(topLevelDocId, context.searcher(), rescore, explanation);
                }
                // we use the top level doc id, since we work with the top level searcher
                hitContext.hit().explanation(explanation);
            }
        };
    }
}
