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
package org.codelibs.fesen.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.common.logging.DeprecationLogger;
import org.codelibs.fesen.common.lucene.ScorerAware;
import org.codelibs.fesen.index.fielddata.ScriptDocValues;
import org.codelibs.fesen.search.lookup.LeafSearchLookup;
import org.codelibs.fesen.search.lookup.SearchLookup;
import org.codelibs.fesen.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

abstract class AbstractSortScript implements ScorerAware {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = org.codelibs.fesen.core.Map.of(
            "doc", value -> {
                deprecationLogger.deprecate("sort-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an sort-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecate("sort-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an sort-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).loadSourceIfNeeded()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /** A scorer that will return the score for the current document when the script is run. */
    private Scorable scorer;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    AbstractSortScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        Map<String, Object> parameters = new HashMap<>(params);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
    }

    protected AbstractSortScript() {
        this.params = null;
        this.leafLookup = null;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public void setScorer(Scorable scorer) {
        this.scorer = scorer;
    }

    /** Return the score of the current document. */
    public double get_score() {
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new FesenException("couldn't lookup score", e);
        }
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Set the current document to run the script on next.
     */
    public void setDocument(int docid) {
        leafLookup.setDocument(docid);
    }
}
