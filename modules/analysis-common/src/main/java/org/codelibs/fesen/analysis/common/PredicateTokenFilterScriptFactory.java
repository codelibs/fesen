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

package org.codelibs.fesen.analysis.common;

import java.io.IOException;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.analysis.AbstractTokenFilterFactory;
import org.codelibs.fesen.script.Script;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.script.ScriptType;

/**
 * A factory for creating FilteringTokenFilters that determine whether or not to
 * accept their underlying token by consulting a script
 */
public class PredicateTokenFilterScriptFactory extends AbstractTokenFilterFactory {

    private final AnalysisPredicateScript.Factory factory;

    public PredicateTokenFilterScriptFactory(IndexSettings indexSettings, String name, Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);
        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ScriptFilteringTokenFilter(tokenStream, factory.newInstance());
    }

    private static class ScriptFilteringTokenFilter extends FilteringTokenFilter {

        final AnalysisPredicateScript script;
        final AnalysisPredicateScript.Token token;

        ScriptFilteringTokenFilter(TokenStream in, AnalysisPredicateScript script) {
            super(in);
            this.script = script;
            this.token = new AnalysisPredicateScript.Token(this);
        }

        @Override
        protected boolean accept() throws IOException {
            token.updatePosition();
            return script.execute(token);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            this.token.reset();
        }
    }
}
