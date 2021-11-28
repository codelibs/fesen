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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.analysis.AbstractIndexAnalyzerProvider;
import org.codelibs.fesen.index.analysis.Analysis;

public class ArmenianAnalyzerProvider extends AbstractIndexAnalyzerProvider<ArmenianAnalyzer> {

    private final ArmenianAnalyzer analyzer;

    ArmenianAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        analyzer = new ArmenianAnalyzer(Analysis.parseStopWords(env, settings, ArmenianAnalyzer.getDefaultStopSet()),
                Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET));
        analyzer.setVersion(version);
    }

    @Override
    public ArmenianAnalyzer get() {
        return this.analyzer;
    }
}
