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

import org.codelibs.fesen.Version;
import org.codelibs.fesen.analysis.common.AnalysisPredicateScript;
import org.codelibs.fesen.analysis.common.CommonAnalysisPlugin;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.env.TestEnvironment;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.analysis.IndexAnalyzers;
import org.codelibs.fesen.index.analysis.NamedAnalyzer;
import org.codelibs.fesen.indices.analysis.AnalysisModule;
import org.codelibs.fesen.script.Script;
import org.codelibs.fesen.script.ScriptContext;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.test.ESTokenStreamTestCase;
import org.codelibs.fesen.test.IndexSettingsModule;

import java.util.Collections;

public class ScriptedConditionTokenFilterTests extends ESTokenStreamTestCase {

    public void testSimpleCondition() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.cond.type", "condition")
            .put("index.analysis.filter.cond.script.source", "token.getPosition() > 1")
            .putList("index.analysis.filter.cond.filter", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "cond")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        AnalysisPredicateScript.Factory factory = () -> new AnalysisPredicateScript() {
            @Override
            public boolean execute(Token token) {
                return token.getPosition() > 1;
            }
        };

        @SuppressWarnings("unchecked")
        ScriptService scriptService = new ScriptService(indexSettings, Collections.emptyMap(), Collections.emptyMap()){
            @Override
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                assertEquals(context, AnalysisPredicateScript.CONTEXT);
                assertEquals(new Script("token.getPosition() > 1"), script);
                return (FactoryType) factory;
            }
        };

        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();
        plugin.createComponents(null, null, null, null, scriptService, null, null, null, null, null, null);
        AnalysisModule module
            = new AnalysisModule(TestEnvironment.newEnvironment(settings), Collections.singletonList(plugin));

        IndexAnalyzers analyzers = module.getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = analyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "Vorsprung Durch Technik", new String[]{
                "Vorsprung", "Durch", "TECHNIK"
            });
        }

    }

}
