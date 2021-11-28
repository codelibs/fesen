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
import java.io.StringReader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.index.analysis.AnalysisTestsHelper;
import org.codelibs.fesen.index.analysis.TokenFilterFactory;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.ESTokenStreamTestCase;

public class ASCIIFoldingTokenFilterFactoryTests extends ESTokenStreamTestCase {
    public void testDefault() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper
                .createTestAnalysisFromSettings(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                        .put("index.analysis.filter.my_ascii_folding.type", "asciifolding").build(), new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_ascii_folding");
        String source = "Ansprüche";
        String[] expected = new String[] { "Anspruche" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testPreserveOriginal() throws IOException {
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper
                .createTestAnalysisFromSettings(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                        .put("index.analysis.filter.my_ascii_folding.type", "asciifolding")
                        .put("index.analysis.filter.my_ascii_folding.preserve_original", true).build(), new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_ascii_folding");
        String source = "Ansprüche";
        String[] expected = new String[] { "Anspruche", "Ansprüche" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);

        // but the multi-term aware component still emits a single token
        tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        expected = new String[] { "Anspruche" };
        assertTokenStreamContents(tokenFilter.normalize(tokenizer), expected);
    }
}
