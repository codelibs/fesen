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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.codelibs.fesen.common.regex.Regex;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.index.IndexSettings;
import org.codelibs.fesen.index.analysis.AbstractTokenizerFactory;

import java.util.regex.Pattern;

public class PatternTokenizerFactory extends AbstractTokenizerFactory {

    private final Pattern pattern;
    private final int group;

    PatternTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, settings, name);

        String sPattern = settings.get("pattern", "\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/);
        if (sPattern == null) {
            throw new IllegalArgumentException("pattern is missing for [" + name + "] tokenizer of type 'pattern'");
        }

        this.pattern = Regex.compile(sPattern, settings.get("flags"));
        this.group = settings.getAsInt("group", -1);
    }

    @Override
    public Tokenizer create() {
        return new PatternTokenizer(pattern, group);
    }
}
