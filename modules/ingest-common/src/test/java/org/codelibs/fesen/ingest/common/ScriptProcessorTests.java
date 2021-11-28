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

package org.codelibs.fesen.ingest.common;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.ingest.IngestDocument;
import org.codelibs.fesen.ingest.RandomDocumentPicks;
import org.codelibs.fesen.script.IngestScript;
import org.codelibs.fesen.script.MockScriptEngine;
import org.codelibs.fesen.script.Script;
import org.codelibs.fesen.script.ScriptModule;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.script.ScriptType;
import org.codelibs.fesen.test.ESTestCase;
import org.junit.Before;

public class ScriptProcessorTests extends ESTestCase {

    private ScriptService scriptService;
    private Script script;
    private IngestScript ingestScript;

    @Before
    public void setupScripting() {
        String scriptName = "script";
        scriptService = new ScriptService(Settings.builder().build(), Collections.singletonMap(Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    Integer bytesIn = (Integer) ctx.get("bytes_in");
                    Integer bytesOut = (Integer) ctx.get("bytes_out");
                    ctx.put("bytes_total", bytesIn + bytesOut);
                    return null;
                }), Collections.emptyMap())), new HashMap<>(ScriptModule.CORE_CONTEXTS));
        script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        ingestScript = scriptService.compile(script, IngestScript.CONTEXT).newInstance(script.getParams());
    }

    public void testScriptingWithoutPrecompiledScriptFactory() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    public void testScriptingWithPrecompiledIngestScript() {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, ingestScript, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    private IngestDocument randomDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", randomInt());
        document.put("bytes_out", randomInt());
        return RandomDocumentPicks.randomIngestDocument(random(), document);
    }

    private void assertIngestDocument(IngestDocument ingestDocument) {
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        int bytesTotal = ingestDocument.getFieldValue("bytes_in", Integer.class) + ingestDocument.getFieldValue("bytes_out", Integer.class);
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(bytesTotal));
    }

    public void testTypeDeprecation() throws Exception {
        String scriptName = "script";
        ScriptService scriptService = new ScriptService(Settings.builder().build(), Collections.singletonMap(Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    ctx.get("_type");
                    return null;
                }), Collections.emptyMap())), new HashMap<>(ScriptModule.CORE_CONTEXTS));
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        processor.execute(ingestDocument);
        assertWarnings("[types removal] Looking up doc types [_type] in scripts is deprecated.");
    }
}
