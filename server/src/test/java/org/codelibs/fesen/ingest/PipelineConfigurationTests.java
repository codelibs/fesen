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

package org.codelibs.fesen.ingest;

import org.codelibs.fesen.common.bytes.BytesArray;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.xcontent.ContextParser;
import org.codelibs.fesen.common.xcontent.DeprecationHandler;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.ingest.PipelineConfiguration;
import org.codelibs.fesen.test.AbstractXContentTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;

public class PipelineConfigurationTests extends AbstractXContentTestCase<PipelineConfiguration> {

    public void testSerialization() throws IOException {
        PipelineConfiguration configuration =
                new PipelineConfiguration("1", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, configuration.getXContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        configuration.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        PipelineConfiguration serialized = PipelineConfiguration.readFrom(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getConfig().utf8ToString());
    }

    public void testParser() throws IOException {
        ContextParser<Void, PipelineConfiguration> parser = PipelineConfiguration.getParser();
        XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference bytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            new PipelineConfiguration("1", new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON).toXContent(builder,
                    ToXContent.EMPTY_PARAMS);
            bytes = BytesReference.bytes(builder);
        }

        XContentParser xContentParser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput());
        PipelineConfiguration parsed = parser.parse(xContentParser, null);
        assertEquals(xContentType, parsed.getXContentType());
        assertEquals("{}", XContentHelper.convertToJson(parsed.getConfig(), false, parsed.getXContentType()));
        assertEquals("1", parsed.getId());
    }

    @Override
    protected PipelineConfiguration createTestInstance() {
        BytesArray config;
        if (randomBoolean()) {
            config = new BytesArray("{}".getBytes(StandardCharsets.UTF_8));
        } else {
            config = new BytesArray("{\"foo\": \"bar\"}".getBytes(StandardCharsets.UTF_8));
        }
        return new PipelineConfiguration(randomAlphaOfLength(4), config, XContentType.JSON);
    }

    @Override
    protected PipelineConfiguration doParseInstance(XContentParser parser) throws IOException {
        return PipelineConfiguration.getParser().parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("config");
    }
}
