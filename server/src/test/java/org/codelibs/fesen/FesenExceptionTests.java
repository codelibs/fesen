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

package org.codelibs.fesen;

import org.apache.lucene.util.Constants;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.ResourceAlreadyExistsException;
import org.codelibs.fesen.ResourceNotFoundException;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.NoShardAvailableActionException;
import org.codelibs.fesen.action.OriginalIndices;
import org.codelibs.fesen.action.RoutingMissingException;
import org.codelibs.fesen.action.search.SearchPhaseExecutionException;
import org.codelibs.fesen.action.search.ShardSearchFailure;
import org.codelibs.fesen.action.support.broadcast.BroadcastShardOperationFailedException;
import org.codelibs.fesen.client.transport.NoNodeAvailableException;
import org.codelibs.fesen.cluster.block.ClusterBlockException;
import org.codelibs.fesen.cluster.coordination.NoMasterBlockService;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.ParsingException;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.UUIDs;
import org.codelibs.fesen.common.bytes.BytesArray;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.ToXContent;
import org.codelibs.fesen.common.xcontent.XContent;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentLocation;
import org.codelibs.fesen.common.xcontent.XContentParseException;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.core.Tuple;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.index.IndexNotFoundException;
import org.codelibs.fesen.index.query.QueryShardException;
import org.codelibs.fesen.index.shard.IndexShardRecoveringException;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.node.NodeClosedException;
import org.codelibs.fesen.repositories.RepositoryException;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.script.ScriptException;
import org.codelibs.fesen.search.SearchContextMissingException;
import org.codelibs.fesen.search.SearchParseException;
import org.codelibs.fesen.search.SearchShardTarget;
import org.codelibs.fesen.search.internal.ShardSearchContextId;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.transport.RemoteTransportException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.codelibs.fesen.test.TestSearchContext.SHARD_TARGET;
import static org.codelibs.fesen.test.hamcrest.FesenAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class FesenExceptionTests extends ESTestCase {

    public void testStatus() {
        FesenException exception = new FesenException("test");
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new FesenException("test", new RuntimeException());
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new FesenException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new RemoteTransportException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));

        exception = new RemoteTransportException("test", new ResourceAlreadyExistsException("test"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalArgumentException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalStateException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testGuessRootCause() {
        {
            FesenException exception = new FesenException("foo", new FesenException("bar",
                    new IndexNotFoundException("foo", new RuntimeException("foobar"))));
            FesenException[] rootCauses = exception.guessRootCauses();
            assertEquals(rootCauses.length, 1);
            assertEquals(FesenException.getExceptionName(rootCauses[0]), "index_not_found_exception");
            assertEquals("no such index [foo]", rootCauses[0].getMessage());
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1});
            if (randomBoolean()) {
                rootCauses = (randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex).guessRootCauses();
            } else {
                rootCauses = FesenException.guessRootCauses(randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex);
            }
            assertEquals("parsing_exception", FesenException.getExceptionName(rootCauses[0]));
            assertEquals("foobar", rootCauses[0].getMessage());

            FesenException oneLevel = new FesenException("foo", new RuntimeException("foobar"));
            rootCauses = oneLevel.guessRootCauses();
            assertEquals("exception", FesenException.getExceptionName(rootCauses[0]));
            assertEquals("foo", rootCauses[0].getMessage());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(
                    new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo1", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo1", "_na_", 2), null, OriginalIndices.NONE));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1, failure2});
            final FesenException[] rootCauses = ex.guessRootCauses();
            assertEquals(rootCauses.length, 2);
            assertEquals(FesenException.getExceptionName(rootCauses[0]), "parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");
            assertEquals(1, ((ParsingException) rootCauses[0]).getLineNumber());
            assertEquals(2, ((ParsingException) rootCauses[0]).getColumnNumber());
            assertEquals("query_shard_exception", FesenException.getExceptionName(rootCauses[1]));
            assertEquals("foo1", rootCauses[1].getIndex().getName());
            assertEquals("foobar", rootCauses[1].getMessage());
        }

        {
            final FesenException[] foobars = FesenException.guessRootCauses(new IllegalArgumentException("foobar"));
            assertEquals(foobars.length, 1);
            assertThat(foobars[0], instanceOf(FesenException.class));
            assertEquals("foobar", foobars[0].getMessage());
            assertEquals(IllegalArgumentException.class, foobars[0].getCause().getClass());
            assertEquals("illegal_argument_exception", foobars[0].getExceptionName());
        }

        {
            final FesenException[] foobars = FesenException.guessRootCauses(
                new RemoteTransportException("abc", new IllegalArgumentException("foobar")));
            assertEquals(foobars.length, 1);
            assertThat(foobars[0], instanceOf(FesenException.class));
            assertEquals("foobar", foobars[0].getMessage());
            assertEquals(IllegalArgumentException.class, foobars[0].getCause().getClass());
            assertEquals("illegal_argument_exception", foobars[0].getExceptionName());
        }

        {
            XContentParseException inner = new XContentParseException(null, "inner");
            XContentParseException outer = new XContentParseException(null, "outer", inner);
            final FesenException[] causes = FesenException.guessRootCauses(outer);
            assertEquals(causes.length, 1);
            assertThat(causes[0], instanceOf(FesenException.class));
            assertEquals("inner", causes[0].getMessage());
            assertEquals("x_content_parse_exception", causes[0].getExceptionName());
        }

        {
            FesenException inner = new FesenException("inner");
            XContentParseException outer = new XContentParseException(null, "outer", inner);
            final FesenException[] causes = FesenException.guessRootCauses(outer);
            assertEquals(causes.length, 1);
            assertThat(causes[0], instanceOf(FesenException.class));
            assertEquals("inner", causes[0].getMessage());
            assertEquals("exception", causes[0].getExceptionName());
        }
    }

    public void testDeduplicate() throws IOException {
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    randomBoolean() ? failure1.getCause() : failure.getCause(), new ShardSearchFailure[]{failure, failure1});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\"," +
                    "\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":" +
                    "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]}";
            assertEquals(expected, Strings.toString(builder));
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo1", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo1", "_na_", 2), null, OriginalIndices.NONE));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1, failure2});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\"," +
                    "\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\"," +
                    "\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}},{\"shard\":1," +
                    "\"index\":\"foo1\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_shard_exception\",\"reason\":\"foobar\"," +
                    "\"index_uuid\":\"_na_\",\"index\":\"foo1\"}}]}";
            assertEquals(expected, Strings.toString(builder));
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE));
            NullPointerException nullPointerException = new NullPointerException();
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", nullPointerException,
                    new ShardSearchFailure[]{failure, failure1});
            assertEquals(nullPointerException, ex.getCause());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\"," +
                    "\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\"," +
                    "\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]," +
                    "\"caused_by\":{\"type\":\"null_pointer_exception\",\"reason\":null}}";
            assertEquals(expected, Strings.toString(builder));
        }
    }

    /**
     * Check whether this exception contains an exception of the given type:
     * either it is of the given class itself or it contains a nested cause
     * of the given type.
     *
     * @param exType the exception type to look for
     * @return whether there is a nested exception of the specified type
     */
    private static boolean contains(Throwable t, Class<? extends Throwable> exType) {
        if (exType == null) {
            return false;
        }
        for (Throwable cause = t; t != null; t = t.getCause()) {
            if (exType.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    public void testGetRootCause() {
        Exception root = new RuntimeException("foobar");
        FesenException exception = new FesenException("foo", new FesenException("bar",
                new IllegalArgumentException("index is closed", root)));
        assertEquals(root, exception.getRootCause());
        assertTrue(contains(exception, RuntimeException.class));
        assertFalse(contains(exception, EOFException.class));
    }

    public void testToString() {
        FesenException exception = new FesenException("foo", new FesenException("bar",
                new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
        assertEquals("FesenException[foo]; nested: FesenException[bar]; nested: IllegalArgumentException" +
                "[index is closed]; nested: RuntimeException[foobar];", exception.toString());
    }

    public void testToXContent() throws IOException {
        {
            FesenException e = new FesenException("test");
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"test\"}");
        }
        {
            FesenException e = new IndexShardRecoveringException(new ShardId("_test", "_0", 5));
            assertExceptionAsJson(e, "{\"type\":\"index_shard_recovering_exception\"," +
                    "\"reason\":\"CurrentState[RECOVERING] Already recovering\",\"index_uuid\":\"_0\"," +
                    "\"shard\":\"5\",\"index\":\"_test\"}");
        }
        {
            FesenException e = new BroadcastShardOperationFailedException(new ShardId("_index", "_uuid", 12), "foo",
                    new IllegalStateException("bar"));
            assertExceptionAsJson(e, "{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}");
        }
        {
            FesenException e = new FesenException(new IllegalArgumentException("foo"));
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"java.lang.IllegalArgumentException: foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"foo\"}}");
        }
        {
            FesenException e = new SearchParseException(SHARD_TARGET, "foo", new XContentLocation(1,0));
            assertExceptionAsJson(e, "{\"type\":\"search_parse_exception\",\"reason\":\"foo\",\"line\":1,\"col\":0}");
        }
        {
            FesenException ex = new FesenException("foo",
                    new FesenException("bar", new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
            assertExceptionAsJson(ex, "{\"type\":\"exception\",\"reason\":\"foo\",\"caused_by\":{\"type\":\"exception\"," +
                    "\"reason\":\"bar\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"index is closed\"," +
                    "\"caused_by\":{\"type\":\"runtime_exception\",\"reason\":\"foobar\"}}}}");
        }
        {
            FesenException e = new FesenException("foo", new IllegalStateException("bar"));
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}}");

            // Test the same exception but with the "rest.exception.stacktrace.skip" parameter disabled: the stack_trace must be present
            // in the JSON. Since the stack can be large, it only checks the beginning of the JSON.
            ToXContent.Params params = new ToXContent.MapParams(
                    Collections.singletonMap(FesenException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
            String actual;
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.startObject();
                e.toXContent(builder, params);
                builder.endObject();
                actual = Strings.toString(builder);
            }
            assertThat(actual, startsWith("{\"type\":\"exception\",\"reason\":\"foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"," +
                    "\"stack_trace\":\"java.lang.IllegalStateException: bar" +
                    (Constants.WINDOWS ? "\\r\\n" : "\\n") +
                    "\\tat org.codelibs.fesen."));
        }
    }

    public void testGenerateThrowableToXContent() throws IOException {
        {
            Exception ex;
            if (randomBoolean()) {
                // just a wrapper which is omitted
                ex = new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            } else {
                ex = new FileNotFoundException("foo not found");
            }
            assertExceptionAsJson(ex, "{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}");
        }
        {
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            assertExceptionAsJson(ex, "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}");
        }

        { // test equivalence
            FesenException ex =  new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            String toXContentString = Strings.toString(ex);
            String throwableString = Strings.toString((builder, params) -> {
                FesenException.generateThrowableXContent(builder, params, ex);
                return builder;
            });

            assertEquals(throwableString, toXContentString);
            assertEquals("{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}", toXContentString);
        }

        { // render header and metadata
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            ex.addMetadata("es.test1", "value1");
            ex.addMetadata("es.test2", "value2");
            ex.addHeader("test", "some value");
            ex.addHeader("test_multi", "some value", "another value");
            String expected = "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2," +
                    "\"test1\":\"value1\",\"test2\":\"value2\"," +
                    "\"header\":{\"test_multi\":" +
                    "[\"some value\",\"another value\"],\"test\":\"some value\"}}";
            assertExceptionAsJson(ex, expected);
        }
    }

    public void testToXContentWithHeadersAndMetadata() throws IOException {
        FesenException e = new FesenException("foo",
            new FesenException("bar",
                new FesenException("baz",
                    new ClusterBlockException(singleton(NoMasterBlockService.NO_MASTER_BLOCK_WRITES)))));
        e.addHeader("foo_0", "0");
        e.addHeader("foo_1", "1");
        e.addMetadata("es.metadata_foo_0", "foo_0");
        e.addMetadata("es.metadata_foo_1", "foo_1");

        final String expectedJson = "{"
            + "\"type\":\"exception\","
            + "\"reason\":\"foo\","
            + "\"metadata_foo_0\":\"foo_0\","
            + "\"metadata_foo_1\":\"foo_1\","
            + "\"caused_by\":{"
                + "\"type\":\"exception\","
                + "\"reason\":\"bar\","
                + "\"caused_by\":{"
                    + "\"type\":\"exception\","
                    + "\"reason\":\"baz\","
                    + "\"caused_by\":{"
                        + "\"type\":\"cluster_block_exception\","
                        + "\"reason\":\"blocked by: [SERVICE_UNAVAILABLE/2/no master];\""
                    + "}"
                + "}"
            + "},"
            + "\"header\":{"
                    + "\"foo_0\":\"0\","
                    + "\"foo_1\":\"1\""
                + "}"
        + "}";

        assertExceptionAsJson(e, expectedJson);

        FesenException parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), expectedJson)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Fesen exception [type=exception, reason=foo]");
        assertThat(parsed.getHeaderKeys(), hasSize(2));
        assertEquals(parsed.getHeader("foo_0").get(0), "0");
        assertEquals(parsed.getHeader("foo_1").get(0), "1");
        assertThat(parsed.getMetadataKeys(), hasSize(2));
        assertEquals(parsed.getMetadata("es.metadata_foo_0").get(0), "foo_0");
        assertEquals(parsed.getMetadata("es.metadata_foo_1").get(0), "foo_1");

        FesenException cause = (FesenException) parsed.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=bar]");

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=baz]");

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Fesen exception [type=cluster_block_exception, reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
    }

    public void testFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent)
                                                    .startObject()
                                                        .field("type", "foo")
                                                        .field("reason", "something went wrong")
                                                        .field("stack_trace", "...")
                                                    .endObject();

        builder = shuffleXContent(builder);
        FesenException parsed;
        try (XContentParser parser = createParser(xContent, BytesReference.bytes(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Fesen exception [type=foo, reason=something went wrong, stack_trace=...]");
    }

    public void testFromXContentWithCause() throws IOException {
        FesenException e = new FesenException("foo",
                new FesenException("bar",
                        new FesenException("baz",
                                new RoutingMissingException("_test", "_type", "_id"))));

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent).startObject().value(e).endObject();
        builder = shuffleXContent(builder);

        FesenException parsed;
        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Fesen exception [type=exception, reason=foo]");

        FesenException cause = (FesenException) parsed.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=bar]");

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=baz]");

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Fesen exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]");
        assertThat(cause.getHeaderKeys(), hasSize(0));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.index"), hasItem("_test"));
        assertThat(cause.getMetadata("es.index_uuid"), hasItem("_na_"));
    }

    public void testFromXContentWithHeadersAndMetadata() throws IOException {
        RoutingMissingException routing = new RoutingMissingException("_test", "_type", "_id");
        FesenException baz = new FesenException("baz", routing);
        baz.addHeader("baz_0", "baz0");
        baz.addMetadata("es.baz_1", "baz1");
        baz.addHeader("baz_2", "baz2");
        baz.addMetadata("es.baz_3", "baz3");
        FesenException bar = new FesenException("bar", baz);
        bar.addMetadata("es.bar_0", "bar0");
        bar.addHeader("bar_1", "bar1");
        bar.addMetadata("es.bar_2", "bar2");
        FesenException foo = new FesenException("foo", bar);
        foo.addMetadata("es.foo_0", "foo0");
        foo.addHeader("foo_1", "foo1");

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent).startObject().value(foo).endObject();
        builder = shuffleXContent(builder);

        FesenException parsed;
        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Fesen exception [type=exception, reason=foo]");
        assertThat(parsed.getHeaderKeys(), hasSize(1));
        assertThat(parsed.getHeader("foo_1"), hasItem("foo1"));
        assertThat(parsed.getMetadataKeys(), hasSize(1));
        assertThat(parsed.getMetadata("es.foo_0"), hasItem("foo0"));

        FesenException cause = (FesenException) parsed.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=bar]");
        assertThat(cause.getHeaderKeys(), hasSize(1));
        assertThat(cause.getHeader("bar_1"), hasItem("bar1"));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.bar_0"), hasItem("bar0"));
        assertThat(cause.getMetadata("es.bar_2"), hasItem("bar2"));

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(), "Fesen exception [type=exception, reason=baz]");
        assertThat(cause.getHeaderKeys(), hasSize(2));
        assertThat(cause.getHeader("baz_0"), hasItem("baz0"));
        assertThat(cause.getHeader("baz_2"), hasItem("baz2"));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.baz_1"), hasItem("baz1"));
        assertThat(cause.getMetadata("es.baz_3"), hasItem("baz3"));

        cause = (FesenException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Fesen exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]");
        assertThat(cause.getHeaderKeys(), hasSize(0));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.index"), hasItem("_test"));
        assertThat(cause.getMetadata("es.index_uuid"), hasItem("_na_"));
    }

    /**
     * Test that some values like arrays of numbers are ignored when parsing back
     * an exception.
     */
    public void testFromXContentWithIgnoredMetadataAndHeaders() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        // The exception content to parse is built using a XContentBuilder
        // because the current Java API does not allow to add metadata/headers
        // of other types than list of strings.
        BytesReference originalBytes;
        try (XContentBuilder builder = XContentBuilder.builder(xContent)) {
            builder.startObject()
                    .field("metadata_int", 1)
                    .array("metadata_array_of_ints", new int[]{8, 13, 21})
                    .field("reason", "Custom reason")
                    .array("metadata_array_of_boolean", new boolean[]{false, false})
                    .startArray("metadata_array_of_objects")
                        .startObject()
                            .field("object_array_one", "value_one")
                        .endObject()
                        .startObject()
                            .field("object_array_two", "value_two")
                        .endObject()
                    .endArray()
                    .field("type", "custom_exception")
                    .field("metadata_long", 1L)
                    .array("metadata_array_of_longs", new long[]{2L, 3L, 5L})
                    .field("metadata_other", "some metadata")
                    .startObject("header")
                        .field("header_string", "some header")
                        .array("header_array_of_strings", new String[]{"foo", "bar", "baz"})
                    .endObject()
                    .startObject("metadata_object")
                        .field("object_field", "value")
                    .endObject()
            .endObject();
            try (XContentBuilder shuffledBuilder = shuffleXContent(builder)) {
                originalBytes = BytesReference.bytes(shuffledBuilder);
            }
        }

        FesenException parsedException;
        try (XContentParser parser = createParser(xContent, originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsedException);
        assertEquals("Fesen exception [type=custom_exception, reason=Custom reason]", parsedException.getMessage());
        assertEquals(2, parsedException.getHeaderKeys().size());
        assertThat(parsedException.getHeader("header_string"), hasItem("some header"));
        assertThat(parsedException.getHeader("header_array_of_strings"), hasItems("foo", "bar", "baz"));
        assertEquals(1, parsedException.getMetadataKeys().size());
        assertThat(parsedException.getMetadata("es.metadata_other"), hasItem("some metadata"));
    }

    public void testThrowableToAndFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        final Tuple<Throwable, FesenException> exceptions = randomExceptions();
        final Throwable throwable = exceptions.v1();
        final FesenException expected = exceptions.v2();
        int suppressedCount = randomBoolean() ? 0 : between(1, 5);
        for (int i = 0; i < suppressedCount; i++) {
            final Tuple<Throwable, FesenException> suppressed = randomExceptions();
            throwable.addSuppressed(suppressed.v1());
            expected.addSuppressed(suppressed.v2());
        }

        BytesReference throwableBytes = toShuffledXContent((builder, params) -> {
            FesenException.generateThrowableXContent(builder, params, throwable);
            return builder;
        }, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        FesenException parsedException;
        try (XContentParser parser = createParser(xContent, throwableBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = FesenException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertDeepEquals(expected, parsedException);

        if (suppressedCount > 0) {
            XContentBuilder builder = XContentBuilder.builder(xContent);
            builder.startObject();
            FesenException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, throwable);
            builder.endObject();
            throwableBytes = BytesReference.bytes(builder);
            try (XContentParser parser = createParser(xContent, throwableBytes)) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                List<String> keys = new ArrayList<>(parser.mapOrdered().keySet());
                assertEquals("last index should be [suppressed]", "suppressed", keys.get(keys.size() - 1));
            }
        }
    }

    public void testUnknownFailureToAndFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        BytesReference failureBytes = toShuffledXContent((builder, params) -> {
            // Prints a null failure using generateFailureXContent()
            FesenException.generateFailureXContent(builder, params, null, randomBoolean());
            return builder;
        }, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        FesenException parsedFailure;
        try (XContentParser parser = createParser(xContent, failureBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedFailure = FesenException.failureFromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }

        // Failure was null, expecting a "unknown" reason
        assertEquals("Fesen exception [type=exception, reason=unknown]", parsedFailure.getMessage());
        assertEquals(0, parsedFailure.getHeaders().size());
        assertEquals(0, parsedFailure.getMetadata().size());
    }

    public void testFailureToAndFromXContentWithNoDetails() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        final Exception failure = (Exception) randomExceptions().v1();
        BytesReference failureBytes = toShuffledXContent((builder, params) -> {
            FesenException.generateFailureXContent(builder, params, failure, false);
            return builder;
        }, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        try (XContentParser parser = createParser(xContent, failureBytes)) {
            failureBytes = BytesReference.bytes(shuffleXContent(parser, randomBoolean()));
        }

        FesenException parsedFailure;
        try (XContentParser parser = createParser(xContent, failureBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedFailure = FesenException.failureFromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertNotNull(parsedFailure);

        String reason;
        if (failure instanceof FesenException) {
            reason = failure.getClass().getSimpleName() + "[" + failure.getMessage() + "]";
        } else {
            reason = "No FesenException found";
        }
        assertEquals(FesenException.buildMessage("exception", reason, null), parsedFailure.getMessage());
        assertEquals(0, parsedFailure.getHeaders().size());
        assertEquals(0, parsedFailure.getMetadata().size());
        assertNull(parsedFailure.getCause());
    }

    public void testFailureToAndFromXContentWithDetails() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        Exception failure;
        Throwable failureCause;
        FesenException expected;
        FesenException expectedCause;
        FesenException suppressed;

        switch (randomIntBetween(0, 6)) {
            case 0: // Simple fesen exception without cause
                failure = new NoNodeAvailableException("A");

                expected = new FesenException("Fesen exception [type=no_node_available_exception, reason=A]");
                expected.addSuppressed(new FesenException("Fesen exception [type=no_node_available_exception, reason=A]"));
                break;

            case 1: // Simple fesen exception with headers (other metadata of type number are not parsed)
                failure = new ParsingException(3, 2, "B", null);
                ((FesenException) failure).addHeader("header_name", "0", "1");
                expected = new FesenException("Fesen exception [type=parsing_exception, reason=B]");
                expected.addHeader("header_name", "0", "1");
                suppressed = new FesenException("Fesen exception [type=parsing_exception, reason=B]");
                suppressed.addHeader("header_name", "0", "1");
                expected.addSuppressed(suppressed);
                break;

            case 2: // Fesen exception with a cause, headers and parsable metadata
                failureCause = new NullPointerException("var is null");
                failure = new ScriptException("C", failureCause, singletonList("stack"), "test", "painless");
                ((FesenException) failure).addHeader("script_name", "my_script");

                expectedCause = new FesenException("Fesen exception [type=null_pointer_exception, reason=var is null]");
                expected = new FesenException("Fesen exception [type=script_exception, reason=C]", expectedCause);
                expected.addHeader("script_name", "my_script");
                expected.addMetadata("es.lang", "painless");
                expected.addMetadata("es.script", "test");
                expected.addMetadata("es.script_stack", "stack");
                suppressed = new FesenException("Fesen exception [type=script_exception, reason=C]");
                suppressed.addHeader("script_name", "my_script");
                suppressed.addMetadata("es.lang", "painless");
                suppressed.addMetadata("es.script", "test");
                suppressed.addMetadata("es.script_stack", "stack");
                expected.addSuppressed(suppressed);
                break;

            case 3: // JDK exception without cause
                failure = new IllegalStateException("D");

                expected = new FesenException("Fesen exception [type=illegal_state_exception, reason=D]");
                suppressed = new FesenException("Fesen exception [type=illegal_state_exception, reason=D]");
                expected.addSuppressed(suppressed);
                break;

            case 4: // JDK exception with cause
                failureCause = new RoutingMissingException("idx", "type", "id");
                failure = new RuntimeException("E", failureCause);

                expectedCause = new FesenException("Fesen exception [type=routing_missing_exception, " +
                        "reason=routing is required for [idx]/[type]/[id]]");
                expectedCause.addMetadata("es.index", "idx");
                expectedCause.addMetadata("es.index_uuid", "_na_");
                expected = new FesenException("Fesen exception [type=runtime_exception, reason=E]", expectedCause);
                suppressed = new FesenException("Fesen exception [type=runtime_exception, reason=E]");
                expected.addSuppressed(suppressed);
                break;

            case 5: // Wrapped exception with cause
                failureCause = new FileAlreadyExistsException("File exists");
                failure = new BroadcastShardOperationFailedException(new ShardId("_index", "_uuid", 5), "F", failureCause);

                expected = new FesenException("Fesen exception [type=file_already_exists_exception, reason=File exists]");
                suppressed = new FesenException("Fesen exception [type=file_already_exists_exception, reason=File exists]");
                expected.addSuppressed(suppressed);
                break;

            case 6: // SearchPhaseExecutionException with cause and multiple failures
                DiscoveryNode node = new DiscoveryNode("node_g", buildNewFakeTransportAddress(), Version.CURRENT);
                failureCause = new NodeClosedException(node);
                failureCause = new NoShardAvailableActionException(new ShardId("_index_g", "_uuid_g", 6), "node_g", failureCause);
                ShardSearchFailure[] shardFailures = new ShardSearchFailure[]{
                        new ShardSearchFailure(new ParsingException(0, 0, "Parsing g", null),
                                new SearchShardTarget("node_g", new ShardId(new Index("_index_g", "_uuid_g"), 61), null,
                                    OriginalIndices.NONE)), new ShardSearchFailure(new RepositoryException("repository_g", "Repo"),
                                new SearchShardTarget("node_g", new ShardId(new Index("_index_g", "_uuid_g"), 62), null,
                                    OriginalIndices.NONE)), new ShardSearchFailure(
                                        new SearchContextMissingException(new ShardSearchContextId(UUIDs.randomBase64UUID(), 0L)), null)
                };
                failure = new SearchPhaseExecutionException("phase_g", "G", failureCause, shardFailures);

                expectedCause = new FesenException("Fesen exception [type=node_closed_exception, " +
                        "reason=node closed " + node + "]");
                expectedCause = new FesenException("Fesen exception [type=no_shard_available_action_exception, " +
                        "reason=node_g]", expectedCause);
                expectedCause.addMetadata("es.index", "_index_g");
                expectedCause.addMetadata("es.index_uuid", "_uuid_g");
                expectedCause.addMetadata("es.shard", "6");

                expected = new FesenException("Fesen exception [type=search_phase_execution_exception, " +
                        "reason=G]", expectedCause);
                expected.addMetadata("es.phase", "phase_g");

                expected.addSuppressed(new FesenException("Fesen exception [type=parsing_exception, reason=Parsing g]"));
                expected.addSuppressed(new FesenException("Fesen exception [type=repository_exception, " +
                        "reason=[repository_g] Repo]"));
                expected.addSuppressed(new FesenException("Fesen exception [type=search_context_missing_exception, " +
                        "reason=No search context found for id [0]]"));
                break;
            default:
                throw new UnsupportedOperationException("Failed to generate randomized failure");
        }

        Exception finalFailure = failure;
        BytesReference failureBytes = toShuffledXContent((builder, params) -> {
            FesenException.generateFailureXContent(builder, params, finalFailure, true);
            return builder;
        }, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        try (XContentParser parser = createParser(xContent, failureBytes)) {
            failureBytes = BytesReference.bytes(shuffleXContent(parser, randomBoolean()));
        }

        FesenException parsedFailure;
        try (XContentParser parser = createParser(xContent, failureBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedFailure = FesenException.failureFromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }

        assertDeepEquals(expected, parsedFailure);
    }

    /**
     * Builds a {@link ToXContent} using a JSON XContentBuilder and compares the result to the given json in string format.
     *
     * By default, the stack trace of the exception is not rendered. The parameter `errorTrace` forces the stack trace to
     * be rendered like the REST API does when the "error_trace" parameter is set to true.
     */
    private static void assertToXContentAsJson(ToXContent e, String expectedJson) throws IOException {
        BytesReference actual = XContentHelper.toXContent(e, XContentType.JSON, randomBoolean());
        assertToXContentEquivalent(new BytesArray(expectedJson), actual, XContentType.JSON);
    }

    private static void assertExceptionAsJson(Exception e, String expectedJson) throws IOException {
        assertToXContentAsJson((builder, params) -> {
            FesenException.generateThrowableXContent(builder, params, e);
            return builder;
        }, expectedJson);
    }

    public static void assertDeepEquals(FesenException expected, FesenException actual) {
        do {
            if (expected == null) {
                assertNull(actual);
            } else {
                assertNotNull(actual);
            }

            assertEquals(expected.getMessage(), actual.getMessage());
            assertEquals(expected.getHeaders(), actual.getHeaders());
            assertEquals(expected.getMetadata(), actual.getMetadata());
            assertEquals(expected.getResourceType(), actual.getResourceType());
            assertEquals(expected.getResourceId(), actual.getResourceId());

            Throwable[] expectedSuppressed = expected.getSuppressed();
            Throwable[] actualSuppressed = actual.getSuppressed();

            if (expectedSuppressed == null) {
                assertNull(actualSuppressed);
            } else {
                assertNotNull(actualSuppressed);
                assertEquals(expectedSuppressed.length, actualSuppressed.length);
                for (int i = 0; i < expectedSuppressed.length; i++) {
                    assertDeepEquals((FesenException) expectedSuppressed[i], (FesenException) actualSuppressed[i]);
                }
            }

            expected = (FesenException) expected.getCause();
            actual = (FesenException) actual.getCause();
            if (expected == null) {
                assertNull(actual);
            }
        } while (expected != null);
    }

    public static Tuple<Throwable, FesenException> randomExceptions() {
        Throwable actual;
        FesenException expected;

        int type = randomIntBetween(0, 5);
        switch (type) {
            case 0:
                actual = new ClusterBlockException(singleton(NoMasterBlockService.NO_MASTER_BLOCK_WRITES));
                expected = new FesenException("Fesen exception [type=cluster_block_exception, " +
                        "reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
                break;
            case 1: // Simple fesen exception with headers (other metadata of type number are not parsed)
                actual = new ParsingException(3, 2, "Unknown identifier", null);
                expected = new FesenException("Fesen exception [type=parsing_exception, reason=Unknown identifier]");
                break;
            case 2:
                actual = new SearchParseException(SHARD_TARGET, "Parse failure", new XContentLocation(12, 98));
                expected = new FesenException("Fesen exception [type=search_parse_exception, reason=Parse failure]");
                break;
            case 3:
                actual = new IllegalArgumentException("Closed resource", new RuntimeException("Resource"));
                expected = new FesenException("Fesen exception [type=illegal_argument_exception, reason=Closed resource]",
                                new FesenException("Fesen exception [type=runtime_exception, reason=Resource]"));
                break;
            case 4:
                actual = new SearchPhaseExecutionException("search", "all shards failed",
                            new ShardSearchFailure[]{
                                    new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                                            new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE))
                            });
                expected = new FesenException("Fesen exception [type=search_phase_execution_exception, " +
                        "reason=all shards failed]");
                expected.addMetadata("es.phase", "search");
                break;
            case 5:
                actual = new FesenException("Parsing failed",
                            new ParsingException(9, 42, "Wrong state",
                                new NullPointerException("Unexpected null value")));

                FesenException expectedCause = new FesenException("Fesen exception [type=parsing_exception, " +
                        "reason=Wrong state]", new FesenException("Fesen exception [type=null_pointer_exception, " +
                        "reason=Unexpected null value]"));
                expected = new FesenException("Fesen exception [type=exception, reason=Parsing failed]", expectedCause);
                break;
            default:
                throw new UnsupportedOperationException("No randomized exceptions generated for type [" + type + "]");
        }

        if (actual instanceof FesenException) {
            FesenException actualException = (FesenException) actual;
            if (randomBoolean()) {
                int nbHeaders = randomIntBetween(1, 5);
                Map<String, List<String>> randomHeaders = new HashMap<>(nbHeaders);

                for (int i = 0; i < nbHeaders; i++) {
                    List<String> values = new ArrayList<>();

                    int nbValues = randomIntBetween(1, 3);
                    for (int j = 0; j < nbValues; j++) {
                        values.add(frequently() ? randomAlphaOfLength(5) : "");
                    }
                    randomHeaders.put("header_" + i, values);
                }

                for (Map.Entry<String, List<String>> entry : randomHeaders.entrySet()) {
                    actualException.addHeader(entry.getKey(), entry.getValue());
                    expected.addHeader(entry.getKey(), entry.getValue());
                }

                if (rarely()) {
                    // Empty or null headers are not printed out by the toXContent method
                    actualException.addHeader("ignored", randomBoolean() ? emptyList() : null);
                }
            }

            if (randomBoolean()) {
                int nbMetadata = randomIntBetween(1, 5);
                Map<String, List<String>> randomMetadata = new HashMap<>(nbMetadata);

                for (int i = 0; i < nbMetadata; i++) {
                    List<String> values = new ArrayList<>();

                    int nbValues = randomIntBetween(1, 3);
                    for (int j = 0; j < nbValues; j++) {
                        values.add(frequently() ? randomAlphaOfLength(5) : "");
                    }
                    randomMetadata.put("es.metadata_" + i, values);
                }

                for (Map.Entry<String, List<String>> entry : randomMetadata.entrySet()) {
                    actualException.addMetadata(entry.getKey(), entry.getValue());
                    expected.addMetadata(entry.getKey(), entry.getValue());
                }

                if (rarely()) {
                    // Empty or null metadata are not printed out by the toXContent method
                    actualException.addMetadata("es.ignored", randomBoolean() ? emptyList() : null);
                }
            }

            if (randomBoolean()) {
                int nbResources = randomIntBetween(1, 5);
                for (int i = 0; i < nbResources; i++) {
                    String resourceType = "type_" + i;
                    String[] resourceIds = new String[randomIntBetween(1, 3)];
                    for (int j = 0; j < resourceIds.length; j++) {
                        resourceIds[j] = frequently() ? randomAlphaOfLength(5) : "";
                    }
                    actualException.setResources(resourceType, resourceIds);
                    expected.setResources(resourceType, resourceIds);
                }
            }
        }
        return new Tuple<>(actual, expected);
    }
}
