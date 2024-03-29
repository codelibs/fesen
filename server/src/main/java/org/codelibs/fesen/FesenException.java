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

import org.codelibs.fesen.action.support.replication.ReplicationOperation;
import org.codelibs.fesen.cluster.action.shard.ShardStateAction;
import org.codelibs.fesen.common.ParseField;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.common.logging.LoggerMessageFormat;
import org.codelibs.fesen.common.xcontent.ToXContentFragment;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParseException;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.core.CheckedFunction;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.core.Tuple;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.search.SearchException;
import org.codelibs.fesen.search.aggregations.MultiBucketConsumerService;
import org.codelibs.fesen.transport.TcpTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.codelibs.fesen.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.codelibs.fesen.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.codelibs.fesen.common.xcontent.XContentParserUtils.ensureFieldName;

/**
 * A base class for all fesen exceptions.
 */
public class FesenException extends RuntimeException implements ToXContentFragment, Writeable {

    private static final Version UNKNOWN_VERSION_ADDED = Version.fromId(0);

    /**
     * Passed in the {@link Params} of {@link #generateThrowableXContent(XContentBuilder, Params, Throwable)}
     * to control if the {@code caused_by} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter.
     */
    private static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";
    /**
     * Passed in the {@link Params} of {@link #generateThrowableXContent(XContentBuilder, Params, Throwable)}
     * to control if the {@code stack_trace} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter. Use the {@code error_trace} parameter instead.
     */
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    private static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;
    private static final String INDEX_METADATA_KEY = "es.index";
    private static final String INDEX_METADATA_KEY_UUID = "es.index_uuid";
    private static final String SHARD_METADATA_KEY = "es.shard";
    private static final String RESOURCE_METADATA_TYPE_KEY = "es.resource.type";
    private static final String RESOURCE_METADATA_ID_KEY = "es.resource.id";

    private static final String TYPE = "type";
    private static final String REASON = "reason";
    private static final String CAUSED_BY = "caused_by";
    private static final ParseField SUPPRESSED = new ParseField("suppressed");
    public static final String STACK_TRACE = "stack_trace";
    private static final String HEADER = "header";
    private static final String ERROR = "error";
    private static final String ROOT_CAUSE = "root_cause";

    private static final Map<Integer, CheckedFunction<StreamInput, ? extends FesenException, IOException>> ID_TO_SUPPLIER;
    private static final Map<Class<? extends FesenException>, FesenExceptionHandle> CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE;
    private final Map<String, List<String>> metadata = new HashMap<>();
    private final Map<String, List<String>> headers = new HashMap<>();

    /**
     * Construct a <code>FesenException</code> with the specified cause exception.
     */
    public FesenException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct a <code>FesenException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg  the detail message
     * @param args the arguments for the message
     */
    public FesenException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
    }

    /**
     * Construct a <code>FesenException</code> with the specified detail message
     * and nested exception.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg   the detail message
     * @param cause the nested exception
     * @param args  the arguments for the message
     */
    public FesenException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }

    public FesenException(StreamInput in) throws IOException {
        super(in.readOptionalString(), in.readException());
        readStackTrace(this, in);
        headers.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
        metadata.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, String... values) {
        addMetadata(key, Arrays.asList(values));
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, List<String> values) {
        //we need to enforce this otherwise bw comp doesn't work properly, as "es." was the previous criteria to split headers in two sets
        if (key.startsWith("es.") == false) {
            throw new IllegalArgumentException("exception metadata must start with [es.], found [" + key + "] instead");
        }
        this.metadata.put(key, values);
    }

    /**
     * Returns a set of all metadata keys on this exception
     */
    public Set<String> getMetadataKeys() {
        return metadata.keySet();
    }

    /**
     * Returns the list of metadata values for the given key or {@code null} if no metadata for the
     * given key exists.
     */
    public List<String> getMetadata(String key) {
        return metadata.get(key);
    }

    protected Map<String, List<String>> getMetadata() {
        return metadata;
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, List<String> value) {
        //we need to enforce this otherwise bw comp doesn't work properly, as "es." was the previous criteria to split headers in two sets
        if (key.startsWith("es.")) {
            throw new IllegalArgumentException("exception headers must not start with [es.], found [" + key + "] instead");
        }
        this.headers.put(key, value);
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, String... value) {
        addHeader(key, Arrays.asList(value));
    }

    /**
     * Returns a set of all header keys on this exception
     */
    public Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    /**
     * Returns the list of header values for the given key or {@code null} if no header for the
     * given key exists.
     */
    public List<String> getHeader(String key) {
        return headers.get(key);
    }

    protected Map<String, List<String>> getHeaders() {
        return headers;
    }

    /**
     * Returns the rest status code associated with this exception.
     */
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }

    /**
     * Unwraps the actual cause from the exception for cases when the exception is a
     * {@link FesenWrapperException}.
     *
     * @see ExceptionsHelper#unwrapCause(Throwable)
     */
    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof FesenException) {
                sb.append(((FesenException) getCause()).getDetailedMessage());
            } else {
                sb.append(getCause());
            }
            return sb.toString();
        } else {
            return super.toString();
        }
    }

    /**
     * Retrieve the innermost cause of this exception, if none, returns the current exception.
     */
    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.getMessage());
        out.writeException(this.getCause());
        writeStackTraces(this, out, StreamOutput::writeException);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMapOfLists(metadata, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static FesenException readException(StreamInput input, int id) throws IOException {
        CheckedFunction<StreamInput, ? extends FesenException, IOException> fesenException = ID_TO_SUPPLIER.get(id);
        if (fesenException == null) {
            if (id == 127 && input.getVersion().before(Version.V_7_5_0)) {
                // was SearchContextException
                return new SearchException(input);
            }
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return fesenException.apply(input);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(Class<? extends Throwable> exception, Version version) {
        FesenExceptionHandle fesenExceptionHandle = CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.get(exception);
        if (fesenExceptionHandle != null) {
            return version.onOrAfter(fesenExceptionHandle.versionAdded);
        }
        return false;
    }

    static Set<Class<? extends FesenException>> getRegisteredKeys() { // for testing
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.keySet();
    }

    /**
     * Returns the serialization id the given exception.
     */
    public static int getId(Class<? extends FesenException> exception) {
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.get(exception).id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            generateThrowableXContent(builder, params, this);
        } else {
            innerToXContent(builder, params, this, getExceptionName(), getMessage(), headers, metadata, getCause());
        }
        return builder;
    }

    protected static void innerToXContent(XContentBuilder builder, Params params,
                                          Throwable throwable, String type, String message, Map<String, List<String>> headers,
                                          Map<String, List<String>> metadata, Throwable cause) throws IOException {
        builder.field(TYPE, type);
        builder.field(REASON, message);

        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            headerToXContent(builder, entry.getKey().substring("es.".length()), entry.getValue());
        }

        if (throwable instanceof FesenException) {
            FesenException exception = (FesenException) throwable;
            exception.metadataToXContent(builder, params);
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            if (cause != null) {
                builder.field(CAUSED_BY);
                builder.startObject();
                generateThrowableXContent(builder, params, cause);
                builder.endObject();
            }
        }

        if (headers.isEmpty() == false) {
            builder.startObject(HEADER);
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                headerToXContent(builder, entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
            builder.field(STACK_TRACE, ExceptionsHelper.stackTrace(throwable));
        }

        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            builder.startArray(SUPPRESSED.getPreferredName());
            for (Throwable suppressed : allSuppressed) {
                builder.startObject();
                generateThrowableXContent(builder, params, suppressed);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    private static void headerToXContent(XContentBuilder builder, String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                builder.field(key, values.get(0));
            } else {
                builder.startArray(key);
                for (String value : values) {
                    builder.value(value);
                }
                builder.endArray();
            }
        }
    }

    /**
     * Renders additional per exception information into the XContent
     */
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
    }

    /**
     * Generate a {@link FesenException} from a {@link XContentParser}. This does not
     * return the original exception type (ie NodeClosedException for example) but just wraps
     * the type, the reason and the cause of the exception. It also recursively parses the
     * tree structure of the cause, returning it as a tree structure of {@link FesenException}
     * instances.
     */
    public static FesenException fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerFromXContent(parser, false);
    }

    public static FesenException innerFromXContent(XContentParser parser, boolean parseRootCauses) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String type = null, reason = null, stack = null;
        FesenException cause = null;
        Map<String, List<String>> metadata = new HashMap<>();
        Map<String, List<String>> headers = new HashMap<>();
        List<FesenException> rootCauses = new ArrayList<>();
        List<FesenException> suppressed = new ArrayList<>();

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isValue()) {
                if (TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (REASON.equals(currentFieldName)) {
                    reason = parser.text();
                } else if (STACK_TRACE.equals(currentFieldName)) {
                    stack = parser.text();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    metadata.put(currentFieldName, Collections.singletonList(parser.text()));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CAUSED_BY.equals(currentFieldName)) {
                    cause = fromXContent(parser);
                } else if (HEADER.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            List<String> values = headers.getOrDefault(currentFieldName, new ArrayList<>());
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        values.add(parser.text());
                                    } else {
                                        parser.skipChildren();
                                    }
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                parser.skipChildren();
                            }
                            headers.put(currentFieldName, values);
                        }
                    }
                } else {
                    // Any additional metadata object added by the metadataToXContent method is ignored
                    // and skipped, so that the parser does not fail on unknown fields. The parser only
                    // support metadata key-pairs and metadata arrays of values.
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseRootCauses && ROOT_CAUSE.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rootCauses.add(fromXContent(parser));
                    }
                } else if (SUPPRESSED.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        suppressed.add(fromXContent(parser));
                    }
                } else {
                    // Parse the array and add each item to the corresponding list of metadata.
                    // Arrays of objects are not supported yet and just ignored and skipped.
                    List<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            values.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                    if (values.size() > 0) {
                        if (metadata.containsKey(currentFieldName)) {
                            values.addAll(metadata.get(currentFieldName));
                        }
                        metadata.put(currentFieldName, values);
                    }
                }
            }
        }

        FesenException e = new FesenException(buildMessage(type, reason, stack), cause);
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            //subclasses can print out additional metadata through the metadataToXContent method. Simple key-value pairs will be
            //parsed back and become part of this metadata set, while objects and arrays are not supported when parsing back.
            //Those key-value pairs become part of the metadata set and inherit the "es." prefix as that is currently required
            //by addMetadata. The prefix will get stripped out when printing metadata out so it will be effectively invisible.
            //TODO move subclasses that print out simple metadata to using addMetadata directly and support also numbers and booleans.
            //TODO rename metadataToXContent and have only SearchPhaseExecutionException use it, which prints out complex objects
            e.addMetadata("es." + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            e.addHeader(header.getKey(), header.getValue());
        }

        // Adds root causes as suppressed exception. This way they are not lost
        // after parsing and can be retrieved using getSuppressed() method.
        for (FesenException rootCause : rootCauses) {
            e.addSuppressed(rootCause);
        }
        for (FesenException s : suppressed) {
            e.addSuppressed(s);
        }
        return e;
    }

    /**
     * Static toXContent helper method that renders {@link org.codelibs.fesen.FesenException} or {@link Throwable} instances
     * as XContent, delegating the rendering to {@link #toXContent(XContentBuilder, Params)}
     * or {@link #innerToXContent(XContentBuilder, Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * This method is usually used when the {@link Throwable} is rendered as a part of another XContent object, and its result can
     * be parsed back using the {@link #fromXContent(XContentParser)} method.
     */
    public static void generateThrowableXContent(XContentBuilder builder, Params params, Throwable t) throws IOException {
        t = ExceptionsHelper.unwrapCause(t);

        if (t instanceof FesenException) {
            ((FesenException) t).toXContent(builder, params);
        } else {
            innerToXContent(builder, params, t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
        }
    }

    /**
     * Render any exception as a xcontent, encapsulated within a field or object named "error". The level of details that are rendered
     * depends on the value of the "detailed" parameter: when it's false only a simple message based on the type and message of the
     * exception is rendered. When it's true all detail are provided including guesses root causes, cause and potentially stack
     * trace.
     *
     * This method is usually used when the {@link Exception} is rendered as a full XContent object, and its output can be parsed
     * by the {@link #failureFromXContent(XContentParser)} method.
     */
    public static void generateFailureXContent(XContentBuilder builder, Params params, @Nullable Exception e, boolean detailed)
            throws IOException {
        // No exception to render as an error
        if (e == null) {
            builder.field(ERROR, "unknown");
            return;
        }

        // Render the exception with a simple message
        if (detailed == false) {
            String message = "No FesenException found";
            Throwable t = e;
            for (int counter = 0; counter < 10 && t != null; counter++) {
                if (t instanceof FesenException) {
                    message = t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
                    break;
                }
                t = t.getCause();
            }
            builder.field(ERROR, message);
            return;
        }

        // Render the exception with all details
        final FesenException[] rootCauses = FesenException.guessRootCauses(e);
        builder.startObject(ERROR);
        {
            builder.startArray(ROOT_CAUSE);
            for (FesenException rootCause : rootCauses) {
                builder.startObject();
                rootCause.toXContent(builder, new DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_CAUSE, "true"), params));
                builder.endObject();
            }
            builder.endArray();
        }
        generateThrowableXContent(builder, params, e);
        builder.endObject();
    }

    /**
     * Parses the output of {@link #generateFailureXContent(XContentBuilder, Params, Exception, boolean)}
     */
    public static FesenException failureFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureFieldName(parser, token, ERROR);

        token = parser.nextToken();
        if (token.isValue()) {
            return new FesenException(buildMessage("exception", parser.text(), null));
        }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        // Root causes are parsed in the innerFromXContent() and are added as suppressed exceptions.
        return innerFromXContent(parser, true);
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions
     */
    public FesenException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof FesenException) {
            return ((FesenException) cause).guessRootCauses();
        }
        return new FesenException[]{this};
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link org.codelibs.fesen.FesenException} an empty array
     * is returned.
     */
    public static FesenException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof FesenException) {
            // FesenException knows how to guess its own root cause
            return ((FesenException) ex).guessRootCauses();
        }
        if (ex instanceof XContentParseException) {
            /*
             * We'd like to unwrap parsing exceptions to the inner-most
             * parsing exception because that is generally the most interesting
             * exception to return to the user. If that exception is caused by
             * an FesenException we'd like to keep unwrapping because
             * ElasticserachExceptions tend to contain useful information for
             * the user.
             */
            Throwable cause = ex.getCause();
            if (cause != null) {
                if (cause instanceof XContentParseException || cause instanceof FesenException) {
                    return guessRootCauses(ex.getCause());
                }
            }
        }
        return new FesenException[]{new FesenException(ex.getMessage(), ex) {
            @Override
            protected String getExceptionName() {
                return getExceptionName(getCause());
            }
        }};
    }

    protected String getExceptionName() {
        return getExceptionName(this);
    }

    /**
     * Returns a underscore case name for the given exception. This method strips {@code Fesen} prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("Fesen")) {
            simpleName = simpleName.substring("Fesen".length());
        }
        // TODO: do we really need to make the exception name in underscore casing?
        return toUnderscoreCase(simpleName);
    }

    static String buildMessage(String type, String reason, String stack) {
        StringBuilder message = new StringBuilder("Fesen exception [");
        message.append(TYPE).append('=').append(type).append(", ");
        message.append(REASON).append('=').append(reason);
        if (stack != null) {
            message.append(", ").append(STACK_TRACE).append('=').append(stack);
        }
        message.append(']');
        return message.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (metadata.containsKey(INDEX_METADATA_KEY)) {
            builder.append(getIndex());
            if (metadata.containsKey(SHARD_METADATA_KEY)) {
                builder.append('[').append(getShardId()).append(']');
            }
            builder.append(' ');
        }
        return builder.append(ExceptionsHelper.detailedMessage(this).trim()).toString();
    }

    /**
     * Deserializes stacktrace elements as well as suppressed exceptions from the given output stream and
     * adds it to the given exception.
     */
    public static <T extends Throwable> T readStackTrace(T throwable, StreamInput in) throws IOException {
        throwable.setStackTrace(in.readArray(i -> {
            final String declaringClasss = i.readString();
            final String fileName = i.readOptionalString();
            final String methodName = i.readString();
            final int lineNumber = i.readVInt();
            return new StackTraceElement(declaringClasss, methodName, fileName, lineNumber);
        }, StackTraceElement[]::new));

        int numSuppressed = in.readVInt();
        for (int i = 0; i < numSuppressed; i++) {
            throwable.addSuppressed(in.readException());
        }
        return throwable;
    }

    /**
     * Serializes the given exceptions stacktrace elements as well as it's suppressed exceptions to the given output stream.
     */
    public static <T extends Throwable> T writeStackTraces(T throwable, StreamOutput out,
                                                           Writer<Throwable> exceptionWriter) throws IOException {
        out.writeArray((o, v) -> {
            o.writeString(v.getClassName());
            o.writeOptionalString(v.getFileName());
            o.writeString(v.getMethodName());
            o.writeVInt(v.getLineNumber());
        }, throwable.getStackTrace());
        out.writeArray(exceptionWriter, throwable.getSuppressed());
        return throwable;
    }

    /**
     * This is the list of Exceptions Fesen can throw over the wire or save into a corruption marker. Each value in the enum is a
     * single exception tying the Class to an id for use of the encode side and the id back to a constructor for use on the decode side. As
     * such its ok if the exceptions to change names so long as their constructor can still read the exception. Each exception is listed
     * in id order below. If you want to remove an exception leave a tombstone comment and mark the id as null in
     * ExceptionSerializationTests.testIds.ids.
     */
    private enum FesenExceptionHandle {
        INDEX_SHARD_SNAPSHOT_FAILED_EXCEPTION(org.codelibs.fesen.index.snapshots.IndexShardSnapshotFailedException.class,
                org.codelibs.fesen.index.snapshots.IndexShardSnapshotFailedException::new, 0, UNKNOWN_VERSION_ADDED),
        DFS_PHASE_EXECUTION_EXCEPTION(org.codelibs.fesen.search.dfs.DfsPhaseExecutionException.class,
                org.codelibs.fesen.search.dfs.DfsPhaseExecutionException::new, 1, UNKNOWN_VERSION_ADDED),
        EXECUTION_CANCELLED_EXCEPTION(org.codelibs.fesen.common.util.CancellableThreads.ExecutionCancelledException.class,
                org.codelibs.fesen.common.util.CancellableThreads.ExecutionCancelledException::new, 2, UNKNOWN_VERSION_ADDED),
        MASTER_NOT_DISCOVERED_EXCEPTION(org.codelibs.fesen.discovery.MasterNotDiscoveredException.class,
                org.codelibs.fesen.discovery.MasterNotDiscoveredException::new, 3, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_SECURITY_EXCEPTION(org.codelibs.fesen.FesenSecurityException.class,
                org.codelibs.fesen.FesenSecurityException::new, 4, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_RESTORE_EXCEPTION(org.codelibs.fesen.index.snapshots.IndexShardRestoreException.class,
                org.codelibs.fesen.index.snapshots.IndexShardRestoreException::new, 5, UNKNOWN_VERSION_ADDED),
        INDEX_CLOSED_EXCEPTION(org.codelibs.fesen.indices.IndexClosedException.class,
                org.codelibs.fesen.indices.IndexClosedException::new, 6, UNKNOWN_VERSION_ADDED),
        BIND_HTTP_EXCEPTION(org.codelibs.fesen.http.BindHttpException.class,
                org.codelibs.fesen.http.BindHttpException::new, 7, UNKNOWN_VERSION_ADDED),
        REDUCE_SEARCH_PHASE_EXCEPTION(org.codelibs.fesen.action.search.ReduceSearchPhaseException.class,
                org.codelibs.fesen.action.search.ReduceSearchPhaseException::new, 8, UNKNOWN_VERSION_ADDED),
        NODE_CLOSED_EXCEPTION(org.codelibs.fesen.node.NodeClosedException.class,
                org.codelibs.fesen.node.NodeClosedException::new, 9, UNKNOWN_VERSION_ADDED),
        SNAPSHOT_FAILED_ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.SnapshotFailedEngineException.class,
                org.codelibs.fesen.index.engine.SnapshotFailedEngineException::new, 10, UNKNOWN_VERSION_ADDED),
        SHARD_NOT_FOUND_EXCEPTION(org.codelibs.fesen.index.shard.ShardNotFoundException.class,
                org.codelibs.fesen.index.shard.ShardNotFoundException::new, 11, UNKNOWN_VERSION_ADDED),
        CONNECT_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.ConnectTransportException.class,
                org.codelibs.fesen.transport.ConnectTransportException::new, 12, UNKNOWN_VERSION_ADDED),
        NOT_SERIALIZABLE_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.NotSerializableTransportException.class,
                org.codelibs.fesen.transport.NotSerializableTransportException::new, 13, UNKNOWN_VERSION_ADDED),
        RESPONSE_HANDLER_FAILURE_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.ResponseHandlerFailureTransportException.class,
                org.codelibs.fesen.transport.ResponseHandlerFailureTransportException::new, 14, UNKNOWN_VERSION_ADDED),
        INDEX_CREATION_EXCEPTION(org.codelibs.fesen.indices.IndexCreationException.class,
                org.codelibs.fesen.indices.IndexCreationException::new, 15, UNKNOWN_VERSION_ADDED),
        INDEX_NOT_FOUND_EXCEPTION(org.codelibs.fesen.index.IndexNotFoundException.class,
                org.codelibs.fesen.index.IndexNotFoundException::new, 16, UNKNOWN_VERSION_ADDED),
        ILLEGAL_SHARD_ROUTING_STATE_EXCEPTION(org.codelibs.fesen.cluster.routing.IllegalShardRoutingStateException.class,
                org.codelibs.fesen.cluster.routing.IllegalShardRoutingStateException::new, 17, UNKNOWN_VERSION_ADDED),
        BROADCAST_SHARD_OPERATION_FAILED_EXCEPTION(org.codelibs.fesen.action.support.broadcast.BroadcastShardOperationFailedException.class,
                org.codelibs.fesen.action.support.broadcast.BroadcastShardOperationFailedException::new, 18, UNKNOWN_VERSION_ADDED),
        RESOURCE_NOT_FOUND_EXCEPTION(org.codelibs.fesen.ResourceNotFoundException.class,
                org.codelibs.fesen.ResourceNotFoundException::new, 19, UNKNOWN_VERSION_ADDED),
        ACTION_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.ActionTransportException.class,
                org.codelibs.fesen.transport.ActionTransportException::new, 20, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_GENERATION_EXCEPTION(org.codelibs.fesen.FesenGenerationException.class,
                org.codelibs.fesen.FesenGenerationException::new, 21, UNKNOWN_VERSION_ADDED),
        //      22 was CreateFailedEngineException
        INDEX_SHARD_STARTED_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardStartedException.class,
                org.codelibs.fesen.index.shard.IndexShardStartedException::new, 23, UNKNOWN_VERSION_ADDED),
        SEARCH_CONTEXT_MISSING_EXCEPTION(org.codelibs.fesen.search.SearchContextMissingException.class,
                org.codelibs.fesen.search.SearchContextMissingException::new, 24, UNKNOWN_VERSION_ADDED),
        GENERAL_SCRIPT_EXCEPTION(org.codelibs.fesen.script.GeneralScriptException.class,
                org.codelibs.fesen.script.GeneralScriptException::new, 25, UNKNOWN_VERSION_ADDED),
        // 26 was BatchOperationException
        SNAPSHOT_CREATION_EXCEPTION(org.codelibs.fesen.snapshots.SnapshotCreationException.class,
                org.codelibs.fesen.snapshots.SnapshotCreationException::new, 27, UNKNOWN_VERSION_ADDED),
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        DOCUMENT_MISSING_EXCEPTION(org.codelibs.fesen.index.engine.DocumentMissingException.class,
                org.codelibs.fesen.index.engine.DocumentMissingException::new, 29, UNKNOWN_VERSION_ADDED),
        SNAPSHOT_EXCEPTION(org.codelibs.fesen.snapshots.SnapshotException.class,
                org.codelibs.fesen.snapshots.SnapshotException::new, 30, UNKNOWN_VERSION_ADDED),
        INVALID_ALIAS_NAME_EXCEPTION(org.codelibs.fesen.indices.InvalidAliasNameException.class,
                org.codelibs.fesen.indices.InvalidAliasNameException::new, 31, UNKNOWN_VERSION_ADDED),
        INVALID_INDEX_NAME_EXCEPTION(org.codelibs.fesen.indices.InvalidIndexNameException.class,
                org.codelibs.fesen.indices.InvalidIndexNameException::new, 32, UNKNOWN_VERSION_ADDED),
        INDEX_PRIMARY_SHARD_NOT_ALLOCATED_EXCEPTION(org.codelibs.fesen.indices.IndexPrimaryShardNotAllocatedException.class,
                org.codelibs.fesen.indices.IndexPrimaryShardNotAllocatedException::new, 33, UNKNOWN_VERSION_ADDED),
        TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.TransportException.class,
                org.codelibs.fesen.transport.TransportException::new, 34, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_PARSE_EXCEPTION(org.codelibs.fesen.FesenParseException.class,
                org.codelibs.fesen.FesenParseException::new, 35, UNKNOWN_VERSION_ADDED),
        SEARCH_EXCEPTION(org.codelibs.fesen.search.SearchException.class,
                org.codelibs.fesen.search.SearchException::new, 36, UNKNOWN_VERSION_ADDED),
        MAPPER_EXCEPTION(org.codelibs.fesen.index.mapper.MapperException.class,
                org.codelibs.fesen.index.mapper.MapperException::new, 37, UNKNOWN_VERSION_ADDED),
        INVALID_TYPE_NAME_EXCEPTION(org.codelibs.fesen.indices.InvalidTypeNameException.class,
                org.codelibs.fesen.indices.InvalidTypeNameException::new, 38, UNKNOWN_VERSION_ADDED),
        SNAPSHOT_RESTORE_EXCEPTION(org.codelibs.fesen.snapshots.SnapshotRestoreException.class,
                org.codelibs.fesen.snapshots.SnapshotRestoreException::new, 39, UNKNOWN_VERSION_ADDED),
        PARSING_EXCEPTION(org.codelibs.fesen.common.ParsingException.class, org.codelibs.fesen.common.ParsingException::new, 40,
            UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_CLOSED_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardClosedException.class,
                org.codelibs.fesen.index.shard.IndexShardClosedException::new, 41, UNKNOWN_VERSION_ADDED),
        RECOVER_FILES_RECOVERY_EXCEPTION(org.codelibs.fesen.indices.recovery.RecoverFilesRecoveryException.class,
                org.codelibs.fesen.indices.recovery.RecoverFilesRecoveryException::new, 42, UNKNOWN_VERSION_ADDED),
        TRUNCATED_TRANSLOG_EXCEPTION(org.codelibs.fesen.index.translog.TruncatedTranslogException.class,
                org.codelibs.fesen.index.translog.TruncatedTranslogException::new, 43, UNKNOWN_VERSION_ADDED),
        RECOVERY_FAILED_EXCEPTION(org.codelibs.fesen.indices.recovery.RecoveryFailedException.class,
                org.codelibs.fesen.indices.recovery.RecoveryFailedException::new, 44, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_RELOCATED_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardRelocatedException.class,
                org.codelibs.fesen.index.shard.IndexShardRelocatedException::new, 45, UNKNOWN_VERSION_ADDED),
        NODE_SHOULD_NOT_CONNECT_EXCEPTION(org.codelibs.fesen.transport.NodeShouldNotConnectException.class,
                org.codelibs.fesen.transport.NodeShouldNotConnectException::new, 46, UNKNOWN_VERSION_ADDED),
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        TRANSLOG_CORRUPTED_EXCEPTION(org.codelibs.fesen.index.translog.TranslogCorruptedException.class,
                org.codelibs.fesen.index.translog.TranslogCorruptedException::new, 48, UNKNOWN_VERSION_ADDED),
        CLUSTER_BLOCK_EXCEPTION(org.codelibs.fesen.cluster.block.ClusterBlockException.class,
                org.codelibs.fesen.cluster.block.ClusterBlockException::new, 49, UNKNOWN_VERSION_ADDED),
        FETCH_PHASE_EXECUTION_EXCEPTION(org.codelibs.fesen.search.fetch.FetchPhaseExecutionException.class,
                org.codelibs.fesen.search.fetch.FetchPhaseExecutionException::new, 50, UNKNOWN_VERSION_ADDED),
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        VERSION_CONFLICT_ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.VersionConflictEngineException.class,
                org.codelibs.fesen.index.engine.VersionConflictEngineException::new, 52, UNKNOWN_VERSION_ADDED),
        ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.EngineException.class, org.codelibs.fesen.index.engine.EngineException::new, 53,
            UNKNOWN_VERSION_ADDED),
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        NO_SUCH_NODE_EXCEPTION(org.codelibs.fesen.action.NoSuchNodeException.class, org.codelibs.fesen.action.NoSuchNodeException::new, 55,
            UNKNOWN_VERSION_ADDED),
        SETTINGS_EXCEPTION(org.codelibs.fesen.common.settings.SettingsException.class,
                org.codelibs.fesen.common.settings.SettingsException::new, 56, UNKNOWN_VERSION_ADDED),
        INDEX_TEMPLATE_MISSING_EXCEPTION(org.codelibs.fesen.indices.IndexTemplateMissingException.class,
                org.codelibs.fesen.indices.IndexTemplateMissingException::new, 57, UNKNOWN_VERSION_ADDED),
        SEND_REQUEST_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.SendRequestTransportException.class,
                org.codelibs.fesen.transport.SendRequestTransportException::new, 58, UNKNOWN_VERSION_ADDED),
        // 59 used to be EsRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        NOT_SERIALIZABLE_EXCEPTION_WRAPPER(org.codelibs.fesen.common.io.stream.NotSerializableExceptionWrapper.class,
                org.codelibs.fesen.common.io.stream.NotSerializableExceptionWrapper::new, 62, UNKNOWN_VERSION_ADDED),
        ALIAS_FILTER_PARSING_EXCEPTION(org.codelibs.fesen.indices.AliasFilterParsingException.class,
                org.codelibs.fesen.indices.AliasFilterParsingException::new, 63, UNKNOWN_VERSION_ADDED),
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        GATEWAY_EXCEPTION(org.codelibs.fesen.gateway.GatewayException.class, org.codelibs.fesen.gateway.GatewayException::new, 65,
            UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_NOT_RECOVERING_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardNotRecoveringException.class,
                org.codelibs.fesen.index.shard.IndexShardNotRecoveringException::new, 66, UNKNOWN_VERSION_ADDED),
        HTTP_EXCEPTION(org.codelibs.fesen.http.HttpException.class, org.codelibs.fesen.http.HttpException::new, 67, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_EXCEPTION(org.codelibs.fesen.FesenException.class,
                org.codelibs.fesen.FesenException::new, 68, UNKNOWN_VERSION_ADDED),
        SNAPSHOT_MISSING_EXCEPTION(org.codelibs.fesen.snapshots.SnapshotMissingException.class,
                org.codelibs.fesen.snapshots.SnapshotMissingException::new, 69, UNKNOWN_VERSION_ADDED),
        PRIMARY_MISSING_ACTION_EXCEPTION(org.codelibs.fesen.action.PrimaryMissingActionException.class,
                org.codelibs.fesen.action.PrimaryMissingActionException::new, 70, UNKNOWN_VERSION_ADDED),
        FAILED_NODE_EXCEPTION(org.codelibs.fesen.action.FailedNodeException.class, org.codelibs.fesen.action.FailedNodeException::new, 71,
            UNKNOWN_VERSION_ADDED),
        SEARCH_PARSE_EXCEPTION(org.codelibs.fesen.search.SearchParseException.class, org.codelibs.fesen.search.SearchParseException::new, 72,
            UNKNOWN_VERSION_ADDED),
        CONCURRENT_SNAPSHOT_EXECUTION_EXCEPTION(org.codelibs.fesen.snapshots.ConcurrentSnapshotExecutionException.class,
                org.codelibs.fesen.snapshots.ConcurrentSnapshotExecutionException::new, 73, UNKNOWN_VERSION_ADDED),
        BLOB_STORE_EXCEPTION(org.codelibs.fesen.common.blobstore.BlobStoreException.class,
                org.codelibs.fesen.common.blobstore.BlobStoreException::new, 74, UNKNOWN_VERSION_ADDED),
        INCOMPATIBLE_CLUSTER_STATE_VERSION_EXCEPTION(org.codelibs.fesen.cluster.IncompatibleClusterStateVersionException.class,
                org.codelibs.fesen.cluster.IncompatibleClusterStateVersionException::new, 75, UNKNOWN_VERSION_ADDED),
        RECOVERY_ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.RecoveryEngineException.class,
                org.codelibs.fesen.index.engine.RecoveryEngineException::new, 76, UNKNOWN_VERSION_ADDED),
        UNCATEGORIZED_EXECUTION_EXCEPTION(org.codelibs.fesen.common.util.concurrent.UncategorizedExecutionException.class,
                org.codelibs.fesen.common.util.concurrent.UncategorizedExecutionException::new, 77, UNKNOWN_VERSION_ADDED),
        TIMESTAMP_PARSING_EXCEPTION(org.codelibs.fesen.action.TimestampParsingException.class,
                org.codelibs.fesen.action.TimestampParsingException::new, 78, UNKNOWN_VERSION_ADDED),
        ROUTING_MISSING_EXCEPTION(org.codelibs.fesen.action.RoutingMissingException.class,
                org.codelibs.fesen.action.RoutingMissingException::new, 79, UNKNOWN_VERSION_ADDED),
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        INDEX_SHARD_RESTORE_FAILED_EXCEPTION(org.codelibs.fesen.index.snapshots.IndexShardRestoreFailedException.class,
                org.codelibs.fesen.index.snapshots.IndexShardRestoreFailedException::new, 81, UNKNOWN_VERSION_ADDED),
        REPOSITORY_EXCEPTION(org.codelibs.fesen.repositories.RepositoryException.class,
                org.codelibs.fesen.repositories.RepositoryException::new, 82, UNKNOWN_VERSION_ADDED),
        RECEIVE_TIMEOUT_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.ReceiveTimeoutTransportException.class,
                org.codelibs.fesen.transport.ReceiveTimeoutTransportException::new, 83, UNKNOWN_VERSION_ADDED),
        NODE_DISCONNECTED_EXCEPTION(org.codelibs.fesen.transport.NodeDisconnectedException.class,
                org.codelibs.fesen.transport.NodeDisconnectedException::new, 84, UNKNOWN_VERSION_ADDED),
        // 85 used to be for AlreadyExpiredException
        AGGREGATION_EXECUTION_EXCEPTION(org.codelibs.fesen.search.aggregations.AggregationExecutionException.class,
                org.codelibs.fesen.search.aggregations.AggregationExecutionException::new, 86, UNKNOWN_VERSION_ADDED),
        // 87 used to be for MergeMappingException
        INVALID_INDEX_TEMPLATE_EXCEPTION(org.codelibs.fesen.indices.InvalidIndexTemplateException.class,
                org.codelibs.fesen.indices.InvalidIndexTemplateException::new, 88, UNKNOWN_VERSION_ADDED),
        REFRESH_FAILED_ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.RefreshFailedEngineException.class,
                org.codelibs.fesen.index.engine.RefreshFailedEngineException::new, 90, UNKNOWN_VERSION_ADDED),
        AGGREGATION_INITIALIZATION_EXCEPTION(org.codelibs.fesen.search.aggregations.AggregationInitializationException.class,
                org.codelibs.fesen.search.aggregations.AggregationInitializationException::new, 91, UNKNOWN_VERSION_ADDED),
        DELAY_RECOVERY_EXCEPTION(org.codelibs.fesen.indices.recovery.DelayRecoveryException.class,
                org.codelibs.fesen.indices.recovery.DelayRecoveryException::new, 92, UNKNOWN_VERSION_ADDED),
        // 93 used to be for IndexWarmerMissingException
        NO_NODE_AVAILABLE_EXCEPTION(org.codelibs.fesen.client.transport.NoNodeAvailableException.class,
                org.codelibs.fesen.client.transport.NoNodeAvailableException::new, 94, UNKNOWN_VERSION_ADDED),
        INVALID_SNAPSHOT_NAME_EXCEPTION(org.codelibs.fesen.snapshots.InvalidSnapshotNameException.class,
                org.codelibs.fesen.snapshots.InvalidSnapshotNameException::new, 96, UNKNOWN_VERSION_ADDED),
        ILLEGAL_INDEX_SHARD_STATE_EXCEPTION(org.codelibs.fesen.index.shard.IllegalIndexShardStateException.class,
                org.codelibs.fesen.index.shard.IllegalIndexShardStateException::new, 97, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_SNAPSHOT_EXCEPTION(org.codelibs.fesen.index.snapshots.IndexShardSnapshotException.class,
                org.codelibs.fesen.index.snapshots.IndexShardSnapshotException::new, 98, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_NOT_STARTED_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardNotStartedException.class,
                org.codelibs.fesen.index.shard.IndexShardNotStartedException::new, 99, UNKNOWN_VERSION_ADDED),
        SEARCH_PHASE_EXECUTION_EXCEPTION(org.codelibs.fesen.action.search.SearchPhaseExecutionException.class,
                org.codelibs.fesen.action.search.SearchPhaseExecutionException::new, 100, UNKNOWN_VERSION_ADDED),
        ACTION_NOT_FOUND_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.ActionNotFoundTransportException.class,
                org.codelibs.fesen.transport.ActionNotFoundTransportException::new, 101, UNKNOWN_VERSION_ADDED),
        TRANSPORT_SERIALIZATION_EXCEPTION(org.codelibs.fesen.transport.TransportSerializationException.class,
                org.codelibs.fesen.transport.TransportSerializationException::new, 102, UNKNOWN_VERSION_ADDED),
        REMOTE_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.RemoteTransportException.class,
                org.codelibs.fesen.transport.RemoteTransportException::new, 103, UNKNOWN_VERSION_ADDED),
        ENGINE_CREATION_FAILURE_EXCEPTION(org.codelibs.fesen.index.engine.EngineCreationFailureException.class,
                org.codelibs.fesen.index.engine.EngineCreationFailureException::new, 104, UNKNOWN_VERSION_ADDED),
        ROUTING_EXCEPTION(org.codelibs.fesen.cluster.routing.RoutingException.class,
                org.codelibs.fesen.cluster.routing.RoutingException::new, 105, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_RECOVERY_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardRecoveryException.class,
                org.codelibs.fesen.index.shard.IndexShardRecoveryException::new, 106, UNKNOWN_VERSION_ADDED),
        REPOSITORY_MISSING_EXCEPTION(org.codelibs.fesen.repositories.RepositoryMissingException.class,
                org.codelibs.fesen.repositories.RepositoryMissingException::new, 107, UNKNOWN_VERSION_ADDED),
        DOCUMENT_SOURCE_MISSING_EXCEPTION(org.codelibs.fesen.index.engine.DocumentSourceMissingException.class,
                org.codelibs.fesen.index.engine.DocumentSourceMissingException::new, 109, UNKNOWN_VERSION_ADDED),
        // 110 used to be FlushNotAllowedEngineException
        NO_CLASS_SETTINGS_EXCEPTION(org.codelibs.fesen.common.settings.NoClassSettingsException.class,
                org.codelibs.fesen.common.settings.NoClassSettingsException::new, 111, UNKNOWN_VERSION_ADDED),
        BIND_TRANSPORT_EXCEPTION(org.codelibs.fesen.transport.BindTransportException.class,
                org.codelibs.fesen.transport.BindTransportException::new, 112, UNKNOWN_VERSION_ADDED),
        ALIASES_NOT_FOUND_EXCEPTION(org.codelibs.fesen.rest.action.admin.indices.AliasesNotFoundException.class,
                org.codelibs.fesen.rest.action.admin.indices.AliasesNotFoundException::new, 113, UNKNOWN_VERSION_ADDED),
        INDEX_SHARD_RECOVERING_EXCEPTION(org.codelibs.fesen.index.shard.IndexShardRecoveringException.class,
                org.codelibs.fesen.index.shard.IndexShardRecoveringException::new, 114, UNKNOWN_VERSION_ADDED),
        TRANSLOG_EXCEPTION(org.codelibs.fesen.index.translog.TranslogException.class,
                org.codelibs.fesen.index.translog.TranslogException::new, 115, UNKNOWN_VERSION_ADDED),
        PROCESS_CLUSTER_EVENT_TIMEOUT_EXCEPTION(org.codelibs.fesen.cluster.metadata.ProcessClusterEventTimeoutException.class,
                org.codelibs.fesen.cluster.metadata.ProcessClusterEventTimeoutException::new, 116, UNKNOWN_VERSION_ADDED),
        RETRY_ON_PRIMARY_EXCEPTION(ReplicationOperation.RetryOnPrimaryException.class,
                ReplicationOperation.RetryOnPrimaryException::new, 117, UNKNOWN_VERSION_ADDED),
        ELASTICSEARCH_TIMEOUT_EXCEPTION(org.codelibs.fesen.FesenTimeoutException.class,
                org.codelibs.fesen.FesenTimeoutException::new, 118, UNKNOWN_VERSION_ADDED),
        QUERY_PHASE_EXECUTION_EXCEPTION(org.codelibs.fesen.search.query.QueryPhaseExecutionException.class,
                org.codelibs.fesen.search.query.QueryPhaseExecutionException::new, 119, UNKNOWN_VERSION_ADDED),
        REPOSITORY_VERIFICATION_EXCEPTION(org.codelibs.fesen.repositories.RepositoryVerificationException.class,
                org.codelibs.fesen.repositories.RepositoryVerificationException::new, 120, UNKNOWN_VERSION_ADDED),
        INVALID_AGGREGATION_PATH_EXCEPTION(org.codelibs.fesen.search.aggregations.InvalidAggregationPathException.class,
                org.codelibs.fesen.search.aggregations.InvalidAggregationPathException::new, 121, UNKNOWN_VERSION_ADDED),
        // 123 used to be IndexAlreadyExistsException and was renamed
        RESOURCE_ALREADY_EXISTS_EXCEPTION(ResourceAlreadyExistsException.class,
            ResourceAlreadyExistsException::new, 123, UNKNOWN_VERSION_ADDED),
        // 124 used to be Script.ScriptParseException
        HTTP_REQUEST_ON_TRANSPORT_EXCEPTION(TcpTransport.HttpRequestOnTransportException.class,
                TcpTransport.HttpRequestOnTransportException::new, 125, UNKNOWN_VERSION_ADDED),
        MAPPER_PARSING_EXCEPTION(org.codelibs.fesen.index.mapper.MapperParsingException.class,
                org.codelibs.fesen.index.mapper.MapperParsingException::new, 126, UNKNOWN_VERSION_ADDED),
        // 127 used to be org.codelibs.fesen.search.SearchContextException
        SEARCH_SOURCE_BUILDER_EXCEPTION(org.codelibs.fesen.search.builder.SearchSourceBuilderException.class,
                org.codelibs.fesen.search.builder.SearchSourceBuilderException::new, 128, UNKNOWN_VERSION_ADDED),
        // 129 was EngineClosedException
        NO_SHARD_AVAILABLE_ACTION_EXCEPTION(org.codelibs.fesen.action.NoShardAvailableActionException.class,
                org.codelibs.fesen.action.NoShardAvailableActionException::new, 130, UNKNOWN_VERSION_ADDED),
        UNAVAILABLE_SHARDS_EXCEPTION(org.codelibs.fesen.action.UnavailableShardsException.class,
                org.codelibs.fesen.action.UnavailableShardsException::new, 131, UNKNOWN_VERSION_ADDED),
        FLUSH_FAILED_ENGINE_EXCEPTION(org.codelibs.fesen.index.engine.FlushFailedEngineException.class,
                org.codelibs.fesen.index.engine.FlushFailedEngineException::new, 132, UNKNOWN_VERSION_ADDED),
        CIRCUIT_BREAKING_EXCEPTION(org.codelibs.fesen.common.breaker.CircuitBreakingException.class,
                org.codelibs.fesen.common.breaker.CircuitBreakingException::new, 133, UNKNOWN_VERSION_ADDED),
        NODE_NOT_CONNECTED_EXCEPTION(org.codelibs.fesen.transport.NodeNotConnectedException.class,
                org.codelibs.fesen.transport.NodeNotConnectedException::new, 134, UNKNOWN_VERSION_ADDED),
        STRICT_DYNAMIC_MAPPING_EXCEPTION(org.codelibs.fesen.index.mapper.StrictDynamicMappingException.class,
                org.codelibs.fesen.index.mapper.StrictDynamicMappingException::new, 135, UNKNOWN_VERSION_ADDED),
        RETRY_ON_REPLICA_EXCEPTION(org.codelibs.fesen.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
                org.codelibs.fesen.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new, 136,
            UNKNOWN_VERSION_ADDED),
        TYPE_MISSING_EXCEPTION(org.codelibs.fesen.indices.TypeMissingException.class,
                org.codelibs.fesen.indices.TypeMissingException::new, 137, UNKNOWN_VERSION_ADDED),
        FAILED_TO_COMMIT_CLUSTER_STATE_EXCEPTION(org.codelibs.fesen.cluster.coordination.FailedToCommitClusterStateException.class,
                org.codelibs.fesen.cluster.coordination.FailedToCommitClusterStateException::new, 140, UNKNOWN_VERSION_ADDED),
        QUERY_SHARD_EXCEPTION(org.codelibs.fesen.index.query.QueryShardException.class,
                org.codelibs.fesen.index.query.QueryShardException::new, 141, UNKNOWN_VERSION_ADDED),
        NO_LONGER_PRIMARY_SHARD_EXCEPTION(ShardStateAction.NoLongerPrimaryShardException.class,
                ShardStateAction.NoLongerPrimaryShardException::new, 142, UNKNOWN_VERSION_ADDED),
        SCRIPT_EXCEPTION(org.codelibs.fesen.script.ScriptException.class, org.codelibs.fesen.script.ScriptException::new, 143,
            UNKNOWN_VERSION_ADDED),
        NOT_MASTER_EXCEPTION(org.codelibs.fesen.cluster.NotMasterException.class, org.codelibs.fesen.cluster.NotMasterException::new, 144,
            UNKNOWN_VERSION_ADDED),
        STATUS_EXCEPTION(org.codelibs.fesen.FesenStatusException.class, org.codelibs.fesen.FesenStatusException::new, 145,
            UNKNOWN_VERSION_ADDED),
        TASK_CANCELLED_EXCEPTION(org.codelibs.fesen.tasks.TaskCancelledException.class,
            org.codelibs.fesen.tasks.TaskCancelledException::new, 146, UNKNOWN_VERSION_ADDED),
        SHARD_LOCK_OBTAIN_FAILED_EXCEPTION(org.codelibs.fesen.env.ShardLockObtainFailedException.class,
                                           org.codelibs.fesen.env.ShardLockObtainFailedException::new, 147, UNKNOWN_VERSION_ADDED),
        // 148 was UnknownNamedObjectException
        TOO_MANY_BUCKETS_EXCEPTION(MultiBucketConsumerService.TooManyBucketsException.class,
            MultiBucketConsumerService.TooManyBucketsException::new, 149, Version.V_6_2_0),
        COORDINATION_STATE_REJECTED_EXCEPTION(org.codelibs.fesen.cluster.coordination.CoordinationStateRejectedException.class,
            org.codelibs.fesen.cluster.coordination.CoordinationStateRejectedException::new, 150, Version.V_7_0_0),
        SNAPSHOT_IN_PROGRESS_EXCEPTION(org.codelibs.fesen.snapshots.SnapshotInProgressException.class,
            org.codelibs.fesen.snapshots.SnapshotInProgressException::new, 151, Version.V_6_7_0),
        NO_SUCH_REMOTE_CLUSTER_EXCEPTION(org.codelibs.fesen.transport.NoSuchRemoteClusterException.class,
            org.codelibs.fesen.transport.NoSuchRemoteClusterException::new, 152, Version.V_6_7_0),
        RETENTION_LEASE_ALREADY_EXISTS_EXCEPTION(
                org.codelibs.fesen.index.seqno.RetentionLeaseAlreadyExistsException.class,
                org.codelibs.fesen.index.seqno.RetentionLeaseAlreadyExistsException::new,
                153,
                Version.V_6_7_0),
        RETENTION_LEASE_NOT_FOUND_EXCEPTION(
                org.codelibs.fesen.index.seqno.RetentionLeaseNotFoundException.class,
                org.codelibs.fesen.index.seqno.RetentionLeaseNotFoundException::new,
                154,
                Version.V_6_7_0),
        SHARD_NOT_IN_PRIMARY_MODE_EXCEPTION(
                org.codelibs.fesen.index.shard.ShardNotInPrimaryModeException.class,
                org.codelibs.fesen.index.shard.ShardNotInPrimaryModeException::new,
                155,
                Version.V_6_8_1),
        RETENTION_LEASE_INVALID_RETAINING_SEQUENCE_NUMBER_EXCEPTION(
                org.codelibs.fesen.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
                org.codelibs.fesen.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
                156,
                Version.V_7_5_0),
        INGEST_PROCESSOR_EXCEPTION(
                org.codelibs.fesen.ingest.IngestProcessorException.class,
                org.codelibs.fesen.ingest.IngestProcessorException::new,
                157,
                Version.V_7_5_0),
        PEER_RECOVERY_NOT_FOUND_EXCEPTION(
                org.codelibs.fesen.indices.recovery.PeerRecoveryNotFound.class,
                org.codelibs.fesen.indices.recovery.PeerRecoveryNotFound::new,
                158,
                Version.V_7_9_0),
        NODE_HEALTH_CHECK_FAILURE_EXCEPTION(
                org.codelibs.fesen.cluster.coordination.NodeHealthCheckFailureException.class,
                org.codelibs.fesen.cluster.coordination.NodeHealthCheckFailureException::new,
                159,
                Version.V_7_9_0),
        NO_SEED_NODE_LEFT_EXCEPTION(
                org.codelibs.fesen.transport.NoSeedNodeLeftException.class,
                org.codelibs.fesen.transport.NoSeedNodeLeftException::new,
                160,
                Version.V_7_10_0);

        final Class<? extends FesenException> exceptionClass;
        final CheckedFunction<StreamInput, ? extends FesenException, IOException> constructor;
        final int id;
        final Version versionAdded;

        <E extends FesenException> FesenExceptionHandle(Class<E> exceptionClass,
                                                                        CheckedFunction<StreamInput, E, IOException> constructor, int id,
                                                                        Version versionAdded) {
            // We need the exceptionClass because you can't dig it out of the constructor reliably.
            this.exceptionClass = exceptionClass;
            this.constructor = constructor;
            this.versionAdded = versionAdded;
            this.id = id;
        }
    }

    /**
     * Returns an array of all registered handle IDs. These are the IDs for every registered
     * exception.
     *
     * @return an array of all registered handle IDs
     */
    static int[] ids() {
        return Arrays.stream(FesenExceptionHandle.values()).mapToInt(h -> h.id).toArray();
    }

    /**
     * Returns an array of all registered pairs of handle IDs and exception classes. These pairs are
     * provided for every registered exception.
     *
     * @return an array of all registered pairs of handle IDs and exception classes
     */
    static Tuple<Integer, Class<? extends FesenException>>[] classes() {
        @SuppressWarnings("unchecked")
        final Tuple<Integer, Class<? extends FesenException>>[] ts =
                Arrays.stream(FesenExceptionHandle.values())
                        .map(h -> Tuple.tuple(h.id, h.exceptionClass)).toArray(Tuple[]::new);
        return ts;
    }

    static {
        ID_TO_SUPPLIER = unmodifiableMap(Arrays
                .stream(FesenExceptionHandle.values()).collect(Collectors.toMap(e -> e.id, e -> e.constructor)));
        CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE = unmodifiableMap(Arrays
                .stream(FesenExceptionHandle.values()).collect(Collectors.toMap(e -> e.exceptionClass, e -> e)));
    }

    public Index getIndex() {
        List<String> index = getMetadata(INDEX_METADATA_KEY);
        if (index != null && index.isEmpty() == false) {
            List<String> index_uuid = getMetadata(INDEX_METADATA_KEY_UUID);
            return new Index(index.get(0), index_uuid.get(0));
        }

        return null;
    }

    public ShardId getShardId() {
        List<String> shard = getMetadata(SHARD_METADATA_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setIndex(Index index) {
        if (index != null) {
            addMetadata(INDEX_METADATA_KEY, index.getName());
            addMetadata(INDEX_METADATA_KEY_UUID, index.getUUID());
        }
    }

    public void setIndex(String index) {
        if (index != null) {
            setIndex(new Index(index, INDEX_UUID_NA_VALUE));
        }
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            setIndex(shardId.getIndex());
            addMetadata(SHARD_METADATA_KEY, Integer.toString(shardId.id()));
        }
    }

    public void setResources(String type, String... id) {
        assert type != null;
        addMetadata(RESOURCE_METADATA_ID_KEY, id);
        addMetadata(RESOURCE_METADATA_TYPE_KEY, type);
    }

    public List<String> getResourceId() {
        return getMetadata(RESOURCE_METADATA_ID_KEY);
    }

    public String getResourceType() {
        List<String> header = getMetadata(RESOURCE_METADATA_TYPE_KEY);
        if (header != null && header.isEmpty() == false) {
            assert header.size() == 1;
            return header.get(0);
        }
        return null;
    }

    // lower cases and adds underscores to transitions in a name
    private static String toUnderscoreCase(String value) {
        StringBuilder sb = new StringBuilder();
        boolean changed = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!changed) {
                    // copy it over here
                    for (int j = 0; j < i; j++) {
                        sb.append(value.charAt(j));
                    }
                    changed = true;
                    if (i == 0) {
                        sb.append(Character.toLowerCase(c));
                    } else {
                        sb.append('_');
                        sb.append(Character.toLowerCase(c));
                    }
                } else {
                    sb.append('_');
                    sb.append(Character.toLowerCase(c));
                }
            } else {
                if (changed) {
                    sb.append(c);
                }
            }
        }
        if (!changed) {
            return value;
        }
        return sb.toString();
    }

}
