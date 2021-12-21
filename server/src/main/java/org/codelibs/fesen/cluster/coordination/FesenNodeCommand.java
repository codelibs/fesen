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
package org.codelibs.fesen.cluster.coordination;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.LockObtainFailedException;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.admin.indices.rollover.Condition;
import org.codelibs.fesen.cli.EnvironmentAwareCommand;
import org.codelibs.fesen.cli.Terminal;
import org.codelibs.fesen.cli.UserException;
import org.codelibs.fesen.cluster.ClusterModule;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.Diff;
import org.codelibs.fesen.cluster.metadata.DataStreamMetadata;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.BigArrays;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.core.Tuple;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.env.NodeEnvironment;
import org.codelibs.fesen.env.NodeMetadata;
import org.codelibs.fesen.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

public abstract class FesenNodeCommand extends EnvironmentAwareCommand {
    private static final Logger logger = LogManager.getLogger(FesenNodeCommand.class);
    protected static final String DELIMITER = "------------------------------------------------------------------------\n";
    static final String STOP_WARNING_MSG =
            DELIMITER +
                    "\n" +
                    "    WARNING: Fesen MUST be stopped before running this tool." +
                    "\n";
    protected static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = "failed to lock node's directory, is Fesen still running?";
    protected static final String ABORTED_BY_USER_MSG = "aborted by user";
    final OptionSpec<Integer> nodeOrdinalOption;
    static final String NO_NODE_FOLDER_FOUND_MSG = "no node folder is found in data folder(s), node has not been started yet?";
    static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";
    protected static final String CS_MISSING_MSG =
        "cluster state is empty, cluster has never been bootstrapped?";

    // fake the registry here, as command-line tools are not loading plugins, and ensure that it preserves the parsed XContent
    public static final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables()) {

        @SuppressWarnings("unchecked")
        @Override
        public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
            // Currently, two unknown top-level objects are present
            if (Metadata.Custom.class.isAssignableFrom(categoryClass)) {
                if (DataStreamMetadata.TYPE.equals(name)) {
                    // DataStreamMetadata is used inside Metadata class for validation purposes and building the indicesLookup,
                    // therefor even es node commands need to be able to parse it.
                    return super.parseNamedObject(categoryClass, name, parser, context);
                    // TODO: Try to parse other named objects (e.g. stored scripts, ingest pipelines) that are part of core es as well?
                    // Note that supporting PersistentTasksCustomMetadata is trickier, because PersistentTaskParams is a named object too.
                } else {
                    return (T) new UnknownMetadataCustom(name, parser.mapOrdered());
                }
            }
            if (Condition.class.isAssignableFrom(categoryClass)) {
                // The parsing for conditions is a bit weird as these represent JSON primitives (strings or numbers)
                // TODO: Make Condition non-pluggable
                assert parser.currentToken() == XContentParser.Token.FIELD_NAME : parser.currentToken();
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw new UnsupportedOperationException("Unexpected token for Condition: " + parser.currentToken());
                }
                parser.nextToken();
                assert parser.currentToken().isValue() : parser.currentToken();
                if (parser.currentToken().isValue() == false) {
                    throw new UnsupportedOperationException("Unexpected token for Condition: " + parser.currentToken());
                }
                return (T) new UnknownCondition(name, parser.objectText());
            }
            assert false : "Unexpected category class " + categoryClass + " for name " + name;
            throw new UnsupportedOperationException("Unexpected category class " + categoryClass + " for name " + name);
        }
    };

    public FesenNodeCommand(String description) {
        super(description);
        nodeOrdinalOption = parser.accepts("ordinal", "Optional node ordinal, 0 if not specified")
                .withRequiredArg().ofType(Integer.class);
    }

    public static PersistedClusterStateService createPersistedClusterStateService(Settings settings, Path[] dataPaths) throws IOException {
        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        if (nodeMetadata == null) {
            throw new FesenException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetadata.nodeId();
        return new PersistedClusterStateService(dataPaths, nodeId, namedXContentRegistry, BigArrays.NON_RECYCLING_INSTANCE,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
    }

    public static ClusterState clusterState(Environment environment, PersistedClusterStateService.OnDiskState onDiskState) {
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(environment.settings()))
            .version(onDiskState.lastAcceptedVersion)
            .metadata(onDiskState.metadata)
            .build();
    }

    public static Tuple<Long, ClusterState> loadTermAndClusterState(PersistedClusterStateService psf,
                                                                    Environment env) throws IOException {
        final PersistedClusterStateService.OnDiskState bestOnDiskState = psf.loadBestOnDiskState();
        if (bestOnDiskState.empty()) {
            throw new FesenException(CS_MISSING_MSG);
        }
        return Tuple.tuple(bestOnDiskState.currentTerm, clusterState(env, bestOnDiskState));
    }

    protected void processNodePaths(Terminal terminal, OptionSet options, Environment env) throws IOException, UserException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining lock for node");
        Integer nodeOrdinal = nodeOrdinalOption.value(options);
        if (nodeOrdinal == null) {
            nodeOrdinal = 0;
        }
        try (NodeEnvironment.NodeLock lock = new NodeEnvironment.NodeLock(nodeOrdinal, logger, env, Files::exists)) {
            final Path[] dataPaths =
                    Arrays.stream(lock.getNodePaths()).filter(Objects::nonNull).map(p -> p.path).toArray(Path[]::new);
            if (dataPaths.length == 0) {
                throw new FesenException(NO_NODE_FOLDER_FOUND_MSG);
            }
            processNodePaths(terminal, dataPaths, nodeOrdinal, options, env);
        } catch (LockObtainFailedException e) {
            throw new FesenException(FAILED_TO_OBTAIN_NODE_LOCK_MSG, e);
        }
    }

    protected void confirm(Terminal terminal, String msg) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new FesenException(ABORTED_BY_USER_MSG);
        }
    }

    @Override
    public final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        terminal.println(STOP_WARNING_MSG);
        if (validateBeforeLock(terminal, env)) {
            processNodePaths(terminal, options, env);
        }
    }

    /**
     * Validate that the command can run before taking any locks.
     * @param terminal the terminal to print to
     * @param env the env to validate.
     * @return true to continue, false to stop (must print message in validate).
     */
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        return true;
    }


    /**
     * Process the paths. Locks for the paths is held during this method invocation.
     * @param terminal the terminal to use for messages
     * @param dataPaths the paths of the node to process
     * @param options the command line options
     * @param env the env of the node to process
     */
    protected abstract void processNodePaths(Terminal terminal, Path[] dataPaths, int nodeLockId, OptionSet options, Environment env)
        throws IOException, UserException;

    protected NodeEnvironment.NodePath[] toNodePaths(Path[] dataPaths) {
        return Arrays.stream(dataPaths).map(FesenNodeCommand::createNodePath).toArray(NodeEnvironment.NodePath[]::new);
    }

    private static NodeEnvironment.NodePath createNodePath(Path path) {
        try {
            return new NodeEnvironment.NodePath(path);
        } catch (IOException e) {
            throw new FesenException("Unable to investigate path [" + path + "]", e);
        }
    }

    //package-private for testing
    OptionParser getParser() {
        return parser;
    }

    public static class UnknownMetadataCustom implements Metadata.Custom {

        private final String name;
        private final Map<String, Object> contents;

        public UnknownMetadataCustom(String name, Map<String, Object> contents) {
            this.name = name;
            this.contents = contents;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.mapContents(contents);
        }
    }

    public static class UnknownCondition extends Condition<Object> {

        public UnknownCondition(String name, Object value) {
            super(name);
            this.value = value;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(name, value);
        }

        @Override
        public Result evaluate(Stats stats) {
            assert false;
            throw new UnsupportedOperationException();
        }
    }
}
