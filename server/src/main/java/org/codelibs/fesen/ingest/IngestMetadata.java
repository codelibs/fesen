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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.cluster.Diff;
import org.codelibs.fesen.cluster.DiffableUtils;
import org.codelibs.fesen.cluster.NamedDiff;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.common.ParseField;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.ObjectParser;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentParser;

/**
 * Holds the ingest pipelines that are available in the cluster
 */
public final class IngestMetadata implements Metadata.Custom {

    public static final String TYPE = "ingest";
    private static final ParseField PIPELINES_FIELD = new ParseField("pipeline");
    private static final ObjectParser<List<PipelineConfiguration>, Void> INGEST_METADATA_PARSER =
            new ObjectParser<>("ingest_metadata", ArrayList::new);

    static {
        INGEST_METADATA_PARSER.declareObjectArray(List::addAll, PipelineConfiguration.getParser(), PIPELINES_FIELD);
    }

    // We can't use Pipeline class directly in cluster state, because we don't have the processor factories around when
    // IngestMetadata is registered as custom metadata.
    private final Map<String, PipelineConfiguration> pipelines;

    private IngestMetadata() {
        this.pipelines = Collections.emptyMap();
    }

    public IngestMetadata(Map<String, PipelineConfiguration> pipelines) {
        this.pipelines = Collections.unmodifiableMap(pipelines);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public Map<String, PipelineConfiguration> getPipelines() {
        return pipelines;
    }

    public IngestMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, PipelineConfiguration> pipelines = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            PipelineConfiguration pipeline = PipelineConfiguration.readFrom(in);
            pipelines.put(pipeline.getId(), pipeline);
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pipelines.size());
        for (PipelineConfiguration pipeline : pipelines.values()) {
            pipeline.writeTo(out);
        }
    }

    public static IngestMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        List<PipelineConfiguration> configs = INGEST_METADATA_PARSER.parse(parser, null);
        for (PipelineConfiguration pipeline : configs) {
            pipelines.put(pipeline.getId(), pipeline);
        }
        return new IngestMetadata(pipelines);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(PIPELINES_FIELD.getPreferredName());
        for (PipelineConfiguration pipeline : pipelines.values()) {
            pipeline.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new IngestMetadataDiff((IngestMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new IngestMetadataDiff(in);
    }

    static class IngestMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, PipelineConfiguration>> pipelines;

        IngestMetadataDiff(IngestMetadata before, IngestMetadata after) {
            this.pipelines = DiffableUtils.diff(before.pipelines, after.pipelines, DiffableUtils.getStringKeySerializer());
        }

        IngestMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), PipelineConfiguration::readFrom,
                    PipelineConfiguration::readDiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new IngestMetadata(pipelines.apply(((IngestMetadata) part).pipelines));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pipelines.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IngestMetadata that = (IngestMetadata) o;

        return pipelines.equals(that.pipelines);

    }

    @Override
    public int hashCode() {
        return pipelines.hashCode();
    }
}
