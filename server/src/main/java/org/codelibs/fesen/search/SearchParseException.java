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

package org.codelibs.fesen.search;

import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.common.xcontent.XContentLocation;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.rest.RestStatus;

import java.io.IOException;

public class SearchParseException extends SearchException {

    public static final int UNKNOWN_POSITION = -1;
    private final int lineNumber;
    private final int columnNumber;

    public SearchParseException(SearchShardTarget shardTarget, String msg, @Nullable XContentLocation location) {
        this(shardTarget, msg, location, null);
    }

    public SearchParseException(SearchShardTarget shardTarget, String msg, @Nullable XContentLocation location, Throwable cause) {
        super(shardTarget, msg, cause);
        int lineNumber = UNKNOWN_POSITION;
        int columnNumber = UNKNOWN_POSITION;
        if (location != null) {
            if (location != null) {
                lineNumber = location.lineNumber;
                columnNumber = location.columnNumber;
            }
        }
        this.columnNumber = columnNumber;
        this.lineNumber = lineNumber;
    }

    public SearchParseException(StreamInput in) throws IOException {
        super(in);
        lineNumber = in.readInt();
        columnNumber = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(lineNumber);
        out.writeInt(columnNumber);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        if (lineNumber != UNKNOWN_POSITION) {
            builder.field("line", lineNumber);
            builder.field("col", columnNumber);
        }
    }

    /**
     * Line number of the location of the error
     *
     * @return the line number or -1 if unknown
     */
    public int getLineNumber() {
        return lineNumber;
    }

    /**
     * Column number of the location of the error
     *
     * @return the column number or -1 if unknown
     */
    public int getColumnNumber() {
        return columnNumber;
    }
}
