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

package org.codelibs.fesen.search.fetch;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.codelibs.fesen.action.search.SearchShardTask;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.common.lucene.Lucene;
import org.codelibs.fesen.core.Nullable;
import org.codelibs.fesen.search.RescoreDocIds;
import org.codelibs.fesen.search.dfs.AggregatedDfs;
import org.codelibs.fesen.search.internal.ShardSearchContextId;
import org.codelibs.fesen.search.internal.ShardSearchRequest;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Shard level fetch base request. Holds all the info needed to execute a fetch.
 * Used with search scroll as the original request doesn't hold indices.
 */
public class ShardFetchRequest extends TransportRequest {

    private ShardSearchContextId contextId;

    private int[] docIds;

    private int size;

    private ScoreDoc lastEmittedDoc;

    public ShardFetchRequest(ShardSearchContextId contextId, IntArrayList list, ScoreDoc lastEmittedDoc) {
        this.contextId = contextId;
        this.docIds = list.buffer;
        this.size = list.size();
        this.lastEmittedDoc = lastEmittedDoc;
    }

    public ShardFetchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        size = in.readVInt();
        docIds = new int[size];
        for (int i = 0; i < size; i++) {
            docIds[i] = in.readVInt();
        }
        byte flag = in.readByte();
        if (flag == 1) {
            lastEmittedDoc = Lucene.readFieldDoc(in);
        } else if (flag == 2) {
            lastEmittedDoc = Lucene.readScoreDoc(in);
        } else if (flag != 0) {
            throw new IOException("Unknown flag: " + flag);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        out.writeVInt(size);
        for (int i = 0; i < size; i++) {
            out.writeVInt(docIds[i]);
        }
        if (lastEmittedDoc == null) {
            out.writeByte((byte) 0);
        } else if (lastEmittedDoc instanceof FieldDoc) {
            out.writeByte((byte) 1);
            Lucene.writeFieldDoc(out, (FieldDoc) lastEmittedDoc);
        } else {
            out.writeByte((byte) 2);
            Lucene.writeScoreDoc(out, lastEmittedDoc);
        }
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public int[] docIds() {
        return docIds;
    }

    public int docIdsSize() {
        return size;
    }

    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + contextId + "], size[" + size + "], lastEmittedDoc[" + lastEmittedDoc + "]";
    }

    @Nullable
    public ShardSearchRequest getShardSearchRequest() {
        return null;
    }

    @Nullable
    public RescoreDocIds getRescoreDocIds() {
        return null;
    }

    @Nullable
    public AggregatedDfs getAggregatedDfs() {
        return null;
    }
}
