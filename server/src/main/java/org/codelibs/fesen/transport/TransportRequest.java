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

package org.codelibs.fesen.transport;

import java.io.IOException;

import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;
import org.codelibs.fesen.tasks.TaskAwareRequest;
import org.codelibs.fesen.tasks.TaskId;

public abstract class TransportRequest extends TransportMessage implements TaskAwareRequest {
    public static class Empty extends TransportRequest {
        public static final Empty INSTANCE = new Empty();

        public Empty() {}

        public Empty(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
     */
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    public TransportRequest() {
    }

    public TransportRequest(StreamInput in) throws IOException {
        parentTaskId = TaskId.readFromStream(in);
    }

    /**
     * Set a reference to task that created this request.
     */
    @Override
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    @Override
    public TaskId getParentTask() {
        return parentTaskId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        parentTaskId.writeTo(out);
    }
}