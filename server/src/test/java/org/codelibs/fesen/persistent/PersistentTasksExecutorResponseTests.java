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
package org.codelibs.fesen.persistent;

import org.codelibs.fesen.common.UUIDs;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.io.stream.Writeable;
import org.codelibs.fesen.persistent.PersistentTaskParams;
import org.codelibs.fesen.persistent.PersistentTaskResponse;
import org.codelibs.fesen.persistent.PersistentTasksCustomMetadata;
import org.codelibs.fesen.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.codelibs.fesen.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.codelibs.fesen.test.AbstractWireSerializingTestCase;

import java.util.Collections;


public class PersistentTasksExecutorResponseTests extends AbstractWireSerializingTestCase<PersistentTaskResponse> {

    @Override
    protected PersistentTaskResponse createTestInstance() {
        if (randomBoolean()) {
            return new PersistentTaskResponse(
                    new PersistentTask<PersistentTaskParams>(UUIDs.base64UUID(), TestPersistentTasksExecutor.NAME,
                            new TestPersistentTasksPlugin.TestParams("test"),
                            randomLong(), PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT));
        } else {
            return new PersistentTaskResponse((PersistentTask<?>) null);
        }
    }

    @Override
    protected Writeable.Reader<PersistentTaskResponse> instanceReader() {
        return PersistentTaskResponse::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class,
                        TestPersistentTasksExecutor.NAME, TestPersistentTasksPlugin.TestParams::new)
        ));
    }
}
