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

package org.codelibs.fesen.index.reindex;

import java.util.Collections;

import org.codelibs.fesen.action.support.PlainActionFuture;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractAsyncBulkByScrollActionTestCase<Request extends AbstractBulkByScrollRequest<Request>, Response extends BulkByScrollResponse>
        extends ESTestCase {
    protected ThreadPool threadPool;
    protected BulkByScrollTask task;

    @Before
    public void setupForTest() {
        threadPool = new TestThreadPool(getTestName());
        task = new BulkByScrollTask(1, "test", "test", "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        task.setWorker(Float.POSITIVE_INFINITY, null);

    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    protected abstract Request request();

    protected PlainActionFuture<Response> listener() {
        return new PlainActionFuture<>();
    }
}
