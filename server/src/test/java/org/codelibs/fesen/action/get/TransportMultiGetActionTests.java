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

package org.codelibs.fesen.action.get;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.IndicesRequest;
import org.codelibs.fesen.action.RoutingMissingException;
import org.codelibs.fesen.action.get.MultiGetAction;
import org.codelibs.fesen.action.get.MultiGetItemResponse;
import org.codelibs.fesen.action.get.MultiGetRequest;
import org.codelibs.fesen.action.get.MultiGetRequestBuilder;
import org.codelibs.fesen.action.get.MultiGetResponse;
import org.codelibs.fesen.action.get.MultiGetShardRequest;
import org.codelibs.fesen.action.get.MultiGetShardResponse;
import org.codelibs.fesen.action.get.TransportMultiGetAction;
import org.codelibs.fesen.action.get.TransportShardMultiGetAction;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.metadata.Metadata;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.cluster.routing.OperationRouting;
import org.codelibs.fesen.cluster.routing.ShardIterator;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.concurrent.AtomicArray;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.common.xcontent.XContentFactory;
import org.codelibs.fesen.common.xcontent.XContentHelper;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.index.Index;
import org.codelibs.fesen.index.shard.ShardId;
import org.codelibs.fesen.indices.IndicesService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.tasks.TaskId;
import org.codelibs.fesen.tasks.TaskManager;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.codelibs.fesen.common.UUIDs.randomBase64UUID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiGetActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static TransportService transportService;
    private static ClusterService clusterService;
    private static TransportMultiGetAction transportAction;
    private static TransportShardMultiGetAction shardAction;

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(TransportMultiGetActionTests.class.getSimpleName());

        transportService = new TransportService(Settings.EMPTY, mock(Transport.class), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.builder().put("node.name", "node1").build(),
                boundAddress.publishAddress(), randomBase64UUID()), null, emptySet()) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };

        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportMultiGetActionTests.class.getSimpleName()))
            .metadata(new Metadata.Builder()
                .put(new IndexMetadata.Builder(index1.getName())
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID()))
                    .putMapping("_doc",
                        XContentHelper.convertToJson(BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                                .startObject("_doc")
                                    .startObject("_routing")
                                        .field("required", false)
                                    .endObject()
                                .endObject()
                            .endObject()), true, XContentType.JSON)))
                .put(new IndexMetadata.Builder(index2.getName())
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID()))
                    .putMapping("_doc",
                        XContentHelper.convertToJson(BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                                .startObject("_doc")
                                    .startObject("_routing")
                                        .field("required", true)
                                    .endObject()
                                .endObject()
                            .endObject()), true, XContentType.JSON)))).build();

        final ShardIterator index1ShardIterator = mock(ShardIterator.class);
        when(index1ShardIterator.shardId()).thenReturn(new ShardId(index1, randomInt()));

        final ShardIterator index2ShardIterator = mock(ShardIterator.class);
        when(index2ShardIterator.shardId()).thenReturn(new ShardId(index2, randomInt()));

        final OperationRouting operationRouting = mock(OperationRouting.class);
        when(operationRouting.getShards(eq(clusterState), eq(index1.getName()), anyString(), anyString(), anyString()))
            .thenReturn(index1ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index1.getName()), anyString(), anyString()))
            .thenReturn(new ShardId(index1, randomInt()));
        when(operationRouting.getShards(eq(clusterState), eq(index2.getName()), anyString(), anyString(), anyString()))
            .thenReturn(index2ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index2.getName()), anyString(), anyString()))
            .thenReturn(new ShardId(index2, randomInt()));

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        shardAction = new TransportShardMultiGetAction(clusterService, transportService, mock(IndicesService.class), threadPool,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void doExecute(Task task, MultiGetShardRequest request, ActionListener<MultiGetShardResponse> listener) {
            }
        };
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        transportService = null;
        clusterService = null;
        transportAction = null;
        shardAction = null;
    }

    public void testTransportMultiGetAction() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index1", "_doc", "1"));
        request.add(new MultiGetRequest.Item("index1", "_doc", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(transportService, clusterService, shardAction,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void executeShardAction(final ActionListener<MultiGetResponse> listener,
                                              final AtomicArray<MultiGetItemResponse> responses,
                                              final Map<ShardId, MultiGetShardRequest> shardRequests) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertNull(responses.get(1));
            }
        };

        transportAction.execute(task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());
    }

    public void testTransportMultiGetAction_withMissingRouting() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index2", "_doc", "1").routing("1"));
        request.add(new MultiGetRequest.Item("index2", "_doc", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(transportService, clusterService, shardAction,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void executeShardAction(final ActionListener<MultiGetResponse> listener,
                                              final AtomicArray<MultiGetItemResponse> responses,
                                              final Map<ShardId, MultiGetShardRequest> shardRequests) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertThat(responses.get(1).getFailure().getFailure(), instanceOf(RoutingMissingException.class));
                assertThat(responses.get(1).getFailure().getFailure().getMessage(),
                    equalTo("routing is required for [index2]/[_doc]/[2]"));
            }
        };

        transportAction.execute(task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());

    }

    private static Task createTask() {
        return new Task(randomLong(), "transport", MultiGetAction.NAME, "description",
            new TaskId(randomLong() + ":" + randomLong()), emptyMap());
    }

    static class Resolver extends IndexNameExpressionResolver {

        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
            return new Index("index1", randomBase64UUID());
        }
    }

    static class ActionListenerAdapter implements ActionListener<MultiGetResponse> {

        @Override
        public void onResponse(MultiGetResponse response) {
        }

        @Override
        public void onFailure(Exception e) {
        }
    }
}
