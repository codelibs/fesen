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

import org.apache.lucene.util.SetOnce;
import org.codelibs.fesen.FesenSecurityException;
import org.codelibs.fesen.FesenStatusException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.action.admin.cluster.node.info.NodeInfo;
import org.codelibs.fesen.action.search.SearchAction;
import org.codelibs.fesen.action.support.ActionFilter;
import org.codelibs.fesen.action.support.ActionFilterChain;
import org.codelibs.fesen.action.support.WriteRequest.RefreshPolicy;
import org.codelibs.fesen.client.Client;
import org.codelibs.fesen.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.bytes.BytesArray;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.network.NetworkModule;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.transport.TransportAddress;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.env.NodeEnvironment;
import org.codelibs.fesen.http.HttpInfo;
import org.codelibs.fesen.index.reindex.ReindexAction;
import org.codelibs.fesen.index.reindex.ReindexPlugin;
import org.codelibs.fesen.index.reindex.ReindexRequestBuilder;
import org.codelibs.fesen.index.reindex.RemoteInfo;
import org.codelibs.fesen.index.reindex.TransportReindexAction;
import org.codelibs.fesen.plugins.ActionPlugin;
import org.codelibs.fesen.plugins.Plugin;
import org.codelibs.fesen.repositories.RepositoriesService;
import org.codelibs.fesen.rest.RestHeaderDefinition;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.test.ESSingleNodeTestCase;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.Netty4Plugin;
import org.codelibs.fesen.watcher.ResourceWatcherService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.codelibs.fesen.index.reindex.ReindexTestCase.matcher;
import static org.hamcrest.Matchers.containsString;

public class ReindexFromRemoteWithAuthTests extends ESSingleNodeTestCase {
    private TransportAddress address;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            Netty4Plugin.class,
            ReindexFromRemoteWithAuthTests.TestPlugin.class,
            ReindexPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings());
        // Whitelist reindexing from the http host we're going to use
        settings.put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:*");
        settings.put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME);
        return settings.build();
    }

    @Before
    public void setupSourceIndex() {
        client().prepareIndex("source", "test").setSource("test", "test").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
    }

    @Before
    public void fetchTransportAddress() {
        NodeInfo nodeInfo = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0);
        address = nodeInfo.getInfo(HttpInfo.class).getAddress().publishAddress();
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String username, String password, Map<String, String> headers) {
        return new RemoteInfo("http", address.getAddress(), address.getPort(), null,
            new BytesArray("{\"match_all\":{}}"), username, password, headers,
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
    }

    public void testReindexFromRemoteWithAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo("Aladdin", "open sesame", emptyMap()));
        assertThat(request.get(), matcher().created(1));
    }

    public void testReindexSendsHeaders() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo(null, null, singletonMap(TestFilter.EXAMPLE_HEADER, "doesn't matter")));
        FesenStatusException e = expectThrows(FesenStatusException.class, () -> request.get());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getMessage(), containsString("Hurray! Sent the header!"));
    }

    public void testReindexWithoutAuthenticationWhenRequired() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo(null, null, emptyMap()));
        FesenStatusException e = expectThrows(FesenStatusException.class, () -> request.get());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
        assertThat(e.getMessage(), containsString("\"reason\":\"Authentication required\""));
        assertThat(e.getMessage(), containsString("\"WWW-Authenticate\":\"Basic realm=auth-realm\""));
    }

    public void testReindexWithBadAuthentication() throws Exception {
        ReindexRequestBuilder request = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source").destination("dest")
                .setRemoteInfo(newRemoteInfo("junk", "auth", emptyMap()));
        FesenStatusException e = expectThrows(FesenStatusException.class, () -> request.get());
        assertThat(e.getMessage(), containsString("\"reason\":\"Bad Authorization\""));
    }

    /**
     * Plugin that demands authentication.
     */
    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final SetOnce<ReindexFromRemoteWithAuthTests.TestFilter> testFilter = new SetOnce<>();

        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                                   IndexNameExpressionResolver expressionResolver,
                                                   Supplier<RepositoriesService> repositoriesServiceSupplier) {
            testFilter.set(new ReindexFromRemoteWithAuthTests.TestFilter(threadPool));
            return Collections.emptyList();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(testFilter.get());
        }

        @Override
        public Collection<RestHeaderDefinition> getRestHeaders() {
            return Arrays.asList(new RestHeaderDefinition(TestFilter.AUTHORIZATION_HEADER, false),
                new RestHeaderDefinition(TestFilter.EXAMPLE_HEADER, false));
        }
    }

    /**
     * ActionType filter that will reject the request if it isn't authenticated.
     */
    public static class TestFilter implements ActionFilter {
        /**
         * The authorization required. Corresponds to username="Aladdin" and password="open sesame". It is the example in
         * <a href="https://tools.ietf.org/html/rfc1945#section-11.1">HTTP/1.0's RFC</a>.
         */
        private static final String REQUIRED_AUTH = "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        private static final String AUTHORIZATION_HEADER = "Authorization";
        private static final String EXAMPLE_HEADER = "Example-Header";
        private final ThreadContext context;

        public TestFilter(ThreadPool threadPool) {
            context = threadPool.getThreadContext();
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action,
                Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            if (false == action.equals(SearchAction.NAME)) {
                chain.proceed(task, action, request, listener);
                return;
            }
            if (context.getHeader(EXAMPLE_HEADER) != null) {
                throw new IllegalArgumentException("Hurray! Sent the header!");
            }
            String auth = context.getHeader(AUTHORIZATION_HEADER);
            if (auth == null) {
                FesenSecurityException e = new FesenSecurityException("Authentication required",
                        RestStatus.UNAUTHORIZED);
                e.addHeader("WWW-Authenticate", "Basic realm=auth-realm");
                throw e;
            }
            if (false == REQUIRED_AUTH.equals(auth)) {
                throw new FesenSecurityException("Bad Authorization", RestStatus.FORBIDDEN);
            }
            chain.proceed(task, action, request, listener);
        }
    }
}
