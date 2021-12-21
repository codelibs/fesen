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

import org.apache.lucene.util.SetOnce;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.network.NetworkModule;
import org.codelibs.fesen.common.network.NetworkService;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.BigArrays;
import org.codelibs.fesen.common.util.PageCacheRecycler;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.http.HttpServerTransport;
import org.codelibs.fesen.http.netty4.Netty4HttpServerTransport;
import org.codelibs.fesen.indices.breaker.CircuitBreakerService;
import org.codelibs.fesen.plugins.NetworkPlugin;
import org.codelibs.fesen.plugins.Plugin;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.netty4.Netty4Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Netty4Plugin extends Plugin implements NetworkPlugin {

    public static final String NETTY_TRANSPORT_NAME = "netty4";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty4";

    private final SetOnce<SharedGroupFactory> groupFactory = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty4HttpServerTransport.SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.WORKER_COUNT,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty4Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty4Transport.NETTY_BOSS_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
                // here we set the netty4 transport and http transport as the default. This is a set once setting
                // ie. if another plugin does that as well the server will fail - only one default network can exist!
                .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NETTY_HTTP_TRANSPORT_NAME)
                .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NETTY_TRANSPORT_NAME)
                .build();
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        return Collections.singletonMap(NETTY_TRANSPORT_NAME, () -> new Netty4Transport(settings, Version.CURRENT, threadPool,
            networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, getSharedGroupFactory(settings)));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        PageCacheRecycler pageCacheRecycler,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher,
                                                                        ClusterSettings clusterSettings) {
        return Collections.singletonMap(NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher,
                clusterSettings, getSharedGroupFactory(settings)));
    }

    private SharedGroupFactory getSharedGroupFactory(Settings settings) {
        SharedGroupFactory groupFactory = this.groupFactory.get();
        if (groupFactory != null) {
            assert groupFactory.getSettings().equals(settings) : "Different settings than originally provided";
            return groupFactory;
        } else {
            this.groupFactory.set(new SharedGroupFactory(settings));
            return this.groupFactory.get();
        }
    }
}
