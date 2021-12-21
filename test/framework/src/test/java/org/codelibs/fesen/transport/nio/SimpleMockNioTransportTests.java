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

package org.codelibs.fesen.transport.nio;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.network.NetworkService;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.transport.TransportAddress;
import org.codelibs.fesen.common.util.MockPageCacheRecycler;
import org.codelibs.fesen.indices.breaker.NoneCircuitBreakerService;
import org.codelibs.fesen.transport.AbstractSimpleTransportTestCase;
import org.codelibs.fesen.transport.ConnectTransportException;
import org.codelibs.fesen.transport.ConnectionProfile;
import org.codelibs.fesen.transport.TcpChannel;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.nio.MockNioTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SimpleMockNioTransportTests extends AbstractSimpleTransportTestCase {

    @Override
    protected Transport build(Settings settings, final Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        return new MockNioTransport(settings, version, threadPool, networkService, new MockPageCacheRecycler(settings),
            namedWriteableRegistry, new NoneCircuitBreakerService()) {

            @Override
            public void executeHandshake(DiscoveryNode node, TcpChannel channel, ConnectionProfile profile,
                                         ActionListener<Version> listener) {
                if (doHandshake) {
                    super.executeHandshake(node, channel, profile, listener);
                } else {
                    listener.onResponse(version.minimumCompatibilityVersion());
                }
            }
        };
    }

    @Override
    protected int channelsPerNodeConnection() {
        return 3;
    }

    public void testConnectException() throws UnknownHostException {
        try {
            serviceA.connectToNode(new DiscoveryNode("C", new TransportAddress(InetAddress.getByName("localhost"), 9876),
                emptyMap(), emptySet(),Version.CURRENT));
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IOException.class));
        }
    }
}
