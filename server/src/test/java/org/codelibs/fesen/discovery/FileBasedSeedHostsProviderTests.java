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

package org.codelibs.fesen.discovery;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.common.io.stream.NamedWriteableRegistry;
import org.codelibs.fesen.common.network.NetworkService;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.transport.BoundTransportAddress;
import org.codelibs.fesen.common.transport.TransportAddress;
import org.codelibs.fesen.common.util.CancellableThreads;
import org.codelibs.fesen.common.util.PageCacheRecycler;
import org.codelibs.fesen.core.TimeValue;
import org.codelibs.fesen.discovery.FileBasedSeedHostsProvider;
import org.codelibs.fesen.discovery.SeedHostsResolver;
import org.codelibs.fesen.indices.breaker.NoneCircuitBreakerService;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.transport.MockTransportService;
import org.codelibs.fesen.threadpool.TestThreadPool;
import org.codelibs.fesen.threadpool.ThreadPool;
import org.codelibs.fesen.transport.TransportService;
import org.codelibs.fesen.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import static org.codelibs.fesen.discovery.FileBasedSeedHostsProvider.UNICAST_HOSTS_FILE;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileBasedSeedHostsProviderTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private MockTransportService transportService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(FileBasedSeedHostsProviderTests.class.getName());
        executorService = Executors.newSingleThreadExecutor();
        createTransportSvc();
    }

    @After
    public void tearDown() throws Exception {
        try {
            terminate(executorService);
        } finally {
            try {
                terminate(threadPool);
            } finally {
                super.tearDown();
            }
        }
    }

    private void createTransportSvc() {
        final MockNioTransport transport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool,
                new NetworkService(Collections.emptyList()), PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(Collections.emptyList()), new NoneCircuitBreakerService()) {
            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9300) },
                        new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
            }
        };
        transportService =
                new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
    }

    public void testBuildDynamicNodes() throws Exception {
        final List<String> hostEntries = Arrays.asList("#comment, should be ignored", "192.168.0.1", "192.168.0.2:9305", "255.255.23.15");
        final List<TransportAddress> nodes = setupAndRunHostProvider(hostEntries);
        assertEquals(hostEntries.size() - 1, nodes.size()); // minus 1 because we are ignoring the first line that's a comment
        assertEquals("192.168.0.1", nodes.get(0).getAddress());
        assertEquals(9300, nodes.get(0).getPort());
        assertEquals("192.168.0.2", nodes.get(1).getAddress());
        assertEquals(9305, nodes.get(1).getPort());
        assertEquals("255.255.23.15", nodes.get(2).getAddress());
        assertEquals(9300, nodes.get(2).getPort());
    }

    public void testEmptyUnicastHostsFile() throws Exception {
        final List<String> hostEntries = Collections.emptyList();
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testUnicastHostsDoesNotExist() {
        final FileBasedSeedHostsProvider provider = new FileBasedSeedHostsProvider(createTempDir().toAbsolutePath());
        final List<TransportAddress> addresses =
                provider.getSeedAddresses(hosts -> SeedHostsResolver.resolveHostsLists(new CancellableThreads(), executorService, logger,
                        hosts, transportService, TimeValue.timeValueSeconds(10)));
        assertEquals(0, addresses.size());
    }

    public void testInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Collections.singletonList("192.168.0.1:9300:9300");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testSomeInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Arrays.asList("192.168.0.1:9300:9300", "192.168.0.1:9301");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(1, addresses.size()); // only one of the two is valid and will be used
        assertEquals("192.168.0.1", addresses.get(0).getAddress());
        assertEquals(9301, addresses.get(0).getPort());
    }

    // sets up the config dir, writes to the unicast hosts file in the config dir,
    // and then runs the file-based unicast host provider to get the list of discovery nodes
    private List<TransportAddress> setupAndRunHostProvider(final List<String> hostEntries) throws IOException {
        final Path homeDir = createTempDir();
        final Path configPath = randomBoolean() ? homeDir.resolve("config") : createTempDir();
        Files.createDirectories(configPath);
        try (BufferedWriter writer = Files.newBufferedWriter(configPath.resolve(UNICAST_HOSTS_FILE))) {
            writer.write(String.join("\n", hostEntries));
        }

        return new FileBasedSeedHostsProvider(configPath)
                .getSeedAddresses(hosts -> SeedHostsResolver.resolveHostsLists(new CancellableThreads(), executorService, logger, hosts,
                        transportService, TimeValue.timeValueSeconds(10)));
    }
}
