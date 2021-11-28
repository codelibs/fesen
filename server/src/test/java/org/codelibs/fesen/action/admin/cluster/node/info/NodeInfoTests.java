/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.action.admin.cluster.node.info;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.codelibs.fesen.Build;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.admin.cluster.node.info.NodeInfo;
import org.codelibs.fesen.cluster.node.DiscoveryNode;
import org.codelibs.fesen.monitor.jvm.JvmInfo;
import org.codelibs.fesen.monitor.os.OsInfo;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.VersionUtils;

/**
 * Tests for {@link NodeInfo}. Serialization and deserialization tested in
 * {@link org.codelibs.fesen.nodesinfo.NodeInfoStreamingTests}.
 */
public class NodeInfoTests extends ESTestCase {

    /**
     * Check that the the {@link NodeInfo#getInfo(Class)} method returns null
     * for absent info objects, and returns the right thing for present info
     * objects.
     */
    public void testGetInfo() {
        NodeInfo nodeInfo = new NodeInfo(Version.CURRENT, Build.CURRENT,
                new DiscoveryNode("test_node", buildNewFakeTransportAddress(), emptyMap(), emptySet(),
                        VersionUtils.randomVersion(random())),
                null, null, null, JvmInfo.jvmInfo(), null, null, null, null, null, null, null);

        // OsInfo is absent
        assertThat(nodeInfo.getInfo(OsInfo.class), nullValue());

        // JvmInfo is present
        assertThat(nodeInfo.getInfo(JvmInfo.class), notNullValue());
    }
}
