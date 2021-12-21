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

package org.codelibs.fesen.indices.recovery;

import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.settings.ClusterSettings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.seqno.SequenceNumbers;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.shard.IndexShardTestCase;
import org.codelibs.fesen.index.store.Store;
import org.codelibs.fesen.indices.IndicesService;
import org.codelibs.fesen.indices.recovery.DelayRecoveryException;
import org.codelibs.fesen.indices.recovery.PeerRecoverySourceService;
import org.codelibs.fesen.indices.recovery.RecoverySettings;
import org.codelibs.fesen.indices.recovery.RecoverySourceHandler;
import org.codelibs.fesen.indices.recovery.StartRecoveryRequest;
import org.codelibs.fesen.test.NodeRoles;
import org.codelibs.fesen.transport.TransportService;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerRecoverySourceServiceTests extends IndexShardTestCase {

    public void testDuplicateRecoveries() throws IOException {
        IndexShard primary = newStartedShard(true);
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(NodeRoles.dataNode());
        when(indicesService.clusterService()).thenReturn(clusterService);
        PeerRecoverySourceService peerRecoverySourceService = new PeerRecoverySourceService(
            mock(TransportService.class), indicesService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        StartRecoveryRequest startRecoveryRequest = new StartRecoveryRequest(primary.shardId(), randomAlphaOfLength(10),
            getFakeDiscoNode("source"), getFakeDiscoNode("target"), Store.MetadataSnapshot.EMPTY, randomBoolean(), randomLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO);
        peerRecoverySourceService.start();
        RecoverySourceHandler handler = peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary);
        DelayRecoveryException delayRecoveryException = expectThrows(DelayRecoveryException.class,
            () -> peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary));
        assertThat(delayRecoveryException.getMessage(), containsString("recovery with same target already registered"));
        peerRecoverySourceService.ongoingRecoveries.remove(primary, handler);
        // re-adding after removing previous attempt works
        handler = peerRecoverySourceService.ongoingRecoveries.addNewRecovery(startRecoveryRequest, primary);
        peerRecoverySourceService.ongoingRecoveries.remove(primary, handler);
        closeShards(primary);
    }
}
