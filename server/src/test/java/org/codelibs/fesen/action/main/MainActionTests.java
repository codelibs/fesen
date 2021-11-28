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

package org.codelibs.fesen.action.main;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.main.MainRequest;
import org.codelibs.fesen.action.main.MainResponse;
import org.codelibs.fesen.action.main.TransportMainAction;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.cluster.ClusterState;
import org.codelibs.fesen.cluster.block.ClusterBlock;
import org.codelibs.fesen.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.cluster.block.ClusterBlocks;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.rest.RestStatus;
import org.codelibs.fesen.tasks.Task;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.transport.Transport;
import org.codelibs.fesen.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MainActionTests extends ESTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("fesen");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 400",
                        randomBoolean(), randomBoolean(), false, RestStatus.BAD_REQUEST, ClusterBlockLevel.ALL)).build();
            }
        } else {
            blocks = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 503",
                    randomBoolean(), randomBoolean(), false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL)).build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportMainAction action = new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        assertNotNull(responseRef.get());
        verify(clusterService, times(1)).state();
    }
}
