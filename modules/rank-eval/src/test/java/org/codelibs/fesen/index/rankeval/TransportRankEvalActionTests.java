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

package org.codelibs.fesen.index.rankeval;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.search.MultiSearchRequest;
import org.codelibs.fesen.action.search.MultiSearchResponse;
import org.codelibs.fesen.action.search.SearchType;
import org.codelibs.fesen.action.support.ActionFilters;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.script.ScriptService;
import org.codelibs.fesen.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.transport.TransportService;

public class TransportRankEvalActionTests extends ESTestCase {

    private Settings settings = Settings.builder().put("path.home", createTempDir().toString()).put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

    /**
     * Test that request parameters like indicesOptions or searchType from ranking evaluation request are transfered to msearch request
     */
    public void testTransferRequestParameters() throws Exception {
        String indexName = "test_index";
        List<RatedRequest> specifications = new ArrayList<>();
        specifications
                .add(new RatedRequest("amsterdam_query", Arrays.asList(new RatedDocument(indexName, "1", 3)), new SearchSourceBuilder()));
        RankEvalRequest rankEvalRequest =
                new RankEvalRequest(new RankEvalSpec(specifications, new DiscountedCumulativeGain()), new String[] { indexName });
        SearchType expectedSearchType = randomFrom(SearchType.CURRENTLY_SUPPORTED);
        rankEvalRequest.searchType(expectedSearchType);
        IndicesOptions expectedIndicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        rankEvalRequest.indicesOptions(expectedIndicesOptions);

        NodeClient client = new NodeClient(settings, null) {
            @Override
            public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                assertEquals(1, request.requests().size());
                assertEquals(expectedSearchType, request.requests().get(0).searchType());
                assertArrayEquals(new String[] { indexName }, request.requests().get(0).indices());
                assertEquals(expectedIndicesOptions, request.requests().get(0).indicesOptions());
            }
        };

        TransportRankEvalAction action = new TransportRankEvalAction(mock(ActionFilters.class), client, mock(TransportService.class),
                mock(ScriptService.class), NamedXContentRegistry.EMPTY);
        action.doExecute(null, rankEvalRequest, null);
    }
}
