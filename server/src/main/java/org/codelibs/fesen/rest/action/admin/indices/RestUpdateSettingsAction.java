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

package org.codelibs.fesen.rest.action.admin.indices;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.codelibs.fesen.client.Requests.updateSettingsRequest;
import static org.codelibs.fesen.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.codelibs.fesen.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.codelibs.fesen.action.support.IndicesOptions;
import org.codelibs.fesen.client.node.NodeClient;
import org.codelibs.fesen.common.Strings;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.rest.BaseRestHandler;
import org.codelibs.fesen.rest.RestRequest;
import org.codelibs.fesen.rest.action.RestToXContentListener;

public class RestUpdateSettingsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(PUT, "/{index}/_settings"), new Route(PUT, "/_settings")));
    }

    @Override
    public String getName() {
        return "update_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        UpdateSettingsRequest updateSettingsRequest = updateSettingsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        updateSettingsRequest.timeout(request.paramAsTime("timeout", updateSettingsRequest.timeout()));
        updateSettingsRequest.setPreserveExisting(request.paramAsBoolean("preserve_existing", updateSettingsRequest.isPreserveExisting()));
        updateSettingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", updateSettingsRequest.masterNodeTimeout()));
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()));
        updateSettingsRequest.fromXContent(request.contentParser());

        return channel -> client.admin().indices().updateSettings(updateSettingsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }
}
