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

package org.codelibs.fesen.rest.action.admin.cluster;

import static org.hamcrest.Matchers.nullValue;

import org.codelibs.fesen.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.codelibs.fesen.common.xcontent.DeprecationHandler;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.common.xcontent.XContentParser;
import org.codelibs.fesen.common.xcontent.XContentType;
import org.codelibs.fesen.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.codelibs.fesen.test.ESTestCase;

public class RestReloadSecureSettingsActionTests extends ESTestCase {

    public void testParserWithPassword() throws Exception {
        final String request = "{" + "\"secure_settings_password\": \"secure_settings_password_string\"" + "}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, request)) {
            NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertEquals("secure_settings_password_string", reloadSecureSettingsRequest.getSecureSettingsPassword().toString());
        }
    }

    public void testParserWithoutPassword() throws Exception {
        final String request = "{" + "}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, request)) {
            NodesReloadSecureSettingsRequest reloadSecureSettingsRequest = RestReloadSecureSettingsAction.PARSER.parse(parser, null);
            assertThat(reloadSecureSettingsRequest.getSecureSettingsPassword(), nullValue());
        }
    }
}
