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

package org.codelibs.fesen.index.mapper;

import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsResponse;
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.XContentBuilder;
import org.codelibs.fesen.index.mapper.StrictDynamicMappingException;
import org.codelibs.fesen.test.ESSingleNodeTestCase;

import static org.codelibs.fesen.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

public class LegacyDynamicMappingTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_3_0).build();
        try (XContentBuilder mapping = jsonBuilder()) {
            mapping.startObject();
            {
                mapping.startObject("_default_");
                {
                    mapping.field("dynamic", "strict");
                }
                mapping.endObject();
            }
            mapping.endObject();
            createIndex("test", settings, "_default_", mapping);
        }
        try (XContentBuilder sourceBuilder = jsonBuilder().startObject().field("test", "test").endObject()) {
            expectThrows(StrictDynamicMappingException.class, () -> client()
                    .prepareIndex()
                    .setIndex("test")
                    .setType("type")
                    .setSource(sourceBuilder)
                    .get());

            GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
            assertNull(getMappingsResponse.getMappings().get("test").get("type"));
        }
    }

}
