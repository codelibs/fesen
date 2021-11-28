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
import org.codelibs.fesen.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.mapper.ContentPath;
import org.codelibs.fesen.index.mapper.Mapper;
import org.codelibs.fesen.test.ESTestCase;

public class MapperTests extends ESTestCase {

    public void testSuccessfulBuilderContext() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        ContentPath contentPath = new ContentPath(1);
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, contentPath);

        assertEquals(settings, context.indexSettings());
        assertEquals(contentPath, context.path());
    }

    public void testBuilderContextWithIndexSettingsAsNull() {
        expectThrows(NullPointerException.class, () -> new Mapper.BuilderContext(null, new ContentPath(1)));
    }

}
