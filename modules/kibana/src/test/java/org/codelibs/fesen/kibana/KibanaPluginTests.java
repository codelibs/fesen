/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.kibana;

import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.indices.SystemIndexDescriptor;
import org.codelibs.fesen.kibana.KibanaPlugin;
import org.codelibs.fesen.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class KibanaPluginTests extends ESTestCase {

    public void testKibanaIndexNames() {
        assertThat(new KibanaPlugin().getSettings(), contains(KibanaPlugin.KIBANA_INDEX_NAMES_SETTING));
        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .map(SystemIndexDescriptor::getIndexPattern)
                .collect(Collectors.toList()),
            contains(".kibana", ".kibana_*", ".reporting-*", ".apm-agent-configuration", ".apm-custom-link")
        );
        final List<String> names = Collections.unmodifiableList(Arrays.asList("." + randomAlphaOfLength(4), "." + randomAlphaOfLength(5)));
        final List<String> namesFromDescriptors = new KibanaPlugin().getSystemIndexDescriptors(
            Settings.builder().putList(KibanaPlugin.KIBANA_INDEX_NAMES_SETTING.getKey(), names).build()
        ).stream().map(SystemIndexDescriptor::getIndexPattern).collect(Collectors.toList());
        assertThat(namesFromDescriptors, is(names));

        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .anyMatch(systemIndexDescriptor -> systemIndexDescriptor.matchesIndexPattern(".kibana-event-log-7-1")),
            is(false)
        );
    }
}
