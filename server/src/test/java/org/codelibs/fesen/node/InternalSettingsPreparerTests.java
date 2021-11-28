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

package org.codelibs.fesen.node;

import org.codelibs.fesen.cluster.ClusterName;
import org.codelibs.fesen.common.settings.MockSecureSettings;
import org.codelibs.fesen.common.settings.SecureSetting;
import org.codelibs.fesen.common.settings.SecureString;
import org.codelibs.fesen.common.settings.Setting;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.settings.SettingsException;
import org.codelibs.fesen.env.Environment;
import org.codelibs.fesen.node.InternalSettingsPreparer;
import org.codelibs.fesen.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public class InternalSettingsPreparerTests extends ESTestCase {
    private static final Supplier<String> DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED = () -> {
        throw new AssertionError("shouldn't be called");
    };

    Path homeDir;
    Settings baseEnvSettings;

    @Before
    public void createBaseEnvSettings() {
        homeDir = createTempDir();
        baseEnvSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), homeDir).build();
    }

    @After
    public void clearBaseEnvSettings() {
        homeDir = null;
        baseEnvSettings = null;
    }

    public void testEmptySettings() {
        Settings settings = InternalSettingsPreparer.prepareSettings(Settings.EMPTY);
        assertNull(settings.get("node.name"));
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        int size = settings.names().size();

        String defaultNodeName = randomAlphaOfLength(8);
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, emptyMap(), null, () -> defaultNodeName);
        settings = env.settings();
        assertEquals(defaultNodeName, settings.get("node.name"));
        assertNotNull(settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey())); // a cluster name was set
        assertEquals(settings.toString(), size + 1 /* path.home is in the base settings */, settings.names().size());
        String home = Environment.PATH_HOME_SETTING.get(baseEnvSettings);
        String configDir = env.configFile().toString();
        assertTrue(configDir, configDir.startsWith(home));
    }

    public void testDefaultClusterName() {
        Settings settings = InternalSettingsPreparer.prepareSettings(Settings.EMPTY);
        assertEquals("fesen", settings.get("cluster.name"));
        settings = InternalSettingsPreparer.prepareSettings(Settings.builder().put("cluster.name", "foobar").build());
        assertEquals("foobar", settings.get("cluster.name"));
    }

    public void testGarbageIsNotSwallowed() throws IOException {
        try {
            InputStream garbage = getClass().getResourceAsStream("/config/garbage/garbage.yml");
            Path home = createTempDir();
            Path config = home.resolve("config");
            Files.createDirectory(config);
            Files.copy(garbage, config.resolve("fesen.yml"));
            InternalSettingsPreparer.prepareEnvironment(Settings.builder().put(baseEnvSettings).build(), emptyMap(), null,
                    () -> "default_node_name");
        } catch (SettingsException e) {
            assertEquals("Failed to load settings from [fesen.yml]", e.getMessage());
        }
    }

    public void testYamlNotAllowed() throws IOException {
        InputStream yaml = getClass().getResourceAsStream("/config/fesen.yml");
        Path config = homeDir.resolve("config");
        Files.createDirectory(config);
        Files.copy(yaml, config.resolve("fesen.yaml"));
        SettingsException e = expectThrows(SettingsException.class,
                () -> InternalSettingsPreparer.prepareEnvironment(Settings.builder().put(baseEnvSettings).build(), emptyMap(), null,
                        DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED));
        assertEquals("fesen.yaml was deprecated in 5.5.0 and must be renamed to fesen.yml", e.getMessage());
    }

    public void testJsonNotAllowed() throws IOException {
        InputStream yaml = getClass().getResourceAsStream("/config/fesen.json");
        Path config = homeDir.resolve("config");
        Files.createDirectory(config);
        Files.copy(yaml, config.resolve("fesen.json"));
        SettingsException e = expectThrows(SettingsException.class,
                () -> InternalSettingsPreparer.prepareEnvironment(Settings.builder().put(baseEnvSettings).build(), emptyMap(), null,
                        DEFAULT_NODE_NAME_SHOULDNT_BE_CALLED));
        assertEquals("fesen.json was deprecated in 5.5.0 and must be converted to fesen.yml", e.getMessage());
    }

    public void testSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "secret");
        Settings input = Settings.builder().put(baseEnvSettings).setSecureSettings(secureSettings).build();
        Environment env = InternalSettingsPreparer.prepareEnvironment(input, emptyMap(), null, () -> "default_node_name");
        Setting<SecureString> fakeSetting = SecureSetting.secureString("foo", null);
        assertEquals("secret", fakeSetting.get(env.settings()).toString());
    }

    public void testDefaultPropertiesDoNothing() throws Exception {
        Map<String, String> props = Collections.singletonMap("default.setting", "foo");
        Environment env = InternalSettingsPreparer.prepareEnvironment(baseEnvSettings, props, null, () -> "default_node_name");
        assertEquals("foo", env.settings().get("default.setting"));
        assertNull(env.settings().get("setting"));
    }

}
