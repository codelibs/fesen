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

package org.codelibs.fesen.common.settings;

import org.codelibs.fesen.cli.Command;
import org.codelibs.fesen.cli.UserException;
import org.codelibs.fesen.common.settings.KeyStoreWrapper;
import org.codelibs.fesen.common.settings.UpgradeKeyStoreCommand;
import org.codelibs.fesen.env.Environment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;

public class UpgradeKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new UpgradeKeyStoreCommand() {

            @Override
            protected Environment createEnv(final Map<String, String> settings) {
                return env;
            }

        };
    }

    /*public void testKeystoreUpgrade() throws Exception {
        final Path keystore = KeyStoreWrapper.keystorePath(env.configFile());
        try (
            InputStream is = KeyStoreWrapperTests.class.getResourceAsStream("/format-v3-fesen.keystore");
            OutputStream os = Files.newOutputStream(keystore)
        ) {
            final byte[] buffer = new byte[4096];
            int read;
            while ((read = is.read(buffer, 0, buffer.length)) >= 0) {
                os.write(buffer, 0, read);
            }
        }
        try (KeyStoreWrapper beforeUpgrade = KeyStoreWrapper.load(env.configFile())) {
            assertNotNull(beforeUpgrade);
            assertThat(beforeUpgrade.getFormatVersion(), equalTo(3));
        }
        execute();
        try (KeyStoreWrapper afterUpgrade = KeyStoreWrapper.load(env.configFile())) {
            assertNotNull(afterUpgrade);
            assertThat(afterUpgrade.getFormatVersion(), equalTo(KeyStoreWrapper.FORMAT_VERSION));
            afterUpgrade.decrypt(new char[0]);
            assertThat(afterUpgrade.getSettingNames(), hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        }
    }*/ // FESEN skip

    public void testKeystoreDoesNotExist() {
        final UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e, hasToString(containsString("keystore not found at [" + KeyStoreWrapper.keystorePath(env.configFile()) + "]")));
    }

}
