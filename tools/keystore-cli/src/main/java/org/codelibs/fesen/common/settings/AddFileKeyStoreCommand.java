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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.codelibs.fesen.cli.ExitCodes;
import org.codelibs.fesen.cli.Terminal;
import org.codelibs.fesen.cli.UserException;
import org.codelibs.fesen.core.PathUtils;
import org.codelibs.fesen.core.SuppressForbidden;
import org.codelibs.fesen.env.Environment;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * A subcommand for the keystore cli which adds a file setting.
 */
class AddFileKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<String> arguments;

    AddFileKeyStoreCommand() {
        super("Add a file setting to the keystore", false);
        this.forceOption = parser.acceptsAll(Arrays.asList("f", "force"),
                "Overwrite existing setting without prompting, creating keystore if necessary");
        // jopt simple has issue with multiple non options, so we just get one set of them here
        // and convert to File when necessary
        // see https://github.com/jopt-simple/jopt-simple/issues/103
        this.arguments = parser.nonOptions("(setting path)+");
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> argumentValues = arguments.values(options);
        if (argumentValues.size() == 0) {
            throw new UserException(ExitCodes.USAGE, "Missing setting name");
        }
        if (argumentValues.size() % 2 != 0) {
            throw new UserException(ExitCodes.USAGE, "settings and filenames must come in pairs");
        }

        final KeyStoreWrapper keyStore = getKeyStore();

        for (int i = 0; i < argumentValues.size(); i += 2) {
            final String setting = argumentValues.get(i);

            if (keyStore.getSettingNames().contains(setting) && options.has(forceOption) == false) {
                if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without modifying keystore.");
                    return;
                }
            }

            final Path file = getPath(argumentValues.get(i + 1));
            if (Files.exists(file) == false) {
                throw new UserException(ExitCodes.IO_ERROR, "File [" + file.toString() + "] does not exist");
            }

            keyStore.setFile(setting, Files.readAllBytes(file));
        }

        keyStore.save(env.configFile(), getKeyStorePassword().getChars());
    }

    @SuppressForbidden(reason = "file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }

}
