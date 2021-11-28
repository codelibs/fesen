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

package org.codelibs.fesen.script;

import java.util.Map;
import java.util.function.Function;

/**
 * A mocked script used for testing purposes.  {@code deterministic} implies cacheability in query shard cache.
 */
public abstract class MockDeterministicScript implements Function<Map<String, Object>, Object>, ScriptFactory {
    public abstract Object apply(Map<String, Object> vars);

    public abstract boolean isResultDeterministic();

    public static MockDeterministicScript asDeterministic(Function<Map<String, Object>, Object> script) {
        return new MockDeterministicScript() {
            @Override
            public boolean isResultDeterministic() {
                return true;
            }

            @Override
            public Object apply(Map<String, Object> vars) {
                return script.apply(vars);
            }
        };
    }

    public static MockDeterministicScript asNonDeterministic(Function<Map<String, Object>, Object> script) {
        return new MockDeterministicScript() {
            @Override
            public boolean isResultDeterministic() {
                return false;
            }

            @Override
            public Object apply(Map<String, Object> vars) {
                return script.apply(vars);
            }
        };
    }
}
