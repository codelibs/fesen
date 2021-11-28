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
package org.codelibs.fesen.test.rest.yaml.section;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.common.xcontent.XContentLocation;
import org.codelibs.fesen.common.xcontent.XContentParser;

/**
 * Represents an is_true assert section:
 *
 *   - is_true:  get.fields.bar
 *
 */
public class IsTrueAssertion extends Assertion {
    public static IsTrueAssertion parse(XContentParser parser) throws IOException {
        return new IsTrueAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    private static final Logger logger = LogManager.getLogger(IsTrueAssertion.class);

    public IsTrueAssertion(XContentLocation location, String field) {
        super(location, field, true);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] has a true value (field [{}])", actualValue, getField());
        String errorMessage = errorMessage();
        assertThat(errorMessage, actualValue, notNullValue());
        String actualString = actualValue.toString();
        assertThat(errorMessage, actualString, not(equalTo("")));
        assertThat(errorMessage, actualString, not(equalToIgnoringCase(Boolean.FALSE.toString())));
        assertThat(errorMessage, actualString, not(equalTo("0")));
    }

    private String errorMessage() {
        return "field [" + getField() + "] doesn't have a true value";
    }
}
