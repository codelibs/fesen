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
package org.codelibs.fesen.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.codelibs.fesen.Version;
import org.codelibs.fesen.action.admin.cluster.stats.ClusterStatsAction;
import org.codelibs.fesen.action.admin.cluster.stats.ClusterStatsRequest;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.io.stream.BytesStreamOutput;
import org.codelibs.fesen.common.logging.Loggers;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.test.ESTestCase;
import org.codelibs.fesen.test.MockLogAppender;
import org.codelibs.fesen.test.junit.annotations.TestLogging;
import org.codelibs.fesen.transport.OutboundMessage;
import org.codelibs.fesen.transport.TcpChannel;
import org.codelibs.fesen.transport.TransportLogger;

import java.io.IOException;

import static org.mockito.Mockito.mock;

@TestLogging(value = "org.codelibs.fesen.transport.TransportLogger:trace", reason = "to ensure we log network events on TRACE level")
public class TransportLoggerTests extends ESTestCase {

    private MockLogAppender appender;

    public void setUp() throws Exception {
        super.setUp();
        appender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(TransportLogger.class), appender);
        appender.start();
    }

    public void tearDown() throws Exception {
        Loggers.removeAppender(LogManager.getLogger(TransportLogger.class), appender);
        appender.stop();
        super.tearDown();
    }

    public void testLoggingHandler() throws IOException {
        final String writePattern = ".*\\[length: \\d+" + ", request id: \\d+" + ", type: request" + ", version: .*"
                + ", header size: \\d+B" + ", action: cluster:monitor/stats]" + " WRITE: \\d+B";
        final MockLogAppender.LoggingExpectation writeExpectation = new MockLogAppender.PatternSeenEventExpectation("hot threads request",
                TransportLogger.class.getCanonicalName(), Level.TRACE, writePattern);

        final String readPattern = ".*\\[length: \\d+" + ", request id: \\d+" + ", type: request" + ", version: .*" + ", header size: \\d+B"
                + ", action: cluster:monitor/stats]" + " READ: \\d+B";

        final MockLogAppender.LoggingExpectation readExpectation = new MockLogAppender.PatternSeenEventExpectation(
                "cluster monitor request", TransportLogger.class.getCanonicalName(), Level.TRACE, readPattern);

        appender.addExpectation(writeExpectation);
        appender.addExpectation(readExpectation);
        BytesReference bytesReference = buildRequest();
        TransportLogger.logInboundMessage(mock(TcpChannel.class), bytesReference.slice(6, bytesReference.length() - 6));
        TransportLogger.logOutboundMessage(mock(TcpChannel.class), bytesReference);
        appender.assertAllExpectationsMatched();
    }

    private BytesReference buildRequest() throws IOException {
        boolean compress = randomBoolean();
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            OutboundMessage.Request request = new OutboundMessage.Request(new ThreadContext(Settings.EMPTY), new String[0],
                    new ClusterStatsRequest(), Version.CURRENT, ClusterStatsAction.NAME, randomInt(30), false, compress);
            return request.serialize(bytesStreamOutput);
        }
    }
}
