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

package org.codelibs.fesen.client.transport;

import java.io.IOException;

import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.rest.RestStatus;

/**
 * An exception indicating no node is available to perform the operation.
 */
public class NoNodeAvailableException extends FesenException {

    public NoNodeAvailableException(String message) {
        super(message);
    }

    public NoNodeAvailableException(String message, Throwable t) {
        super(message, t);
    }

    public NoNodeAvailableException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
