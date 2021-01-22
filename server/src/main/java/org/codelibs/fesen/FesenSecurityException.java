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
package org.codelibs.fesen;

import java.io.IOException;

import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.rest.RestStatus;

/**
 * Generic security exception
 */
public class FesenSecurityException extends FesenStatusException {
    /**
     * Build the exception with a specific status and cause.
     */
    public FesenSecurityException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, status, cause, args);
    }

    /**
     * Build the exception with the status derived from the cause.
     */
    public FesenSecurityException(String msg, Exception cause, Object... args) {
        this(msg, ExceptionsHelper.status(cause), cause, args);
    }

    /**
     * Build the exception with a status of {@link RestStatus#INTERNAL_SERVER_ERROR} without a cause.
     */
    public FesenSecurityException(String msg, Object... args) {
        this(msg, RestStatus.INTERNAL_SERVER_ERROR, args);
    }

    /**
     * Build the exception without a cause.
     */
    public FesenSecurityException(String msg, RestStatus status, Object... args) {
        super(msg, status, args);
    }

    /**
     * Read from a stream.
     */
    public FesenSecurityException(StreamInput in) throws IOException {
        super(in);
    }
}
