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

package org.codelibs.fesen.action.admin.cluster.storedscripts;

import org.codelibs.fesen.action.support.master.AcknowledgedRequestBuilder;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.client.FesenClient;
import org.codelibs.fesen.common.bytes.BytesReference;
import org.codelibs.fesen.common.xcontent.XContentType;

public class PutStoredScriptRequestBuilder
        extends AcknowledgedRequestBuilder<PutStoredScriptRequest, AcknowledgedResponse, PutStoredScriptRequestBuilder> {

    public PutStoredScriptRequestBuilder(FesenClient client, PutStoredScriptAction action) {
        super(client, action, new PutStoredScriptRequest());
    }

    public PutStoredScriptRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Set the source of the script along with the content type of the source
     */
    public PutStoredScriptRequestBuilder setContent(BytesReference source, XContentType xContentType) {
        request.content(source, xContentType);
        return this;
    }
}
