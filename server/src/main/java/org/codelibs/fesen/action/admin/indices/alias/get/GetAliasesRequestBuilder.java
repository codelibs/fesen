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

package org.codelibs.fesen.action.admin.indices.alias.get;

import org.codelibs.fesen.client.FesenClient;

public class GetAliasesRequestBuilder extends BaseAliasesRequestBuilder<GetAliasesResponse, GetAliasesRequestBuilder> {

    public GetAliasesRequestBuilder(FesenClient client, GetAliasesAction action, String... aliases) {
        super(client, action, aliases);
    }
}
