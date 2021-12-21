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

package org.codelibs.fesen.action.admin.indices.template.delete;

import static org.codelibs.fesen.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Objects;

import org.codelibs.fesen.action.ActionRequestValidationException;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.action.support.master.MasterNodeRequest;
import org.codelibs.fesen.common.io.stream.StreamInput;
import org.codelibs.fesen.common.io.stream.StreamOutput;

public class DeleteComposableIndexTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComposableIndexTemplateAction INSTANCE = new DeleteComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/delete";

    private DeleteComposableIndexTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        public Request() { }

        /**
         * Constructs a new delete template request for the specified name.
         */
        public Request(String name) {
            this.name = name;
        }

        /**
         * Set the index template name to delete.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        /**
         * The index template name to delete.
         */
        public String name() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(other.name, this.name);
        }
    }
}
