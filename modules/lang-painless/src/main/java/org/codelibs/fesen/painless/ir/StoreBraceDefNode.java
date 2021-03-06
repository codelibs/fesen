/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.painless.ir;

import org.codelibs.fesen.painless.ClassWriter;
import org.codelibs.fesen.painless.DefBootstrap;
import org.codelibs.fesen.painless.Location;
import org.codelibs.fesen.painless.MethodWriter;
import org.codelibs.fesen.painless.lookup.PainlessLookupUtility;
import org.codelibs.fesen.painless.lookup.def;
import org.codelibs.fesen.painless.phase.IRTreeVisitor;
import org.codelibs.fesen.painless.symbol.WriteScope;
import org.objectweb.asm.Type;

public class StoreBraceDefNode extends StoreNode {

    /* ---- begin node data ---- */

    private Class<?> indexType;

    public void setIndexType(Class<?> indexType) {
        this.indexType = indexType;
    }

    public Class<?> getIndexType() {
        return indexType;
    }

    public String getIndexCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(indexType);
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitStoreBraceDef(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        // do nothing; terminal node
    }

    /* ---- end visitor ---- */

    public StoreBraceDefNode(Location location) {
        super(location);
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        getChildNode().write(classWriter, methodWriter, writeScope);

        methodWriter.writeDebugInfo(getLocation());
        Type methodType = Type.getMethodType(
                MethodWriter.getType(void.class),
                MethodWriter.getType(def.class),
                MethodWriter.getType(indexType),
                MethodWriter.getType(getStoreType()));
        methodWriter.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }
}
