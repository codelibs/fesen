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

package org.codelibs.fesen.index.fielddata.plain;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.codelibs.fesen.index.fielddata.FieldData;
import org.codelibs.fesen.index.fielddata.LeafFieldData;
import org.codelibs.fesen.index.fielddata.ScriptDocValues;
import org.codelibs.fesen.index.fielddata.ScriptDocValues.Strings;
import org.codelibs.fesen.index.fielddata.SortedBinaryDocValues;

/** {@link LeafFieldData} impl on top of Lucene's binary doc values. */
public class BinaryDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String field;

    public BinaryDVLeafFieldData(LeafReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        try {
            final BinaryDocValues values = DocValues.getBinary(reader, field);
            return FieldData.singleton(values);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues());
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

}
