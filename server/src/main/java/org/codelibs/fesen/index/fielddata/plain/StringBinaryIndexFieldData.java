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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.codelibs.fesen.common.util.BigArrays;
import org.codelibs.fesen.index.fielddata.IndexFieldData;
import org.codelibs.fesen.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.codelibs.fesen.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.codelibs.fesen.search.DocValueFormat;
import org.codelibs.fesen.search.MultiValueMode;
import org.codelibs.fesen.search.aggregations.support.ValuesSourceType;
import org.codelibs.fesen.search.sort.BucketedSort;
import org.codelibs.fesen.search.sort.SortOrder;

public class StringBinaryIndexFieldData implements IndexFieldData<StringBinaryDVLeafFieldData> {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    public StringBinaryIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public StringBinaryDVLeafFieldData load(LeafReaderContext context) {
        try {
            return new StringBinaryDVLeafFieldData(DocValues.getBinary(context.reader(), fieldName));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("can't sort on binary field");
    }

    @Override
    public StringBinaryDVLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }
}
