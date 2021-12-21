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
package org.codelibs.fesen.search.sort;

import org.apache.lucene.search.Sort;
import org.codelibs.fesen.search.DocValueFormat;

public final class SortAndFormats {

    public final Sort sort;
    public final DocValueFormat[] formats;

    public SortAndFormats(Sort sort, DocValueFormat[] formats) {
        if (sort.getSort().length != formats.length) {
            throw new IllegalArgumentException("Number of sort field mismatch: "
                    + sort.getSort().length + " != " + formats.length);
        }
        this.sort = sort;
        this.formats = formats;
    }

}
