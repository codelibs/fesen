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
package org.codelibs.fesen.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.codelibs.fesen.index.shard.ShardId;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} that exposes
 * Fesen internal per shard / index information like the shard ID.
 */
public class FesenLeafReader extends SequentialStoredFieldsLeafReader {

    private final ShardId shardId;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public FesenLeafReader(LeafReader in, ShardId shardId) {
        super(in);
        this.shardId = shardId;
    }

    /**
     * Returns the shard id this segment belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public static FesenLeafReader getFesenLeafReader(LeafReader reader) {
        if (reader instanceof FilterLeafReader) {
            if (reader instanceof FesenLeafReader) {
                return (FesenLeafReader) reader;
            } else {
                // We need to use FilterLeafReader#getDelegate and not FilterLeafReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of FesenLeafReader. This can cause us to miss the shardId.
                return getFesenLeafReader(((FilterLeafReader) reader).getDelegate());
            }
        }
        return null;
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
