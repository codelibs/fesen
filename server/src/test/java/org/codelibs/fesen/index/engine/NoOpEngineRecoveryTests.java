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
package org.codelibs.fesen.index.engine;

import org.codelibs.fesen.cluster.routing.ShardRouting;
import org.codelibs.fesen.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.index.engine.NoOpEngine;
import org.codelibs.fesen.index.shard.IndexShard;
import org.codelibs.fesen.index.shard.IndexShardTestCase;

import static org.codelibs.fesen.cluster.routing.ShardRoutingHelper.initWithSameId;

import java.io.IOException;

public class NoOpEngineRecoveryTests extends IndexShardTestCase {

    public void testRecoverFromNoOp() throws IOException {
        final int nbDocs = scaledRandomIntBetween(1, 100);

        final IndexShard indexShard = newStartedShard(true);
        for (int i = 0; i < nbDocs; i++) {
            indexDoc(indexShard, "_doc", String.valueOf(i));
        }
        indexShard.close("test", true);

        final ShardRouting shardRouting = indexShard.routingEntry();
        IndexShard primary = reinitShard(indexShard, initWithSameId(shardRouting, ExistingStoreRecoverySource.INSTANCE),
            indexShard.indexSettings().getIndexMetadata(), NoOpEngine::new);
        recoverShardFromStore(primary);
        assertEquals(primary.seqNoStats().getMaxSeqNo(), primary.getMaxSeqNoOfUpdatesOrDeletes());
        assertEquals(nbDocs, primary.docStats().getCount());

        IndexShard replica = newShard(false, Settings.EMPTY, NoOpEngine::new);
        recoverReplica(replica, primary, true);
        assertEquals(replica.seqNoStats().getMaxSeqNo(), replica.getMaxSeqNoOfUpdatesOrDeletes());
        assertEquals(nbDocs, replica.docStats().getCount());
        closeShards(primary, replica);
    }
}
