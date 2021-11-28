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

package org.codelibs.fesen.repositories.blobstore;

import java.util.Map;

import org.codelibs.fesen.cluster.metadata.RepositoryMetadata;
import org.codelibs.fesen.cluster.service.ClusterService;
import org.codelibs.fesen.common.UUIDs;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.indices.recovery.RecoverySettings;
import org.codelibs.fesen.repositories.RepositoryInfo;
import org.codelibs.fesen.repositories.RepositoryStatsSnapshot;
import org.codelibs.fesen.threadpool.ThreadPool;

public abstract class MeteredBlobStoreRepository extends BlobStoreRepository {
    private final RepositoryInfo repositoryInfo;

    public MeteredBlobStoreRepository(RepositoryMetadata metadata, boolean compress, NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService, RecoverySettings recoverySettings, Map<String, String> location) {
        super(metadata, compress, namedXContentRegistry, clusterService, recoverySettings);
        ThreadPool threadPool = clusterService.getClusterApplierService().threadPool();
        this.repositoryInfo =
                new RepositoryInfo(UUIDs.randomBase64UUID(), metadata.name(), metadata.type(), location, threadPool.absoluteTimeInMillis());
    }

    public RepositoryStatsSnapshot statsSnapshot() {
        return new RepositoryStatsSnapshot(repositoryInfo, stats(), RepositoryStatsSnapshot.UNKNOWN_CLUSTER_VERSION, false);
    }

    public RepositoryStatsSnapshot statsSnapshotForArchival(long clusterVersion) {
        RepositoryInfo stoppedRepoInfo = repositoryInfo.stopped(threadPool.absoluteTimeInMillis());
        return new RepositoryStatsSnapshot(stoppedRepoInfo, stats(), clusterVersion, true);
    }
}
