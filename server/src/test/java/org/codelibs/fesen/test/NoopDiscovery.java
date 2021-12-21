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
package org.codelibs.fesen.test;

import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.cluster.ClusterChangedEvent;
import org.codelibs.fesen.common.component.Lifecycle;
import org.codelibs.fesen.common.component.LifecycleListener;
import org.codelibs.fesen.discovery.Discovery;
import org.codelibs.fesen.discovery.DiscoveryStats;

public class NoopDiscovery implements Discovery {

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        publishListener.onResponse(null);
    }

    @Override
    public DiscoveryStats stats() {
        return null;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void close() {}
}
