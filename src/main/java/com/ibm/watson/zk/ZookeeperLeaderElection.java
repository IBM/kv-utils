/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.zk;

import com.ibm.watson.kvutils.LeaderElection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.io.IOException;

/**
 *
 */
public class ZookeeperLeaderElection implements LeaderElection {

    private final LeaderLatch latch;
    private final boolean observeOnly;

    public ZookeeperLeaderElection(CuratorFramework curator, String path, String id) {
        observeOnly = id == null;
        latch = new LeaderLatch(curator, path, observeOnly? "" : id);
    }

    @Override
    public String getId() {
        return observeOnly? null : latch.getId();
    }

    @Override
    public String getLeaderId() throws Exception {
        return latch.getLeader().getId();
    }

    @Override
    public boolean isLeader() {
        return latch.hasLeadership();
    }

    @Override
    public void addListener(ElectionListener listener) {
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void notLeader() {
                listener.leadershipChange(false);
            }

            @Override
            public void isLeader() {
                listener.leadershipChange(true);
            }
        });
    }

    @Override
    public void start() throws Exception {
        if (!observeOnly) {
            latch.start();
        }
    }

    @Override
    public void close() throws IOException {
        if (!observeOnly) {
            latch.close();
        }
    }

}
