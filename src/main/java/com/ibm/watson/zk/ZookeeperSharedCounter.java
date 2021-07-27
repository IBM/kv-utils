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

import com.ibm.watson.kvutils.SharedCounter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;

import java.io.IOException;

/**
 * Zookeeper {@link SharedCounter} implementation
 * <p>
 * <b>Note</b> it is a known limitation that this impl only
 * supports 32 bit values. It would be trivial to enhance this
 * but not a priority given etcd direction.
 */
public class ZookeeperSharedCounter implements SharedCounter {

    protected static final int MAX_TRIES = 24;

    protected final SharedCount count;

    public ZookeeperSharedCounter(CuratorFramework curator,
            String counterPath, int startFrom) throws Exception {
        count = new SharedCount(curator, counterPath, startFrom);
        count.start();
    }

    public ZookeeperSharedCounter(CuratorFramework curator,
            String counterPath) throws Exception {
        this(curator, counterPath, 0);
    }

    @Override
    public long getCurrentCount() throws Exception {
        return count.getCount();
    }

    @Override
    public long getNextCount() throws Exception {
        for (int i = 0; i < MAX_TRIES; i++) {
            VersionedValue<Integer> vv = count.getVersionedValue();
            int cur = vv.getValue();
            if (count.trySetCount(vv, cur + 1)) {
                return cur;
            }
        }
        throw new Exception("Failed to get next unique id in " + MAX_TRIES + " attempts");
    }

    @Override
    public void close() throws IOException {
        count.close();
    }

}
