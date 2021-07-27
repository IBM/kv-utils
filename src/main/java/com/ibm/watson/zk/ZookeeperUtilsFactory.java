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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.kvutils.Distributor;
import com.ibm.watson.kvutils.DynamicConfig;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTable.Helper;
import com.ibm.watson.kvutils.LeaderElection;
import com.ibm.watson.kvutils.SessionNode;
import com.ibm.watson.kvutils.SharedCounter;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 *
 */
public class ZookeeperUtilsFactory extends KVUtilsFactory {

    private final CuratorFramework curator;

    public ZookeeperUtilsFactory(String zkConnString) {
        this(ZookeeperClient.getCurator(zkConnString));
    }

    public ZookeeperUtilsFactory(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public SessionNode newSessionNode(String path, byte[] defaultValue) {
        return new SettablePersistentNode(curator, CreateMode.EPHEMERAL,
                false, path, defaultValue);
    }

    @Override
    public long getSessionId() {
        return 100L; // this just needs to be non-zero in ZK case
    }

    @Override
    public SharedCounter newSharedCounter(String path, long startFrom) throws Exception {
        return new ZookeeperSharedCounter(curator, path, (int) startFrom);
    }

    @Override
    public KVTable newKVTable(String path, int defaultBuckets) {
        return new ZookeeperKVTable(curator, path, defaultBuckets);
    }

    @Override
    public Helper KVTableHelper() {
        return ZookeeperKVTable.helper;
    }

    @Override
    public DynamicConfig newDynamicConfig(String path) {
        return new SimpleZookeeperConfig(curator, path);
    }

    @Override
    public LeaderElection newLeaderElection(String path, String id) {
        return new ZookeeperLeaderElection(curator, path, id);
    }

    @Override
    public Distributor newDistributor(String path, String id) {
        return new ZookeeperDistributor(curator, path, id);
    }

    /**
     * Zookeeper impl only supports int-sized counts
     *
     * @see KVUtilsFactory#getOrAssignSharedCount(String, String)
     */
    @Override
    public long getOrAssignSharedCount(String path, String counterPath) throws Exception {
        Objects.requireNonNull(path, "path");
        int assigned = -1;
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = curator.getData().storingStatIn(stat).forPath(path);
                if (data != null) {
                    if (data.length == 4) {
                        return ByteBuffer.wrap(data).getInt();
                    }
                    if (data.length == 8) {
                        return ByteBuffer.wrap(data).getLong();
                    }
                }
                if (assigned < 0) {
                    if (counterPath == null) {
                        return -1L; // "read-only" query
                    }
                    assigned = getNewSlotNum(counterPath);
                }
                try {
                    curator.setData().withVersion(stat.getVersion())
                            .forPath(path, ByteBuffer.allocate(4).putInt(assigned).array());
                    return assigned;
                } catch (KeeperException.BadVersionException bve) {
                } // continue loop
            } catch (KeeperException.NoNodeException nne) {
                if (assigned < 0) {
                    if (counterPath == null) {
                        return -1L; // "read-only" query
                    }
                    assigned = getNewSlotNum(counterPath);
                }
                try {
                    curator.create().creatingParentsIfNeeded()
                            .forPath(path, ByteBuffer.allocate(4).putInt(assigned).array());
                    return assigned;
                } catch (KeeperException.NodeExistsException nee) {
                } // continue loop
            }
        }
    }

    private int getNewSlotNum(String counterPath) throws Exception {
        try (final SharedCounter count = newSharedCounter(counterPath, 1)) {
            return (int) count.getNextCount();
        }
    }

    @Override
    public ListenableFuture<Boolean> verifyKvStoreConnection() {
        if (curator.getState() != CuratorFrameworkState.STARTED) {
            return Futures.immediateFailedFuture(new RuntimeException("Zookeeper not connected"));
        }
        final SettableFuture<Boolean> future = SettableFuture.create();
        try {
            curator.checkExists().inBackground((_c, e) -> {
                KeeperException.Code code = KeeperException.Code.get(e.getResultCode());
                if (code != KeeperException.Code.OK && code != KeeperException.Code.NONODE) {
                    future.setException(new RuntimeException(
                            "Zookeeper connection verification failed with code: " + code));
                } else {
                    future.set(Boolean.TRUE);
                }
            }).withUnhandledErrorListener((m, e) -> {
                future.setException(new RuntimeException("Zookeeper connection verification failed: " + m, e));
            }).forPath("/__DUMMY_PATH_FOR_TESTING_CONNECTION");
        } catch (Exception e) {
            future.setException(new RuntimeException("Zookeeper connection verification failed", e));
        }
        return future;
    }

    @Override
    public boolean[] pathsExist(String[] paths) throws Exception {
        //TODO make atomic
        boolean[] results = new boolean[paths.length];
        for (int i = 0; i < paths.length; i++) {
            results[i] = curator.checkExists().forPath(paths[i]) != null;
        }
        return results;
    }

    @Override
    public byte[] compareAndSetNodeContents(String path,
            byte[] expectedValue, byte[] newValue) throws Exception {
        Objects.requireNonNull(path, "path");
        while (true) {
            Stat stat = new Stat();
            byte[] data;
            try {
                data = curator.getData().storingStatIn(stat).forPath(path);
            } catch (KeeperException.NoNodeException nne) {
                data = null;
            }
            if (!Arrays.equals(expectedValue, data)) {
                return data;
            }
            if (data == null) {
                try {
                    if (newValue != null) {
                        curator.create().creatingParentsIfNeeded().forPath(path, newValue);
                    }
                    return newValue;
                } catch (KeeperException.NodeExistsException nee) {
                    // continue retry loop
                }
            } else {
                try {
                    int version = stat.getVersion();
                    if (newValue != null) {
                        curator.setData().withVersion(version).forPath(path, newValue);
                    } else {
                        curator.delete().withVersion(version).forPath(path);
                    }
                    return newValue;
                } catch (KeeperException.BadVersionException bve) {
                    // continue retry loop
                } catch (KeeperException.NoNodeException nne) {
                    return null;
                }
            }
        }
    }

    @Override
    public String getKvStoreType() {
        return ZK_TYPE;
    }
}
