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

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ibm.watson.kvutils.AbstractDistributor;
import com.ibm.watson.kvutils.Distributor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.*;

public class ZookeeperDistributor extends AbstractDistributor {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

    private final PersistentNode ourNode;
    private final TreeCache cache;

    public ZookeeperDistributor(CuratorFramework curator, String path, String participantId) {
        super(participantId);
        ourNode = id != null? new PersistentNode(curator, CreateMode.EPHEMERAL,
                true, ZKPaths.makePath(path, id), id.getBytes(UTF_8)) : null;
        cache = new TreeCache(curator, path);
        cache.getListenable().addListener((c, event) -> updateParticipants());
    }

    private void updateParticipants() {
        Iterator<ChildData> cdIt = cache.getChildren(cache.getRootPath());
        if (cdIt == null || !cdIt.hasNext()) {
            updateParticipants(EMPTY_ARR, false);
        } else {
            Iterator<Participant> pIt = Iterators.transform(Iterators.filter(cdIt, cd
                    -> cd.getData() != null), cd -> new Participant(new String(cd.getData(), UTF_8)));
            boolean[] found = new boolean[1];
            if (id != null) {
                pIt = Iterators.filter(pIt, p -> !(id.equals(p.id) && (found[0] = true)));
            }
            updateParticipants(Iterators.toArray(pIt, Participant.class), found[0]);
        }
    }

    private static final ThreadFactory DAEMON_TF = new ThreadFactoryBuilder().setDaemon(true).build();

    @Override
    public ListenableFuture<Distributor> start() throws Exception {
        //TODO tidy up, this is kind of messy
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors
                .newSingleThreadExecutor(DAEMON_TF));
        boolean ok = false;
        try {
            CountDownLatch cdl = new CountDownLatch(1);
            cache.getListenable().addListener(new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework c, TreeCacheEvent e) {
                    if (e.getType() == Type.INITIALIZED) {
                        cdl.countDown();
                        cache.getListenable().removeListener(this);
                    }
                }
            });
            cache.start();
            if (ourNode != null) {
                ourNode.start();
            }
            ListenableFuture<Distributor> fut = executor.submit(() -> {
                try {
                    if (ourNode != null && !ourNode.waitForInitialCreate(30, TimeUnit.SECONDS)) {
                        throw new TimeoutException();
                    }
                    cdl.await();
                    return this;
                } finally {
                    executor.shutdown();
                }
            });
            ok = true;
            return fut;
        } finally {
            if (!ok) {
                executor.shutdown();
                close();
            }
        }
    }

    @Override
    public void doClose() {
        if (ourNode != null) {
            try {
                ourNode.close();
            } catch (IOException e) {
                logger.error("Error closing PersistentNode", e);
            }
        }
        cache.close();
    }

}
