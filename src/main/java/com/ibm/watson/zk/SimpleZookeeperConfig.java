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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.kvutils.DynamicConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.*;

/**
 *
 */
public class SimpleZookeeperConfig extends AbstractMap<String, String>
        implements DynamicConfig, Closeable {

    protected final String root;
    protected final TreeCache cache;
    private boolean started;
    private volatile boolean vstarted;

    public SimpleZookeeperConfig(CuratorFramework curator, String root) {
        if (root.endsWith("/")) {
            throw new IllegalArgumentException("path must not end with /");
        }
        this.root = root;
        cache = TreeCache.newBuilder(curator, root).setMaxDepth(1).build();
    }

    @Override
    public void start() throws Exception {
        try {
            startAsync().get(3, TimeUnit.MINUTES);
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e, Exception.class);
            Throwables.throwIfUnchecked(e);
            throw e;
        }
    }

    @Override
    public ListenableFuture<Boolean> startAsync() {
        final SettableFuture<Boolean> future = SettableFuture.create();
        cache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) {
                if (event.getType() == Type.INITIALIZED) {
                    vstarted = started = true;
                    cache.getListenable().removeListener(this);
                    future.set(Boolean.TRUE);
                }
            }
        });
        try {
            cache.start();
            return future;
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public String getOrDefault(Object key, String defaultVal) {
        ensureStarted();
        ChildData cd = cache.getCurrentData(ZKPaths.makePath(root, key.toString()));
        return cd == null? defaultVal : valueOf(cd);
    }

    public String get(String key) {
        return getOrDefault(key, null);
    }

    @Override
    public String get(Object key) {
        return get((String) key);
    }

    @Override
    public void close() throws IOException {
        cache.close();
    }

    @Override
    public void addListener(final Listener listener) {
        cache.getListenable().addListener(makeListener(listener, null));
    }

    @Override
    public void addAndInvokeListener(final Listener listener, final Set<String> forKeys) {
        synchronized (listener) {
            cache.getListenable().addListener(makeListener(listener, forKeys));
            for (String k : forKeys) {
                listener.configChanged(k, get(k));
            }
        }
    }

    private TreeCacheListener makeListener(final Listener listener, final Set<String> forKeys) {
        return (client, event) -> {
            if (event.getData() != null &&
                event.getData().getPath().length() <= root.length() + 1) {
                return;
            }
            String val = null;
            switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
                val = valueOf(event.getData());
                // fall-thru
            case NODE_REMOVED:
                String key = keyFromPath(event.getData().getPath());
                if (key.indexOf('/') >= 0) {
                    return;
                }
                if (forKeys == null) {
                    listener.configChanged(key, val);
                } else if (forKeys.contains(key)) {
                    synchronized (listener) {
                        listener.configChanged(key, val);
                    }
                }
                break;
            default:
                // ignore
                break;
            }
        };
    }

    private String keyFromPath(String path) {
        return path.substring(root.length() + 1);
    }

    private final Set<Entry<String, String>> entrySet = new AbstractSet<Entry<String, String>>() {
        @Override
        public Iterator<Entry<String, String>> iterator() {
            ensureStarted();
            Iterator<ChildData> it = cache.getChildren(root);
            return it == null? Collections.emptyIterator() : Iterators.transform(it, cd
                    -> Maps.immutableEntry(keyFromPath(cd.getPath()), valueOf(cd)));
        }

        @Override
        public int size() {
            ensureStarted();
            ChildData rootData = cache.getCurrentData(root);
            Stat stat = rootData != null? rootData.getStat() : null;
            return stat != null? stat.getNumChildren() : 0;
        }
    };

    @Override
    public Set<Entry<String, String>> entrySet() {
        return entrySet;
    }

    private void ensureStarted() throws IllegalStateException {
        if (!started && !vstarted) {
            throw new IllegalStateException("config map not started");
        }
    }

    static String valueOf(ChildData cd) {
        byte[] data = cd.getData();
        return data != null? new String(data, UTF_8) : null;
    }
}

