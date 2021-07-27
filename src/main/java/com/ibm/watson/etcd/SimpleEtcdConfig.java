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

package com.ibm.watson.etcd;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.utils.RangeCache;
import com.ibm.watson.kvutils.DynamicConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Etcd-backed configuration map. Automatically mirrors a prefixed range
 * of entries in etcd, and supports listening for updates of all or
 * a subset of those keys.
 */
public class SimpleEtcdConfig extends AbstractMap<String, String>
        implements DynamicConfig, Closeable {

    protected final ByteString prefix;
    protected final RangeCache cache;
    private boolean started;
    private volatile boolean vstarted;

    public SimpleEtcdConfig(EtcdClient client, ByteString prefix) {
        if (prefix == null || prefix.isEmpty()) {
            throw new IllegalArgumentException("prefix must be nonempty");
        }
        this.prefix = prefix;
        cache = new RangeCache(client, prefix);
    }

    public SimpleEtcdConfig(EtcdClient client, String root) {
        this(client, ByteString.copyFromUtf8(root));
    }

    @Override
    public void start() throws Exception {
        cache.start().get(3, TimeUnit.MINUTES); // blocking for now
        vstarted = started = true;
    }

    @Override
    public ListenableFuture<Boolean> startAsync() {
        return Futures.transform(cache.start(), b -> vstarted = started = b,
                MoreExecutors.directExecutor());
    }

    @Override
    public String getOrDefault(Object key, String defaultVal) {
        ensureStarted();
        KeyValue kv = cache.get(prefix.concat(ByteString
                .copyFromUtf8((String) key)));
        return kv == null? defaultVal : kv.getValue().toStringUtf8();
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
        cache.addListener(makeListener(listener, null));
    }

    @Override
    public void addAndInvokeListener(final Listener listener, final Set<String> forKeys) {
        synchronized (listener) {
            cache.addListener(makeListener(listener, forKeys));
            for (String k : forKeys) {
                listener.configChanged(k, get(k));
            }
        }
    }

    private RangeCache.Listener makeListener(final Listener listener, final Set<String> forKeys) {
        return (type, kv) -> {
            if (kv == null) {
                return;
            }
            String val = null;
            switch (type) {
            case UPDATED:
                val = kv.getValue().toStringUtf8();
                // fall-thru
            case DELETED:
                String key = stringKeyFromKv(kv);
                if (key == null) {
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

    private String stringKeyFromKv(KeyValue kv) {
        ByteString key = kv.getKey();
        if (key.size() <= prefix.size()) {
            return null;
        }
        return key.substring(prefix.size()).toStringUtf8();
    }

    private final Set<Entry<String, String>> entrySet = new AbstractSet<Entry<String, String>>() {
        @Override
        public Iterator<Entry<String, String>> iterator() {
            ensureStarted();
            Iterator<KeyValue> it = cache.iterator();
            return it == null? null : Iterators.transform(it, kv ->
                    Maps.immutableEntry(stringKeyFromKv(kv),
                            kv.getValue().toStringUtf8()));
        }

        @Override
        public int size() {
            ensureStarted();
            return cache.size();
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

    @Override
    public String toString() {
        return !started && !vstarted? "<config-not-started>" : super.toString();
    }

}
