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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.utils.PersistentLeaseKey;
import com.ibm.etcd.client.utils.RangeCache;
import com.ibm.etcd.client.utils.RangeCache.Listener;
import com.ibm.watson.kvutils.AbstractDistributor;
import com.ibm.watson.kvutils.Distributor;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.*;
import static com.ibm.etcd.client.KeyUtils.*;

/**
 * Etcd-based distributor pattern based on rendezvous hashing
 */
public class EtcdDistributor extends AbstractDistributor {

    protected final ByteString bytesPrefix;
    protected final int prefixLen;

    protected final PersistentLeaseKey ourParticipant; // null if id is null

    protected final RangeCache cache;

    protected boolean initialized;

    /**
     * Observer-only mode, won't participate in election, leaders have no effect
     */
    public EtcdDistributor(EtcdClient client, ByteString prefix) {
        this(client, prefix, null);
    }

    // maybe use serializedexecutor here

    public EtcdDistributor(EtcdClient client, ByteString prefix, String participantId) {
        super(participantId);
        this.bytesPrefix = prefix;
        this.prefixLen = bytesPrefix.size();
        this.cache = new RangeCache(client, prefix, true);

        cache.addListener((event, kv) -> {
            if (!initialized) {
                if (event != Listener.EventType.INITIALIZED) {
                    return;
                }
                initialized = true;
            }
            updateParticipants();
        });
        ourParticipant = id == null? null
                : new PersistentLeaseKey(client, cacheKey(id), bs(id), cache);
    }

    private void updateParticipants() {
        List<Participant> participantList = new ArrayList<>(cache.size());
        boolean foundUs = false;
        synchronized (cache) {
            if (cache.isClosed()) {
                return;
            }
            for (KeyValue kv : cache) {
                String thisId = participantId(kv);
                if (thisId.equals(id)) {
                    foundUs = true;
                } else {
                    participantList.add(new Participant(thisId));
                }
            }
        }
        updateParticipants(participantList.toArray(EMPTY_ARR), foundUs);
    }

    @Override
    public ListenableFuture<Distributor> start() throws Exception {
        synchronized (cache) {
            ListenableFuture<Boolean> cacheFut = cache.start();
            if (ourParticipant == null) {
                return Futures.transform(cacheFut, b -> this, directExecutor());
            }
            ListenableFuture<ByteString> nodeFut = ourParticipant.startWithFuture();
            return Futures.whenAllComplete(cacheFut, nodeFut).call(() -> {
                cacheFut.get();
                nodeFut.get();
                return this;
            }, directExecutor());
        }
    }

    @Override
    public void doClose() {
        synchronized (cache) {
            if (ourParticipant != null) {
                ourParticipant.close();
            }
            cache.close();
        }
    }

    // ------ static conversion utility methods

    protected ByteString cacheKey(String str) {
        return str == null? null : bytesPrefix.concat(bs(str));
    }

    protected static String participantId(KeyValue kv) {
        String str = kv.getValue().toStringUtf8();
        int nl = str.indexOf('\n');
        return nl == -1? str : str.substring(0, nl);
    }

}
