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

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.config.EtcdClusterConfig;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentTxnRequest;
import com.ibm.etcd.client.kv.KvClient.FluentTxnSuccOps;
import com.ibm.watson.kvutils.Distributor;
import com.ibm.watson.kvutils.DynamicConfig;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTable.Helper;
import com.ibm.watson.kvutils.LeaderElection;
import com.ibm.watson.kvutils.OrderedShutdownHooks;
import com.ibm.watson.kvutils.SessionNode;
import com.ibm.watson.kvutils.SharedCounter;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 *
 */
public class EtcdUtilsFactory extends KVUtilsFactory {

    private final EtcdClient client;
    private final ByteString rootPrefix; // null => no namespace

    //TODO chroot path conversion considerations (leading & trailing /)
    // maybe enforce namspace doesn't end with / and that all paths start with it

    public EtcdUtilsFactory(String jsonConfigFileOrSimpleString) throws Exception {
        this(EtcdClusterConfig.fromJsonFileOrSimple(jsonConfigFileOrSimpleString));
    }

    public EtcdUtilsFactory(EtcdClusterConfig clusterConfig) throws Exception {
        this(clusterConfig.getClient(), clusterConfig.getRootPrefix());
    }

    public EtcdUtilsFactory(EtcdClient client, ByteString rootPrefix) {
        this.client = client;
        this.rootPrefix = rootPrefix == null || rootPrefix.isEmpty()
                ? null : rootPrefix;
    }

    @Override
    public SessionNode newSessionNode(String path, byte[] defaultValue) {
        return new EtcdSessionNode(client,
                pathToKey(path, false), ByteString.copyFrom(defaultValue), null);
    }

    @Override
    public long getSessionId() {
        return client.getSessionLease().getLeaseId();
    }

    @Override
    public SharedCounter newSharedCounter(String path, long startFrom) throws Exception {
        return new EtcdSharedCounter(client, pathToKey(path, false), startFrom);
    }

    @Override
    public KVTable newKVTable(String path, int defaultBuckets) {
        return new EtcdKVTable(client, pathToKey(path, true));
    }

    @Override
    public Helper KVTableHelper() {
        return EtcdKVTable.helper;
    }

    @Override
    public DynamicConfig newDynamicConfig(String path) {
        return new SimpleEtcdConfig(client, pathToKey(path, true));
    }

    @Override
    public LeaderElection newLeaderElection(String path, String id) {
        return new EtcdElection(client, pathToKey(path, true), id);
    }

    @Override
    public Distributor newDistributor(String path, String id) {
        return new EtcdDistributor(client, pathToKey(path, true), id);
    }

    @Override
    public long getOrAssignSharedCount(String path, String counterPath) throws Exception {
        KvClient kvClient = client.getKvClient();
        ByteString bsPath = pathToKey(path, false), bsValue = null;
        long assigned = -1;
        RangeResponse rr = kvClient.get(bsPath).sync();
        while (true) {
            long modRev = 0;
            if (rr.getKvsCount() > 0) {
                ByteBuffer bb = rr.getKvs(0).getValue().asReadOnlyByteBuffer();
                if (bb.remaining() == 8) {
                    return bb.getLong();
                }
                if (bb.remaining() == 4) {
                    return bb.getInt();
                }
                modRev = rr.getKvs(0).getModRevision();
            }
            if (assigned < 0) {
                if (counterPath == null) {
                    return -1L; // "read only" query
                }
                try (final SharedCounter count = newSharedCounter(counterPath, 1)) {
                    assigned = count.getNextCount();
                    bsValue = UnsafeByteOperations.unsafeWrap(Longs.toByteArray(assigned));
                }
            }
            TxnResponse tr = kvClient.txnIf().cmpEqual(bsPath).mod(modRev)
                    .then().put(PutRequest.newBuilder().setKey(bsPath).setValue(bsValue))
                    .elseDo().get(RangeRequest.newBuilder().setKey(bsPath)).sync();
            if (tr.getSucceeded()) {
                return assigned;
            }
            rr = tr.getResponses(0).getResponseRange();
        }
    }

    protected ByteString pathToKey(String path, boolean ensureTrailingSlash) {
        if (path.isEmpty() || path.charAt(0) != '/') {
            throw new IllegalArgumentException("path must start with \"/\": " + path);
        }
        if (ensureTrailingSlash && path.charAt(path.length() - 1) != '/') {
            path += "/";
        }
        ByteString bs = ByteString.copyFromUtf8(path);
        return rootPrefix != null ? rootPrefix.concat(bs) : bs;
    }

    @Override
    public String getKvStoreType() {
        return ETCD_TYPE;
    }

    private static final String TEST_PATH = "/__DUMMY_KEY_FOR_TESTING_CONNECTION";

    @Override
    public ListenableFuture<Boolean> verifyKvStoreConnection() {
        return Futures.catching(Futures.transform(client.getKvClient().get(pathToKey(TEST_PATH, false)).countOnly()
                        .serializable(true).async(), rr -> Boolean.TRUE, MoreExecutors.directExecutor()),
                Exception.class, e -> { // Intentionally not catching Errors
                    throw new RuntimeException("etcd connection verification failed", e);
                }, MoreExecutors.directExecutor());
    }

    @Override
    public boolean[] pathsExist(String[] paths) throws Exception {
        final KvClient kvc = client.getKvClient();
        final FluentTxnSuccOps txn = kvc.txnIf().then();
        for (String path : Objects.requireNonNull(paths, "paths")) {
            Objects.requireNonNull(path, "path");
            txn.get(kvc.get(pathToKey(path, false)).asPrefix().countOnly().asRequest());
        }
        TxnResponse resp = txn.sync();
        boolean[] results = new boolean[paths.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = resp.getResponses(i).getResponseRange().getCount() != 0;
        }
        return results;
    }

    @Override
    public byte[] compareAndSetNodeContents(String path,
            byte[] expectedValue, byte[] newValue) throws Exception {
        final KvClient kvc = client.getKvClient();
        final ByteString key = pathToKey(Objects.requireNonNull(path, "path"), false);
        FluentTxnRequest txn = kvc.txnIf();
        if (expectedValue == null) {
            txn.notExists(key);
        } else {
            txn.cmpEqual(key).value(UnsafeByteOperations.unsafeWrap(expectedValue));
        }
        FluentTxnSuccOps succ = txn.then();
        succ = newValue == null? succ.delete(kvc.delete(key).asRequest())
                : succ.put(kvc.put(key, UnsafeByteOperations.unsafeWrap(newValue)).asRequest());
        TxnResponse resp = succ.elseDo().get(kvc.get(key).asRequest()).sync();
        if (resp.getSucceeded()) {
            return newValue;
        }
        RangeResponse rr = resp.getResponses(0).getResponseRange();
        return rr.getKvsCount() == 0 ? null : rr.getKvs(0).getValue().toByteArray();
    }

    public EtcdClient getClient() {
        return client;
    }

    public ByteString getRootPrefix() {
        return rootPrefix;
    }

    static {
        OrderedShutdownHooks.addHook(1, EtcdClusterConfig::shutdownAll);
    }
}
