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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;
import com.ibm.etcd.client.kv.KvClient.FluentTxnRequest;
import com.ibm.etcd.client.kv.KvClient.FluentTxnSuccOps;
import com.ibm.etcd.client.utils.RangeCache;
import com.ibm.etcd.client.utils.RangeCache.PutResult;
import com.ibm.watson.kvutils.AbstractKVTable;
import com.ibm.watson.kvutils.KVRecord;
import com.ibm.watson.kvutils.KVTable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.IntPredicate;

/**
 * etcd v3 {@link KVTable} implementation
 */
public class EtcdKVTable extends AbstractKVTable {

    protected final ByteString bytesPrefix;
    protected final int prefixLen;
    protected final TableRangeCache cache;

    protected final EtcdClient client;


    public EtcdKVTable(EtcdClient client, ByteString prefix) {
        bytesPrefix = prefix;
        prefixLen = bytesPrefix.size();
        this.client = client;
        cache = new TableRangeCache(client, bytesPrefix);
    }

    public EtcdKVTable(EtcdClient client, String prefix) {
        this(client, ByteString.copyFromUtf8(prefix));
    }

    @Override
    public ListenableFuture<Boolean> start() {
        return cache.start();
    }

    @Override
    public void close() throws IOException {
        cache.close(); //TODO

    }

    // --------- non view-specific methods

    @Override
    public int getCountAtLevel(int level) {
        if (level == 1) {
            return cache.size();
        }
        return level == 0? 1 : 0;
    }

    @Override
    public boolean containsConsistent(String key) throws Exception {
        return cache.keyExistsRemote(cacheKey(key));
    }

    @Override
    public boolean contains(String key) {
        return cache.keyExists(cacheKey(key));
    }

    @Override
    public boolean unconditionalDelete(String key) throws Exception {
        validateKey(key);
        return cache.delete(cacheKey(key));
    }

    @Override
    public boolean delete(String key, long version) throws Exception {
        validateKey(key);
        return cache.delete(cacheKey(key), version);
    }

    // ---------- TableView

    @Override
    public <T extends KVRecord> TableView<T> getView(Serializer<T> ser, int level) {
        if (level != 1) {
            throw new IllegalArgumentException("only level == 1 supported");
        }
        return new TableView<>(ser);
    }

    class TableView<T extends KVRecord> extends AbstractTableView<T> {

        protected TableView(Serializer<T> ser) {
            super(ser);
        }

        @Override
        public T get(String key) {
            return makeRecord(cache.get(cacheKey(key)));
        }

        @Override
        public T getConsistent(String key) throws Exception {
            return makeRecord(cache.getRemoteWeak(cacheKey(key)));
        }

        @Override
        public T getStrong(String key) throws Exception {
            return makeRecord(cache.getRemote(cacheKey(key)));
        }

        @Override
        public boolean conditionalSet(String key, T val) throws Exception {
            return conditionalSet(key, val, val.getLeaseId());
        }

        @Override
        public boolean conditionalSet(String key, T val, long leaseId) throws Exception {
            validateKey(key);
            long vers = val.getVersion();
            long modRev = cache.putNoGet(cacheKey(key), valBs(val),
                    leaseId, vers == -1L? 0L : vers);
            if (modRev == -1L) {
                return false;
            }
            setVersionAndReset(val, modRev, leaseId);
            return true;
        }

        @Override
        public T conditionalSetAndGet(String key, T val) throws Exception {
            return conditionalSetAndGet(key, val, val.getLeaseId());
        }

        @Override
        public T conditionalSetAndGet(String key, T val, long leaseId) throws Exception {
            validateKey(key);
            long vers = val.getVersion();
            PutResult pr = cache.put(cacheKey(key), valBs(val),
                    leaseId, vers == -1L? 0L : vers);
            return pr.succ()? setVersionAndReset(val, pr.kv().getModRevision(), leaseId)
                    : makeRecord(pr.kv());
        }

        @Override
        public void addListener(final boolean actionableOnly, Listener<T> listener) {
            cache.addListener((type, keyValue) -> {
                if (type == RangeCache.Listener.EventType.INITIALIZED) {
                    listener.event(KVTable.EventType.INITIALIZED, null, null);
                } else if (isCacheKeyValid(keyValue)) { // FOUND, UPDATED, REMOVED
                    T record = makeRecord(keyValue, !actionableOnly);
                    if (actionableOnly) {
                        if (!record.isActionableUpdate() &&
                            type != RangeCache.Listener.EventType.DELETED) {
                            return;
                        }
                        resetActionableUpdate(record);
                    }
                    listener.event(convertEvent(type, keyValue.getVersion()),
                            tableKey(keyValue), record);
                }
            });
        }

        @Override
        public Iterator<Entry<String, T>> iterator() {
            return keyValToEntryIterator(cache.iterator());
        }

        @Override
        public Iterator<Entry<String, T>> distributedIterator(IntPredicate distributor) {
            Preconditions.checkNotNull(distributor);
            // use explicit hash function to not depend on internal ByteString impl
            return keyValToEntryIterator(Iterators.filter(cache.iterator(),
                    kv -> distributor.test(byteStringHash(kv.getKey(), prefixLen))));
        }

        @Override
        protected Iterator<String> keyIterator() {
            //cache.keys().iterator();
            return Iterators.transform(cache.iterator(), EtcdKVTable.this::tableKey);
        }

        @Override
        protected Iterator<Entry<String, T>> strongIterator() {
            return keyValToEntryIterator(cache.strongIterator());
        }

        @Override
        public EtcdKVTable getTable() {
            return EtcdKVTable.this;
        }

        @Override
        protected int getLevel() {
            return 1;
        }

        // ------ conversion methods

        protected Iterator<Entry<String, T>> keyValToEntryIterator(Iterator<KeyValue> kvIt) {
            return Iterators.transform(kvIt, kv -> {
                String key = null;
                try {
                    return Maps.immutableEntry(key = tableKey(kv), makeRecord(kv));
                } catch (Exception e) {
                    if (key == null) {
                        key = kv.getKey().toStringUtf8();
                    }
                    throw new RuntimeException("Error deserializing etcd record with key: " + key, e);
                }
            });
        }

        protected T makeRecord(KeyValue kv, boolean reset) {
            if (kv == null) {
                return null;
            }
            T rec = deserialize(ser, kv.getValue());
            setVersion(rec, kv.getModRevision(), kv.getLease());
            if (reset) {
                resetActionableUpdate(rec);
            }
            return rec;
        }

        protected T makeRecord(KeyValue kv) {
            return makeRecord(kv, true);
        }

        protected ByteString valBs(T val) {
            return UnsafeByteOperations.unsafeWrap(ser.serialize(val));
        }

        // ------- unsupported methods

        @Override
        public Iterator<Entry<String, T>> children(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, T> childMap(String key) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Helper getHelper() {
        return helper;
    }

    private static final Field bytesField;
    private static final Class<?> literalBsClass;

    static {
        Class<?> lbs = null;
        Field bf = null;
        try {
            lbs = Class.forName(ByteString.class.getName() + "$LiteralByteString");
            bf = lbs.getDeclaredField("bytes");
            bf.setAccessible(true);
        } catch (Exception e) {
            logger.warn("Could not initialize ByteString deserialization optimization", e);
        }
        literalBsClass = lbs;
        bytesField = bf;
    }

    static <T> T deserialize(Serializer<T> ser, ByteString bs) {
        if (literalBsClass != null && literalBsClass.isInstance(bs)) {
            try {
                return ser.deserialize((byte[]) bytesField.get(bs));
            } catch (IllegalAccessException iae) { /* fallback */ }
        }
        return ser.deserialize(bs.newInput());
    }

    // ------ unsupported for non-hierarchical

    @Override
    public List<String> children(String key) throws Exception {
        throw new UnsupportedOperationException();
    }

    // ------ range check

    protected boolean isCacheKeyValid(KeyValue kv) {
        return kv != null && kv.getKey().startsWith(bytesPrefix);
    }

    protected String validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IndexOutOfBoundsException();
        }
        return key;
    }

    // ------ conversion utility methods

    protected ByteString cacheKey(String str) {
        return str == null? null : bytesPrefix.concat(bs(str));
    }

    protected String tableKey(KeyValue kv) {
        return kv.getKey().substring(prefixLen).toStringUtf8();
    }

    static ByteString bs(String str) {
        return ByteString.copyFromUtf8(str);
    }

    static int byteStringHash(ByteString bs, int offset) {
        int len = bs.size(), h = len - offset;
        for (int i = offset; i < len; i++) {
            h = h * 31 + bs.byteAt(i);
        }
        return h != 0? h : 1;
    }

    static EventType convertEvent(RangeCache.Listener.EventType type, long etcdVersion) {
        switch (type) {
        case INITIALIZED:
            return EventType.INITIALIZED;
        case DELETED:
            return EventType.ENTRY_DELETED;
        //case FOUND:
        case UPDATED:
            return etcdVersion > 1L?
                    EventType.ENTRY_UPDATED : EventType.ENTRY_ADDED;
        default:
            return null;
        }
    }


    // ------- helper

    /**
     * Subclass just to be able to access the protected offer methods
     */
    static class TableRangeCache extends RangeCache {
        public TableRangeCache(EtcdClient client, ByteString prefix) {
            super(client, prefix);
        }

        protected KeyValue offerUpdate(KeyValue keyValue) {
            return offerUpdate(keyValue, false);
        }

        @Override
        protected KeyValue offerDelete(ByteString key, long modRevision) {
            return super.offerDelete(key, modRevision);
        }
    }

    static class TxnComponent {
        final TableView<?> view;
        final ByteString key, val; // val == null for delete
        final KVRecord value; // null for "ensureAbsent"

        public TxnComponent(TableView<?> view, String key, KVRecord value, ByteString val) {
            this.view = view;
            EtcdKVTable table = view.getTable();
            this.key = table.cacheKey(table.validateKey(key));
            this.val = val;
            this.value = value;
        }
    }

    static final Helper helper = new Helper() {
        @Override
        public TableTxn txn() {
            @SuppressWarnings("serial")
            class EtcdTableTxn extends ArrayList<TxnComponent> implements TableTxn {
                EtcdTableTxn() {
                    super(4);
                }

                @Override
                public <T extends KVRecord> TableTxn set(KVTable.TableView<T> view, String key, T value) {
                    if (value == null) {
                        throw new NullPointerException("value");
                    }
                    return add(view, key, value, true);
                }

                @Override
                public <T extends KVRecord> TableTxn delete(KVTable.TableView<T> view, String key, T value) {
                    if (value == null) {
                        throw new NullPointerException("value");
                    }
                    return add(view, key, value, false);
                }

                @Override
                public <T extends KVRecord> TableTxn ensureAbsent(KVTable.TableView<T> view, String key) {
                    return add(view, key, null, false);
                }

                private <T extends KVRecord> TableTxn add(KVTable.TableView<T> view, String key, T value,
                        boolean put) {
                    if (view == null) {
                        throw new NullPointerException("view");
                    }
                    if (!(view instanceof TableView)) {
                        throw new IllegalArgumentException("incompatible KVTable type: " + view.getClass());
                    }
                    TableView<T> tv = (TableView<T>) view;
                    if (!isEmpty() && tv.getTable().client != get(0).view.getTable().client) {
                        throw new IllegalArgumentException("KVTables must have been created "
                                                           + "with the same EtcdClient");
                    }
                    add(new TxnComponent(tv, key, value, put? tv.valBs(value) : null));
                    return this;
                }

                @Override
                public boolean execute() throws Exception {
                    return execute(false) == null;
                }

                @Override
                public List<KVRecord> executeAndGet() throws Exception {
                    return execute(true);
                }

                private List<KVRecord> execute(boolean andGet) throws Exception {
                    final int n = size();
                    if (n == 0) {
                        return null;
                    }
                    KvClient kvc = get(0).view.getTable().client.getKvClient();
                    // build and execute transaction
                    FluentTxnRequest txn = kvc.txnIf();
                    FluentTxnSuccOps then = txn.then();
                    for (int i = 0; i < n; i++) {
                        final TxnComponent tc = get(i);
                        final KVRecord value = tc.value;
                        final long vers = value != null? value.getVersion() : 0L;
                        final ByteString key = tc.key;
                        txn = txn.cmpEqual(key).mod(vers == -1L? 0L : vers);
                        if (value == null) {
                            continue;
                        }
                        final ByteString val = tc.val;
                        if (val == null) {
                            then.delete(kvc.delete(key).asRequest());
                        } else {
                            then.put(kvc.put(key, val).asRequest())
                                    .get(kvc.get(key).asRequest());
                        }
                    }
                    if (andGet) {
                        FluentTxnOps<?> onFail = then.elseDo();
                        for (int i = 0; i < n; i++) {
                            onFail.get(kvc.get(get(i).key).asRequest());
                        }
                    }
                    final TxnResponse tr = txn.timeout(RangeCache.TIMEOUT_MS).sync();
                    if (!tr.getSucceeded()) {
                        if (!andGet) {
                            return Collections.emptyList();
                        }
                        final List<KVRecord> response = new ArrayList<>(n);
                        for (int i = 0; i < n; i++) {
                            final TxnComponent tc = get(i);
                            final TableRangeCache cache = tc.view.getTable().cache;
                            final RangeResponse rr = tr.getResponses(i).getResponseRange();
                            response.add(tc.view.makeRecord(rr.getKvsCount() > 0
                                    ? cache.offerUpdate(rr.getKvs(0))
                                    : cache.offerDelete(tc.key, tr.getHeader().getRevision())));
                        }
                        return response; // fail
                    }
                    for (int i = 0, j = 0; i < n; i++) {
                        final TxnComponent tc = get(i);
                        final TableRangeCache cache = tc.view.getTable().cache;
                        final KVRecord value = tc.value;
                        if (tc.val != null) {
                            KeyValue kv = tr.getResponses(++j).getResponseRange().getKvs(0);
                            cache.offerUpdate(kv);
                            setVersionAndReset(value, kv.getModRevision(), kv.getLease());
                        } else {
                            long modRev = tr.getHeader().getRevision();
                            cache.offerDelete(tc.key, modRev);
                            if (value != null) {
                                setVersionAndReset(value, -1L, 0L);
                            }
                        }
                        if (value != null) {
                            j++;
                        }
                    }
                    return null;
                }
            }
            return new EtcdTableTxn();
        }
    };
}
