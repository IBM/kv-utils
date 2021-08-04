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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.kvutils.AbstractKVTable;
import com.ibm.watson.kvutils.KVRecord;
import com.ibm.watson.kvutils.KVTable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.IntPredicate;

/**
 * Basic concurrent DHT
 */
public class ZookeeperKVTable extends AbstractKVTable { //, Iterable<String> {

    public static final boolean FILTER_BUCKETS = false;

    private static final byte[] EMPTY = new byte[0];

    private final CuratorFramework curator;
    private final String tablePath;

    private final TreeCache cache;
    private final ListeningExecutorService executor;

    private final int defaultNumBuckets;
    private int numBuckets = -1;

    // these depend on numBuckets
    private int pathPrefixLen;
    private String[] bucketPaths; // null if numBuckets < 2
    protected Predicate<ChildData> BUC_FILTER;

    public ZookeeperKVTable(CuratorFramework curator,
            String path, int defaultNumBuckets) {
        this.curator = curator;
        tablePath = path;
        this.defaultNumBuckets = defaultNumBuckets;

        executor = MoreExecutors.listeningDecorator(Executors
                .newSingleThreadExecutor(TreeCache.defaultThreadFactory));

        // this also validates the path
        cache = TreeCache.newBuilder(curator, path).setExecutor(executor).build();
    }

    @Override
    public synchronized ListenableFuture<Boolean> start() {
        if (BUC_FILTER != null) {
            return Futures.immediateFailedFuture(new Exception("already started"));
        }
        // set just to ensure start isn't called multiple times
        BUC_FILTER = Predicates.alwaysFalse();

        final SettableFuture<Boolean> endFuture = SettableFuture.create();
        final ListenableFuture<Void> fut = executor.submit(() -> {
            numBuckets = getNumBuckets();
            if (numBuckets >= 2) {
                final int numDigits = Math.max(2, (int) (Math.log10(numBuckets) + 1));
                String buckFormat = String.format("bucket-%%0%dd-of-%0" + numDigits + 'd', numDigits, numBuckets);

                bucketPaths = new String[numBuckets];
                for (int i = 0; i < numBuckets; i++) {
                    bucketPaths[i] = ZKPaths.makePath(tablePath, String.format(buckFormat, i + 1));
                }

                BUC_FILTER = FILTER_BUCKETS? new Predicate<ChildData>() {
                    private final int bucstart = tablePath.length() + "/bucket-".length() + numDigits;
                    private final int bucend = bucstart + "-of-".length() + numDigits;
                    private final String SUFF = String.format("-of-%0" + numDigits + 'd', numBuckets);

                    @Override
                    public boolean apply(ChildData input) {
                        return SUFF.equals(input.getPath().substring(bucstart, bucend));
                    }
                } : null;
            } else {
                bucketPaths = null;
            }
            pathPrefixLen = (bucketPaths == null? tablePath : bucketPaths[0]).length() + 1;

            cache.getListenable().addListener(new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, TreeCacheEvent event) {
                    if (event.getType() == Type.INITIALIZED) {
                        endFuture.set(Boolean.TRUE);
                        cache.getListenable().removeListener(this);
                    }
                }
            });
            cache.start();
            return null;
        });
        fut.addListener(() -> {
            try {
                fut.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
                endFuture.setException(ee.getCause());
            }
        }, MoreExecutors.directExecutor());

        return endFuture;
    }

    protected int getNumBuckets() throws Exception {
        while (true) {
            try {
                byte[] nodeData = curator.getData().forPath(tablePath);
                try {
                    int numBuckets = Integer.parseInt(new String(nodeData).split("\n")[0]);
                    if (numBuckets < 0) {
                        throw new Exception("Invalid bucket count");
                    }
                    return numBuckets;
                } catch (Exception e) {
                    throw new Exception("Unable to determine number of buckets from table znode: " + tablePath);
                }
            } catch (KeeperException.NoNodeException nne) {
                try {
                    curator.create().creatingParentsIfNeeded()
                            .forPath(tablePath, Integer.toString(defaultNumBuckets)
                                    .getBytes(StandardCharsets.UTF_8));
                    return defaultNumBuckets;
                } catch (KeeperException.NodeExistsException nee) { // ok, continue
                }
            }
        }
    }

    @Override
    public int getCountAtLevel(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be greater than 0");
        }
        if (numBuckets <= 1) {
            level--;
        }
        int retval = 0;
        for (Iterator<ChildData> it = cache.childIterator(level); it.hasNext(); ) {
            retval += it.next().getStat().getNumChildren();
        }
        return retval;
    }


    @Override
    public void close() {
        cache.close();
        executor.shutdown();
    }

    @Override
    public <T extends KVRecord> TableView<T> getView(Serializer<T> ser, int level) {
        return new TableView<T>(ser, level);
    }

    class TableView<T extends KVRecord> extends AbstractTableView<T> {
        protected final int level;

        protected TableView(Serializer<T> ser, int level) {
            super(ser);
            this.level = level;
        }

        @Override
        public T get(String key) {
            if (!validKey(key)) {
                return null;
            }
            ChildData cd = cache.getCurrentData(keyToPath(key));
            return cd != null? makeRecord(cd) : null;
        }

        @Override
        public T getConsistent(String key) throws Exception {
            if (validKey(key)) {
                try {
                    final Stat stat = new Stat();
                    final String path = keyToPath(key);
                    byte[] data = curator.getData().storingStatIn(stat).forPath(path);
                    cache.offerUpdate(path, stat, data);
                    return makeRecord(data, stat, true);
                } catch (KeeperException.NoNodeException ok) {
                }
            }
            return null;
        }

        public T getConsistentNoStat(String key) throws Exception {
            if (validKey(key)) {
                try {
                    return makeRecord(curator.getData().forPath(keyToPath(key)), null, true);
                } catch (KeeperException.NoNodeException ok) {
                }
            }
            return null;
        }

        @Override
        public T getStrong(String key) throws Exception {
            curator.sync().forPath(keyToPath(key));
            return getConsistent(key);
        }

        /**
         * @param key
         * @param val
         * @return true if successful, false if was already there (no modification made)
         * @throws Exception
         */
        public boolean createIfAbsent(String key, T val) throws Exception {
            return createIfAbsent(key, val, val.getLeaseId() != 0L);
        }

        public boolean createIfAbsent(String key, T val, boolean ephemeral) throws Exception {
            if (!validKey(key)) {
                throw new IllegalArgumentException("Invalid key: " + key);
            }
            String parent = getParent(key), path = ZKPaths.makePath(parent, key);
            byte[] data = val != null? ser.serialize(val) : EMPTY;
            CreateMode createMode = ephemeral? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
            while (true) {
                try {
                    for (CuratorTransactionResult ctr : curator.inTransaction()
                            .create().withMode(createMode).forPath(path, EMPTY).and()
                            .setData().forPath(path, data).and().commit()) {
                        Stat stat = ctr.getResultStat();
                        if (stat != null) {
                            cache.offerCreate(path, stat, data);
                            if (val != null) {
                                setVersionAndReset(val, stat.getVersion(),
                                        stat.getEphemeralOwner(), stat);
                            }
                            break;
                        }
                    }
                    return true;
                } catch (KeeperException.NodeExistsException e) {
                    return false;
                } catch (KeeperException.NoNodeException e) {
                    // recursively create empty parents
                    int lastSlash = key.lastIndexOf('/');
                    if (lastSlash >= 0) {
                        createIfAbsent(key.substring(0, lastSlash), null, false);
                    } else {
                        try {
                            curator.create().creatingParentsIfNeeded().forPath(parent, EMPTY);
                        } catch (KeeperException.NodeExistsException ok) {
                        }
                    }
                    // continue while loop
                }
            }
        }

        /**
         * @param key
         * @param val
         * @return true if successful, false if version mismatch (no modification made)
         * @throws Exception
         */
        @Override
        public boolean conditionalSet(String key, T val) throws Exception {
            return conditionalSet(key, val, val.getLeaseId());
        }

        /**
         * @param key
         * @param val
         * @param leaseId for zk implementation, leaseId != 0L => ephemeral;
         *                only has effect if entry doesn't already exist
         * @return true if successful, false if version mismatch (no modification made)
         * @throws Exception
         */
        @Override
        public boolean conditionalSet(String key, T val, long leaseId) throws Exception {
            long version = val.getVersion();
            if (version == -1L) {
                return createIfAbsent(key, val, leaseId != 0L);
            }
            if (!validKey(key)) {
                throw new IllegalArgumentException();
            }
            try {
                String path = keyToPath(key);
                byte[] data = ser.serialize(val);
                Stat stat = curator.setData().withVersion((int) version).forPath(path, data);
                setVersionAndReset(val, cache.offerUpdate(path, stat, data).getVersion(),
                        stat.getEphemeralOwner(), stat);
                return true;
            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
                return false;
            }
        }

//		// these two methods are for doing transactions but break the abstraction a bit
//
//		public CuratorTransactionFinal put(CuratorTransaction trans, String key, T val) throws Exception {
//			if(!validKey(key)) throw new IllegalArgumentException("Invalid key: "+key);
//			String path = keyToPath(key);
//			return trans.create().forPath(path, EMPTY).and().setData().forPath(path, ser.serialize(val)).and();
//		}
//
//		public CuratorTransactionFinal conditionalSet(CuratorTransaction trans, String key, T val) throws Exception {
//			if(!validKey(key)) throw new IllegalArgumentException("Invalid key: "+key);
//			return trans.setData().withVersion((int)val.getVersion()).forPath(keyToPath(key), ser.serialize(val)).and();
//		}

//		//TODO TBD pass in rec or not?
//		//TODO TBD maybe need to return new rec, then how to reflect success?
//		//TODO document usage clearly (and resulting state after different outcomes)
//		public boolean performLockedOperation(String key, T rec, Operation<T> operation) throws Exception {
//			if(rec == null || rec.getStat() == null) rec = lazyGet(key);
//			if(rec.isLocked()) return false; //TODO TBD
//			rec.actionLock = System.currentTimeMillis();
//			rec.lockOwner = true;
//			if(!conditionalSet(key, rec)) return false; // collision //TODO throw or return?
//			boolean succ = false, condSetSucc = false;
//			try {
//				succ = operation.perform(key, rec); //TODO maybe wrap exception in ExecutionException to distinguish
//			} finally {
//				long myLockTime = rec.actionLock;
//				while(true) {
//					rec.unlock();
//					if(succ) rec = operation.updateRecord(key, rec);
//					// TODO should remove here be conditional?
//					condSetSucc = rec != null ?	conditionalSet(key, rec)
//							: ( unconditionalRemove(key) || true );
//					if(condSetSucc) break;
//					rec = get(key);
//					if(rec == null || rec.actionLock != myLockTime) break;
//				}
//			}
//			return succ && condSetSucc;
//		}

        @Override
        public void addListener(final boolean actionableOnly, final Listener<T> listener) {
            cache.getListenable().addListener((client, event) -> {
                Type etype = event.getType();
                ChildData cd = event.getData();
                if (cd != null) { // NODE_ADDED, NODE_UPDATED, NODE_REMOVED
                    String path = cd.getPath();
                    if (path == null || path.length() <= pathPrefixLen) {
                        return;
                    }
                    String key = path.substring(pathPrefixLen);
                    if (levelFromKey(key) != level) {
                        return;
                    }
                    T record = makeRecord(cd, !actionableOnly);
                    if (actionableOnly) {
                        if (!record.isActionableUpdate() &&
                            etype != Type.NODE_REMOVED) {
                            return;
                        }
                        resetActionableUpdate(record);
                    }
                    listener.event(convertCuratorEventType(etype), key, record);
                } else if (etype == Type.INITIALIZED) {
                    listener.event(EventType.INITIALIZED, null, null);
                }
                // else type is CONNECTION_LOST, CONNECTION_SUSPENDED or CONNECTION_RECONNECTED
            });
        }

        @SuppressWarnings("unused")
        protected Iterator<ChildData> cdIterator() {
            int itLevel = numBuckets > 1? level + 1 : level;
            Iterator<ChildData> cdIterator = cache.childIterator(itLevel);
            if (FILTER_BUCKETS && numBuckets > 1) {
                cdIterator = Iterators.filter(cdIterator, BUC_FILTER);
            }
            return cdIterator;
        }

        // cache vers
        @Override
        public Iterator<Entry<String, T>> iterator() {
            return Iterators.transform(cdIterator(), CD_TO_ENTRY);
        }

        @Override
        public Iterator<Entry<String, T>> distributedIterator(IntPredicate distributor) {
            return Iterators.transform(Iterators.filter(cdIterator(),
                    cd -> distributor.test(subhash(cd.getPath(), pathPrefixLen))), CD_TO_ENTRY);
        }

        @Override
        protected Iterator<String> keyIterator() {
            return Iterators.transform(cdIterator(), cd -> cd.getPath().substring(pathPrefixLen));
        }

        @Override
        public Iterator<Entry<String, T>> children(String key) {
            Iterator<ChildData> it = cache.getChildren(keyToPath(key));
            return it == null? null : Iterators.transform(it,
                    cd -> Maps.immutableEntry(node(cd.getPath()), makeRecord(cd)));
        }

        @Override
        public Map<String, T> childMap(String key) {
            Iterator<ChildData> it = cache.getChildren(keyToPath(key));
            if (it == null) {
                return null;
            }
            if (!it.hasNext()) {
                return Collections.emptyMap();
            }
            ImmutableMap.Builder<String, T> imb = ImmutableMap.builder();
            while (it.hasNext()) {
                ChildData cd = it.next();
                imb.put(node(cd.getPath()), makeRecord(cd));
            }
            return imb.build();
        }

        @Override
        public ZookeeperKVTable getTable() {
            return ZookeeperKVTable.this;
        }

        @Override
        protected int getLevel() {
            return level;
        }

        // ----- strong iterator support

        @Override
        public Iterable<Entry<String, T>> strongIterable(boolean canThrow) {
            if (numBuckets <= 1 && level == 1) {
                return super.strongIterable(canThrow);
            }
            throw new UnsupportedOperationException("only supported for non-bucketed first level");
        }

        @Override
        protected Iterator<Entry<String, T>> strongIterator() {
            try {
                Stat parentStat = getRootStat();
                if (parentStat == null || parentStat.getNumChildren() == 0) {
                    return Collections.emptyIterator();
                }
                final long pzxid = parentStat.getPzxid();
                // cache is fresh enough
                ChildData cacheParent = cache.getCurrentData(tablePath);
                if (cacheParent != null && cacheParent.getStat().getPzxid() >= pzxid) {
                    return iterator();
                }
                return Iterators.filter(Iterators.transform(ZookeeperKVTable.this.children(null).iterator(),
                        this::getEntryOrConsistentIfAbsent), Predicates.notNull());
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private Entry<String, T> getEntryOrConsistentIfAbsent(String key) {
            try {
                T val = get(key);
                if (val == null) {
                    val = getConsistent(key);
                }
                return val != null? Maps.immutableEntry(key, val) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error obtaining zookeeper record with key: " + key, e);
            }
        }

        private Stat getRootStat() throws Exception {
            curator.sync().forPath(tablePath);
            return curator.checkExists().forPath(tablePath);
        }


        // ----- utility methods

        final Function<ChildData, Entry<String, T>> CD_TO_ENTRY = cd -> {
            String key = cd.getPath().substring(pathPrefixLen);
            try {
                return Maps.immutableEntry(key, makeRecord(cd));
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing zookeeper record with key: " + key, e);
            }
        };

        protected byte[] serialize(T value) {
            return ser.serialize(value);
        }

        private T makeRecord(ChildData cd) {
            return makeRecord(cd.getData(), cd.getStat(), true);
        }

        private T makeRecord(ChildData cd, boolean reset) {
            return makeRecord(cd.getData(), cd.getStat(), reset);
        }

        private T makeRecord(byte[] data, Stat stat, boolean reset) {
            T val = ser.deserialize(data);
            if (reset) {
                resetActionableUpdate(val);
            }
            if (stat != null) {
                setVersion(val, stat.getVersion(),
                        stat.getEphemeralOwner(), stat);
            }
            return val;
        }
    }

    private static EventType convertCuratorEventType(Type curatorEventType) {
        switch (curatorEventType) {
        case NODE_ADDED:
            return EventType.ENTRY_ADDED;
        case NODE_UPDATED:
            return EventType.ENTRY_UPDATED;
        case NODE_REMOVED:
            return EventType.ENTRY_DELETED;
        case INITIALIZED:
            return EventType.INITIALIZED;
        case CONNECTION_LOST:
        case CONNECTION_RECONNECTED:
        case CONNECTION_SUSPENDED:
        default:
            return null;
        }
    }

    static int levelFromKey(String key) {
        int count = 1;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '/') {
                count++;
            }
        }
        return count;
    }

    static String node(String path) {
        int s = path.lastIndexOf('/');
        return s < 0? path : path.substring(s + 1);
    }

    @Override
    public boolean containsConsistent(String key) throws Exception {
        return validKey(key) && curator.checkExists().forPath(keyToPath(key)) != null;
    }

    @Override
    public boolean contains(String key) {
        return validKey(key) && cache.getCurrentData(keyToPath(key)) != null;
    }

    /**
     * @param key
     * @return false if was not found, true if removed
     * @throws Exception
     */
    @Override
    public boolean unconditionalDelete(String key) throws Exception {
        if (!validKey(key)) {
            return false;
        }
        String path = keyToPath(key);
        ChildData cdBefore = cache.getCurrentData(path);
        try {
            curator.delete().forPath(path);
            if (cdBefore != null) {
                cache.offerDelete(path, cdBefore.getStat().getCzxid());
            }
            return true;
        } catch (KeeperException.NoNodeException e) {
            return false; // no problem
        }
    }

    /**
     * @param key
     * @return true if was removed, false if version didn't match
     * @throws Exception
     */
    @Override
    public boolean delete(String key, long version) throws Exception {
        if (!validKey(key)) {
            return false;
        }
        String path = keyToPath(key);
        ChildData cdBefore = cache.getCurrentData(path);
        try {
            curator.delete().withVersion((int) version).forPath(path);
            if (cdBefore != null) {
                cache.offerDelete(path, cdBefore.getStat().getCzxid());
            }
        } catch (KeeperException.NoNodeException e) {
        } // consider as success
        catch (KeeperException.BadVersionException e) {
            return false;
        }
        return true;
    }


//	/**
//	 * @deprecated best to use iterable version
//	 */
//	public List<String> keys() throws Exception {
//		return Lists.newArrayList(iterator()); //checkedIterator());
//	}

//	/**
//	 * better if this could throw checked exception :-/
//	 */
//	@Override
//	public Iterator<String> iterator() {
//		try {
//			return checkedIterator();
//		} catch(Exception e) {
//			throw new RuntimeException(e);
//		}
//	}


//	public Iterator<String> checkedIterator() throws Exception {
//		if(numBuckets < 2) return childrenForPath(tablePath).iterator();
//		@SuppressWarnings("unchecked")
//		Iterator<String>[] lists = new Iterator[numBuckets];
//		for(int i=0;i<numBuckets;i++) lists[i] = childrenForPath(bucketPaths[i]).iterator();
//		return Iterators.concat(lists);
//	}

    //TODO maybe
//	public Iterator<String> lazyIterator() throws Exception {
//		return new Iterator<String>() {
//			int i = 0;
//			Iterator<String> cur = null;
//			@Override public boolean hasNext() {
//				if(cur != null) {
//				}
//				return false;
//			}
//			@Override public String next() {
//				return null;
//			}
//			@Override public void remove() {
//			}
//		};
//	}

    //TODO should this be here and should it be remote?
    @Override
    public List<String> children(String key) throws Exception {
        try {
            return curator.getChildren().forPath(keyToPath(key));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        }
    }


    private String keyToPath(String key) {
        return key == null || key.isEmpty() || "/".equals(key)
                ? tablePath : ZKPaths.makePath(getParent(key), key);
    }

    private String getParent(String key) {
        String parent;
        if (numBuckets < 2) {
            parent = tablePath;
        } else {
            int slash = key.indexOf('/');
            parent = bucketPaths[hash(slash >= 0? key.substring(0, slash) : key, numBuckets)];
        }
        return parent;
    }


//	//TODO document
//	public static interface Operation<T extends ZookeeperRecord> {
//		//TODO tbc whether to pass key, tbc whether to pass record
//		public boolean perform(String key, T record) throws Exception;
//		/**
//		 * should update record only, and not throw anything.
//		 * can return null, which means delete
//		 */
//		//TODO tbd whether to pass key or not
//		public T updateRecord(String key, T record);
//	}

    @Override
    public Helper getHelper() {
        return helper;
    }

    // --- statics

    private static boolean validKey(String key) {
        return key != null && !key.isEmpty() && key.indexOf(' ') == -1;
    }

    // WARNING no range checking
    protected static int subhash(String str, int start, int end) {
        int hash = 0;
        for (int i = start; i < end; i++) {
            hash = (hash << 5) - hash + str.charAt(i);
        }
        return hash;
    }

    protected static int subhash(String str, int start) {
        return subhash(str, start, str.length());
    }

    /**
     * Using an explicit hash function instead of
     * String.hashCode() to ensure no dependency on JVM vers
     */
    protected static int hash(String key, int base) {
        if (key == null) {
            return 0;
        }
        int hash = subhash(key, 0, key.length());
        return (hash >= 0? hash : -hash) % base;
    }

    // transaction helper

    static class TxnComponent {
        final TableView<?> view;
        final String key, path;
        final byte[] val; // val == null for delete
        final KVRecord value; // null for "ensureAbsent"

        int parents; // used only for ensureAbsent and create

        public TxnComponent(TableView<?> view, String key, KVRecord value, byte[] val) {
            this.view = view;
            this.key = key;
            this.path = view.getTable().keyToPath(key);
            this.val = val;
            this.value = value;
        }
    }

    static final Helper helper = new Helper() {
        @Override
        public TableTxn txn() {
            @SuppressWarnings("serial")
            class ZookeeperTableTxn extends ArrayList<TxnComponent> implements TableTxn {
                ZookeeperTableTxn() {
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
                    if (!validKey(key)) {
                        throw new IllegalArgumentException("Invalid key: " + key);
                    }
                    TableView<T> tv = (TableView<T>) view;
                    if (!isEmpty() && tv.getTable().curator != get(0).view.getTable().curator) {
                        throw new IllegalArgumentException("KVTables must have been created "
                                                           + "with the same CuratorFramework instance");
                    }
                    add(new TxnComponent(tv, key, value, put? tv.serialize(value) : null));
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
                    CuratorFramework client = get(0).view.getTable().curator;
                    // build and execute transaction
                    retry_txn:
                    for (boolean firstTry = true; ; firstTry = false) {
                        CuratorTransactionFinal txn = (CuratorTransactionFinal) client.inTransaction();
                        boolean failFast = false, hasSet = false, hasAbsent = false;
                        // need to keep track of earlier changes in the transaction
                        Set<String> created = null, deleted = null;
                        for (int i = 0; i < n; i++) {
                            TxnComponent tc = get(i);
                            final TreeCache cache = firstTry? tc.view.getTable().cache : null;
                            final KVRecord value = tc.value;
                            final String path = tc.path;
                            if (value == null) {
                                // ensure absent
                                tc.parents = 0;
                                String toCheck = path;
                                for (String parent; ; toCheck = parent, tc.parents++) {
                                    if (exists(parent = parent(toCheck), created, deleted, cache, client)) {
                                        break;
                                    }
                                }
                                // unfortunately this is the only way i can see to achieve this
                                txn = txn.create().forPath(toCheck, EMPTY).and()
                                        .delete().withVersion(0).forPath(toCheck).and();
                                hasAbsent = true;
                            } else {
                                final byte[] val = tc.val;
                                if (value.isNew()) {
                                    if (val == null) { // can't delete a new record
                                        failFast = true;
                                        break;
                                    }
                                    // create
                                    tc.parents = 0;
                                    LinkedList<String> parents = null;
                                    for (String toCheck = path; ; tc.parents++) {
                                        if (exists(toCheck = parent(toCheck), created, deleted, cache, client)) {
                                            break;
                                        }
                                        (parents != null? parents : (parents = new LinkedList<>())).push(toCheck);
                                    }
                                    if (parents != null) {
                                        while (!parents.isEmpty()) {
                                            String parent = parents.pop();
                                            txn = txn.create().forPath(parent, EMPTY).and();
                                            if (deleted == null || !deleted.remove(parent)) {
                                                (created != null? created : (created = new TreeSet<>())).add(parent);
                                            }
                                        }
                                    }
                                    txn = txn.create().forPath(path, EMPTY).and()
                                            .setData().forPath(path, tc.val).and();
                                    hasSet = true;
                                    if (deleted == null || !deleted.remove(path)) {
                                        (created != null? created : (created = new TreeSet<>())).add(path);
                                    }
                                } else {
                                    // delete or update
                                    final int vers = (int) value.getVersion();
                                    if (tc.val != null) { // update
                                        txn = txn.setData().withVersion(vers).forPath(path, val).and();
                                        hasSet = true;
                                    } else { //delete
                                        txn = txn.delete().withVersion(vers).forPath(path).and();
                                        if (created == null || !created.remove(path)) {
                                            (deleted != null? deleted : (deleted = new TreeSet<>())).add(path);
                                        }
                                        hasAbsent = true;
                                    }
                                }
                            }
                        }
                        if (!failFast) {
                            try {
                                Iterator<CuratorTransactionResult> it = txn.commit().iterator();
                                // success; update cache and record objects
                                List<TxnComponent> deletes = hasSet && hasAbsent? new ArrayList<>(size()) : null;
                                long txnZxid = -1L;
                                for (int i = 0; i < n; i++) {
                                    final TxnComponent tc = get(i);
                                    CuratorTransactionResult ctr = it.next();
                                    final KVRecord value = tc.value;
                                    if (tc.value == null) { //ensure absent
                                        it.next();
                                        if (deletes != null) {
                                            deletes.add(tc);
                                        }
                                        continue;
                                    }
                                    TreeCache cache = tc.view.getTable().cache;
                                    if (tc.val == null) {
                                        if (deletes != null) {
                                            deletes.add(tc);
                                        } else { //weaker
                                            Stat stat = (Stat) value.getKvMetadata();
                                            if (stat != null) {
                                                cache.offerDelete(tc.path, stat.getCzxid());
                                            }
                                        }
                                        setVersionAndReset(value, -1L, 0L, null);
                                    } else {
                                        boolean create = value.isNew();
                                        if (create) {
                                            for (int j = 0; j <= tc.parents; j++) {
                                                ctr = it.next();
                                            }
                                        }
                                        Stat stat = ctr.getResultStat();
                                        txnZxid = stat.getMzxid();
                                        if (create) {
                                            cache.offerCreate(tc.path, stat, tc.val);
                                            if (tc.parents > 0) {
                                                Stat parentStat = new Stat();
                                                DataTree.copyStat(stat, parentStat);
                                                parentStat.setVersion(0);
                                                parentStat.setDataLength(0);
                                                parentStat.setNumChildren(1); //TODO could this be !=1?
                                                parentStat.setCversion(1);
                                                String parent = tc.path;
                                                for (int j = 0; j < tc.parents; j++) {
                                                    cache.offerCreate(parent = parent(parent), parentStat, EMPTY);
                                                }
                                            }
                                        } else {
                                            cache.offerUpdate(tc.path, stat, tc.val);
                                        }
                                        setVersionAndReset(value, stat.getVersion(),
                                                stat.getEphemeralOwner(), stat);
                                    }
                                }
                                if (txnZxid >= 0L && deletes != null) {
                                    for (TxnComponent tc : deletes) {
                                        tc.view.getTable().cache.offerDelete(tc.path, txnZxid);
                                    }
                                }
                                return null; // all good
                            } catch (NoNodeException | NodeExistsException | BadVersionException ke) {
                                List<org.apache.zookeeper.OpResult> oprs = ke.getResults();
                                //System.out.println("OpResults: "+IntStream.range(0, oprs.size())
                                //     .mapToObj(i -> errCode(oprs, i)).collect(Collectors.toList()));
                                for (int i = 0, o = 0; i < n; i++) {
                                    final TxnComponent tc = get(i);
                                    Code c = errCode(oprs, o++);
                                    if (tc.value == null) {
                                        o++; // ensure absent, skip dummy delete
                                    } else if (tc.value.isNew()) {
                                        // create
                                        for (int j = 0; j < tc.parents; j++) {
                                            if (c == Code.NONODE || c == Code.NODEEXISTS) {
                                                continue retry_txn;
                                            }
                                            if (c != Code.OK) {
                                                throw ke; // unexpected
                                            }
                                            c = errCode(oprs, o++);
                                        }
                                        o++; // (skip setData)
                                    }
                                    // legit update or delete failures
                                    else if (c == Code.NONODE || c == Code.BADVERSION) {
                                        break retry_txn;
                                    }
                                    // common
                                    if (c == Code.NONODE) {
                                        continue retry_txn;
                                    }
                                    if (c == Code.NODEEXISTS) {
                                        break retry_txn; // legit txn fail
                                    }
                                    if (c != Code.OK) {
                                        throw ke; // unexpected
                                    }
                                }
                                throw ke; // should not reach here
                            }
                        }
                    }
                    // "legit" txn failure case
                    if (!andGet) {
                        return Collections.emptyList();
                    }
                    List<KVRecord> response = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        TxnComponent tc = get(i);
                        response.add(tc.view.getConsistent(tc.key));
                    }
                    return response;
                }
            }
            return new ZookeeperTableTxn();
        }
    };

    static String parent(String path) {
        int idx = path.lastIndexOf('/');
        return idx > 0? path.substring(0, idx) : "/";
    }

    static boolean exists(String path, Set<String> created, Set<String> deleted,
            TreeCache cache, CuratorFramework client) throws Exception {
        if ("/".equals(path) || created != null && created.contains(path)) {
            return true;
        }
        if (deleted != null && deleted.contains(path)) {
            return false;
        }
        if (cache == null) {
            return client.checkExists().forPath(path) != null;
        }
        return path.length() <= cache.getRootPath().length() || cache.getCurrentData(path) != null;
    }

    static Code errCode(List<org.apache.zookeeper.OpResult> oprs, int i) {
        org.apache.zookeeper.OpResult opr = oprs.get(i);
        return opr instanceof ErrorResult? Code.get(((ErrorResult) opr).getErr()) : Code.OK;
    }

}
