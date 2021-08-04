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
package com.ibm.watson.kvutils;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.watson.kvutils.AbstractKVTable.OpResult;
import com.ibm.watson.kvutils.KVTable.Helper.TableTxn;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

/**
 * A consistent key-value cache which is automatically kept in sync
 * with a centralized KV store such as etcd or zookeeper.
 * <p>
 * Provides atomic CAS-style update functions which write-through to
 * the centralized store.
 * <p>
 * Values are assumed to be of some type T that extends {@link KVRecord}.
 * Serialization/deserialization of these values is handled via a provided
 * {@link Serializer}, typically the included {@link JsonSerializer}. Null
 * values are not permitted (null represents absence).
 * <p>
 * Listeners can be attached which are notified of any changes to the
 * cache's state.
 * <p>
 * <b>Usage:</b> When performing conditional sets/updates, the "expected"
 * version is assumed to be whatever it was when the KVRecord value
 * was obtained from the cache, <i>or</i> was last updated via a
 * <b>successful</b> cache update. If the record was instead constructed
 * by the user, then it's "expected" to be absent.
 * <p>
 * If a conditional update or transaction fails, the record(s) used should be
 * discarded and fresh one(s) obtained from the store. The most efficient way
 * of doing this is to use the {@link TableView#conditionalSetAndGet(String, KVRecord)},
 * {@link TableView#conditionalUpdate(String, Operation)}, or
 * {@link TableTxn#executeAndGet()} methods, which return fresh records after
 * failures as part of the same operation.
 * <p>
 * If a conditional update or transaction succeeds:
 * <ol>
 * <li>The non-deleted value objects used can be considered "current", and
 * used as-is in subsequent operations (their internal version is updated
 * automatically)</li>
 * <li>Subsequent "weak" {@link TableView#get(String)} calls for the
 * corresponding keys are guaranteed to reflect the updates made (they
 * might return a newer version but it won't be older)</li>
 * </ol>
 * <p>
 * <code>null</code> {@code KVRecord}s returned from conditional updates
 * reflect current deletion/absence of the corresponding entry.
 */
public interface KVTable extends Closeable {

    /**
     * Used in the context of a {@link TableView} to convert between
     * bytes and records.
     *
     * @param <T>
     */
    interface Serializer<T> {
        byte[] serialize(T obj);

        T deserialize(byte[] data);

        default T deserialize(InputStream in) {
            try {
                return deserialize(ByteStreams.toByteArray(in));
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO
            }
        }

        default void serialize(T obj, OutputStream out) {
            try {
                out.write(serialize(obj));
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO
            }
        }
    }

    /**
     * Event types of which {@link Listener}s are notified.
     */
    enum EventType {
        INITIALIZED, ENTRY_ADDED, ENTRY_DELETED, ENTRY_UPDATED
    }

    /**
     * Listener for changes to the table records
     *
     * @param <T>
     */
    @FunctionalInterface
    interface Listener<T extends KVRecord> {
        void event(EventType type, String key, T record);
    }

    /**
     * Encapsulates logic to optionally perform an update to a table
     * record from a provided starting value. Used in the conditional
     * update methods, where it could be invoked zero or more times
     * per method invocation (but won't be invoked again after returning
     * false).
     *
     * @param <T>
     */
    @FunctionalInterface
    interface Operation<T> {
        /**
         * Result from a conditional update operation.
         *
         * @param <T>
         */
        interface Result<T> {
            /**
             * @return true if the update was successful,
             * false otherwise - either the record no longer
             * exists or the provided {@link Operation#update(Object)}
             * implementation returned false.
             */
            boolean done();

            /**
             * @return new record value following the update
             * attempt. null if the record no longer exists
             */
            T value();
        }

        /**
         * Optionally make updates to the provided record
         * value, which <i>won't</i> be null. Returning
         * false will cancel the operation and no update
         * will be made.
         *
         * @param value
         * @return true to attempt to commit the update,
         * false to abort the overall conditional update operation.
         * @throws Exception
         */
        boolean update(T value) throws Exception;
    }

    /**
     * A view on the table with methods for accessing records via a particular
     * {@link Serializer}.
     *
     * @param <T>
     */
    interface TableView<T extends KVRecord> extends Iterable<Entry<String, T>> {

        /**
         * Get a value from the cache
         *
         * @param key
         * @return T the value, or null if not present
         */
        T get(String key);

        /**
         * Get a value from the remote store. The cache is guaranteed to reflect at least
         * the returned version in subsequent accesses.
         *
         * @param key
         * @return T the value, or null if not present
         */
        T getConsistent(String key) throws Exception;

        /**
         * Get a value from the remote store, ensuring global sequential consistency. The
         * cache is guaranteed to reflect at least the returned version in subsequent accesses.
         *
         * @param key
         * @return T the value, or null if not present
         */
        T getStrong(String key) throws Exception;

        /**
         * Get a value from the cache, and if not present attempt to get the value from
         * the remote store. This is intended to be used when the entry is expected
         * to be present but might have been created by someone else.
         *
         * @param key
         * @return T the value, or null if not present
         */
        default T getOrStrongIfAbsent(String key) throws Exception {
            T val = get(key);
            return val != null? val : getStrong(key);
        }

        /**
         * Atomically creates or updates a record in the store, conditional on its
         * version matching that at the time the record was obtained prior to
         * modification. If val was constructed rather than obtained from the table,
         * this is effectively putIfAbsent.
         * <p>
         * If this method returns false, val is no longer valid and shouldn't be used.
         *
         * @param key
         * @param val
         * @return true if successful, false if version mismatch (no modification made)
         * @throws Exception
         */
        boolean conditionalSet(String key, T val) throws Exception;
        // this would be a good default but no way to enforce that
        // *at least one* of the two methods is implemented
        //{
        //    return conditionalPutAndGet(key, val) == val;
        //}

        default boolean conditionalSet(String key, T val, long leaseId) throws Exception {
            return conditionalSet(key, val);
        }

        /**
         * Atomically creates or updates a record in the store, conditional on its
         * version matching that at the time the record was obtained prior to
         * modification. If val was constructed rather than obtained from the table,
         * this is effectively putIfAbsent.
         * <p>
         * If the conditional update fails (this method returns a value != val), val
         * should no longer be used - the returned record should be used instead.
         *
         * @param key
         * @param val the record to set
         * @return == val if success, otherwise the conflicting value (or null if absent)
         */
        default T conditionalSetAndGet(String key, T val) throws Exception {
            return conditionalSet(key, val)? val : getConsistent(key);
        }

        default T conditionalSetAndGet(String key, T val, long leaseId) throws Exception {
            return conditionalSet(key, val, leaseId)? val : getConsistent(key);
        }

        /**
         * Deletes a record from the store, conditional on it having the same version
         * as when it was obtained.
         *
         * @param key
         * @param val
         * @return true if the delete was successful, false otherwise
         * @throws Exception
         */
        default boolean conditionalDelete(String key, T val) throws Exception {
            long vers = val.getVersion();
            if (vers == -1L) {
                return false;
            }
            boolean result = getTable().delete(key, vers);
            if (result) {
                val.setVersion(-1L, 0L);
                val.setKvMetadata(null);
                val.resetActionableUpdate();
            }
            return result;
        }

        /**
         * Deletes a record from the store with the specified key unconditionally.
         *
         * @param key
         * @return false if was not found, true if removed
         * @throws Exception
         */
        default boolean unconditionalDelete(String key) throws Exception {
            return getTable().unconditionalDelete(key);
        }

        /**
         * Checks store for existence of record with specified key.
         *
         * @param key
         * @return true if a record exists, false otherwise
         * @throws Exception
         */
        default boolean containsConsistent(String key) throws Exception {
            return getTable().containsConsistent(key);
        }

        /**
         * Checks local cache for existence of record with specified key.
         *
         * @param key
         * @return true if a record exists, false otherwise
         * @throws Exception
         */
        default boolean contains(String key) {
            return getTable().contains(key);
        }

        /**
         * Convenience method for performing a transactional update
         * to an <b>existing</b> table record. The provided {@link Operation}
         * inspects a version of the record and decides whether or not to update it.
         * If an update is required it should modify the record accordingly
         * and return true, and otherwise return false.
         * <p>
         * The operation might be invoked multiple times as needed to ensure
         * a consistent update to the store.
         * <p>
         * Note this method can't be used to update (create) nonexistent records
         * or to delete records.
         *
         * @param key
         * @param updateOperation return false to abort any update, or
         *                        true after making desired modifications to passed-in record
         * @return result of the operation - see {@link Operation.Result}
         * @throws Exception
         */
        default Operation.Result<T> conditionalUpdate(String key,
                Operation<T> updateOperation) throws Exception {
            return conditionalUpdate(key, null, updateOperation);
        }

        /**
         * More efficient version of {@link #conditionalUpdate(String, Operation)}
         * for when a starting value for the record is already in hand.
         *
         * @param key
         * @param value           starting value from cache (<b>shouldn't be used
         *                        after this invocation</b>), or null to first retrieve the value
         * @param updateOperation return false to abort any update, or
         *                        true after making desired modifications to passed-in record
         * @return result of the operation - see {@link Operation.Result}
         * @throws Exception from provided update operation or remote cache write
         */
        default Operation.Result<T> conditionalUpdate(String key, T value,
                Operation<T> updateOperation) throws Exception {
            if (value == null) {
                value = getOrStrongIfAbsent(key);
                if (value == null) {
                    return new OpResult<>(false, null);
                }
            }
            while (true) {
                if (!updateOperation.update(value)) {
                    return new OpResult<>(false, value);
                }
                T newValue = conditionalSetAndGet(key, value);
                if (newValue == null) {
                    return new OpResult<>(false, null);
                }
                if (newValue == value) {
                    return new OpResult<>(true, value);
                }
                value = newValue;
            }
        }

        /**
         * @param actionableOnly if true, listener events will only fire for removed
         *                       entries, or those with the "actionable" flag set prior to their addition
         *                       or last update
         * @param listener
         */
        void addListener(boolean actionableOnly, Listener<T> listener);

        /**
         * Not supported by non-hierarchical stores.
         *
         * @param key
         * @return {@link Entry} iterator over children of key
         */
        Iterator<Entry<String, T>> children(String key);

        /**
         * Not supported by non-hierarchical stores.
         *
         * @param key
         * @return {@link Map} of children of key
         */
        Map<String, T> childMap(String key);

        /**
         * @return the underlying {@link KVTable}
         */
        KVTable getTable();

        /**
         * @return the number of entries in this view
         */
        int getCount();

        /**
         * @return a <i>view</i> of this TableView as a read-only {@link Map}
         */
        Map<String, T> asReadOnlyMap();

        /**
         * This method can be used to skip over corrupt/badly serialized records
         * rather than failing mid-iteration, so that processing of all other
         * records isn't affected.
         *
         * @return an {@link Iterable} whose Iterators' next() method
         * will log but not throw exceptions from record deserialization
         */
        Iterable<Entry<String, T>> noThrowIterable();

        /**
         * Provides Iterators whose contents are guaranteed to be globally
         * consistent as of the point in time that they're created. Note,
         * this guarantee only applies to the <i>existence</i> of entries.
         * The <i>contents</i> of the returned records doesn't have the same
         * guarantee if they have been updated concurrently.
         *
         * @param canThrow if false then iterators will log and skip
         *                 any record deserialization errors (like {@link #noThrowIterable()})
         *                 instead of throwing
         * @return an {@link Iterable} whose contents is guaranteed to be
         * globally sequentially consistent
         */
        Iterable<Entry<String, T>> strongIterable(boolean canThrow);

        /**
         * @param canThrow if false then iterators will log and skip
         *                 any record deserialization errors (like {@link #noThrowIterable()})
         * @return an {@link Iterable} spanning a subset of the table keys
         * determined by the provided {@link IntPredicate} (typically
         * a {@link Distributor}).
         */
        Iterable<Entry<String, T>> distributedIterable(IntPredicate distributor, boolean canThrow);

        /**
         * @return an {@link Iterable} over the String keys of this table view
         */
        Iterable<String> keyIterable();
    }

    /**
     * Start table asynchronously
     *
     * @return future which completes when table is fully intitialized
     * (populated with initial snapshot), or upon failure (with an exception)
     */
    ListenableFuture<Boolean> start();

    /**
     * This blocks until the table is fully initialized (fully populated
     * with initial snapshot)
     *
     * @throws Exception
     */
    default void start(long timeout, TimeUnit unit) throws Exception {
        boolean started = false;
        try {
            start().get(timeout, unit);
            started = true;
        } catch (ExecutionException ee) {
            Throwables.throwIfInstanceOf(ee.getCause(), Exception.class);
            Throwables.throwIfUnchecked(ee.getCause());
            throw new RuntimeException(ee.getCause());
        } finally {
            if (!started) {
                close();
            }
        }
    }

    int getCountAtLevel(int level);

    /**
     * Get a view of the table at a particular level, assuming all entries
     * at that level are of type T. Non-hierarchical stores (such as etcd 3+),
     * only support level == 1.
     *
     * @param ser   the {@link Serializer} to use for records exposed by this view.
     * @param level
     * @return view backed by this table
     */
    <T extends KVRecord> TableView<T> getView(Serializer<T> ser, int level);

    /**
     * Obtain an instance of a helper class associated with this table's back-end
     * kv store connection.
     *
     * @return
     */
    Helper getHelper();

    boolean containsConsistent(String key) throws Exception;

    boolean contains(String key);

    /**
     * @param key
     * @return false if was not found, true if removed
     * @throws Exception
     */
    boolean unconditionalDelete(String key) throws Exception;

    /**
     * @param key
     * @return true if was removed, false if version didn't match
     * @throws Exception
     */
    boolean delete(String key, long version) throws Exception;

    /**
     * Not supported by non-hierarchical stores.
     *
     * @param key
     * @return list of keys of children of specified key
     * @throws Exception
     */
    List<String> children(String key) throws Exception;


    /**
     * Provides method for building and executing intra- and inter-KVTable transactions.
     */
    interface Helper {
        interface TableTxn {
            <T extends KVRecord> TableTxn set(TableView<T> view, String key, T value);

            <T extends KVRecord> TableTxn delete(TableView<T> view, String key, T value);

            <T extends KVRecord> TableTxn ensureAbsent(TableView<T> view, String key);

            /**
             * @return true if transaction was successful, false otherwise
             */
            boolean execute() throws Exception;

            /**
             * @return null if the transaction was successful, otherwise
             * a list of fresh {@link KVRecord} values corresponding to the
             * keys used in the transaction (in the same order). null entries
             * in the list indicate the corresponding record is now deleted/absent.
             */
            List<KVRecord> executeAndGet() throws Exception;

            /**
             * @return the number of operations which have been added to this transaction
             */
            int size();
        }

        /**
         * Construct a transaction of table operations comprising multiple entries
         * in the same and/or different {@link KVTable}s. If the transaction
         * spans multiple tables, they must have come from the same factory/client.
         * <p>
         * The transaction will either succeed or fail in full, in the failure case
         * no modifications to store records will have been made.
         */
        TableTxn txn();
    }
}
