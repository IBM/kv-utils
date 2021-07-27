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

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * For use by {@link KVTable} implementors.
 */
public abstract class AbstractKVTable implements KVTable {

    protected static final Logger logger = LoggerFactory.getLogger(KVTable.class);

    protected static <T extends KVRecord> T setVersion(T rec, long version, long leaseId) {
        rec.setVersion(version, leaseId);
        return rec;
    }

    protected static <T extends KVRecord> T setVersionAndReset(T rec, long version, long leaseId) {
        rec.setVersion(version, leaseId);
        rec.resetActionableUpdate();
        return rec;
    }

    /**
     * @deprecated use {@link #setVersion(KVRecord, long, long)}
     */
    @Deprecated
    protected static <T extends KVRecord> T setVersion(T rec, long version,
            long leaseId, Object kvMetadata) {
        rec.setVersion(version, leaseId);
        rec.setKvMetadata(kvMetadata);
        return rec;
    }

    /**
     * @deprecated use {@link #setVersionAndReset(KVRecord, long, long)}
     */
    @Deprecated
    protected static <T extends KVRecord> T setVersionAndReset(T rec, long version,
            long leaseId, Object kvMetadata) {
        rec.setVersion(version, leaseId);
        rec.setKvMetadata(kvMetadata);
        rec.resetActionableUpdate();
        return rec;
    }

    protected static void resetActionableUpdate(KVRecord rec) {
        rec.resetActionableUpdate();
    }

    protected abstract static class AbstractTableView<T extends KVRecord> implements TableView<T> {

        private final Map<String, T> mapView;
        private final Set<Entry<String, T>> entrySet;

        private final Iterable<Entry<String, T>> noThrowIterable, strongNoThrowIterable;

        protected final Serializer<T> ser;

        protected AbstractTableView(Serializer<T> ser) {
            this.ser = ser;
            this.entrySet = new AbstractSet<Entry<String,T>>() {
                @Override
                public Iterator<Entry<String, T>> iterator() {
                    return AbstractTableView.this.iterator();
                }

                @Override
                public int size() {
                    return getCount();
                }
            };
            this.mapView = new AbstractMap<String, T>() {
                @Override
                public Set<Entry<String, T>> entrySet() {
                    return entrySet;
                }

                @Override
                public Set<String> keySet() {
                    return new AbstractSet<String>() {
                        @Override
                        public Iterator<String> iterator() {
                            return AbstractTableView.this.keyIterable().iterator();
                        }

                        @Override
                        public int size() {
                            return getCount();
                        }
                    };
                }
            };
            this.noThrowIterable = insulatedIterable(this::iterator);
            this.strongNoThrowIterable = insulatedIterable(this::strongIterator);
        }

        protected abstract int getLevel();

        protected abstract Iterator<String> keyIterator();

        protected abstract Iterator<Entry<String, T>> strongIterator();

        protected abstract Iterator<Entry<String, T>> distributedIterator(IntPredicate distributor);

        @Override
        public int getCount() {
            return getTable().getCountAtLevel(getLevel());
        }

        @Override
        public Map<String, T> asReadOnlyMap() {
            return mapView;
        }

        @Override
        public Iterable<Entry<String, T>> noThrowIterable() {
            return noThrowIterable;
        }

        @Override
        public Iterable<Entry<String, T>> strongIterable(boolean canThrow) {
            return canThrow? this::strongIterator : strongNoThrowIterable;
        }

        @Override
        public Iterable<Entry<String, T>> distributedIterable(IntPredicate distributor, boolean canThrow) {
            return canThrow? () -> distributedIterator(distributor) : distributedIterable(distributor, true);
        }

        @Override
        public Iterable<String> keyIterable() {
            return this::keyIterator;
        }
    }

    protected static <U> Iterable<U> insulatedIterable(Iterable<U> iterable) {
        return () -> new AbstractIterator<U>() {
            final Iterator<U> baseIt = iterable.iterator();

            @Override
            protected U computeNext() {
                //TODO maybe safeguard to ensure against infinite loop
                while (baseIt.hasNext()) {
                    try {
                        return baseIt.next();
                    } catch (Exception e) {
                        logger.error("KVTable iteration skipping record due to exception", e);
                    }
                }
                return endOfData();
            }
        };
    }

    protected static class OpResult<T> implements Operation.Result<T> {
        private final boolean done;
        private final T value;

        public OpResult(boolean done, T value) {
            this.done = done;
            this.value = value;
        }

        @Override
        public boolean done() {
            return done;
        }

        @Override
        public T value() {
            return value;
        }
    }

}
