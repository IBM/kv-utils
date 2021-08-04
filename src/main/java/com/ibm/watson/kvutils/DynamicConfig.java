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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.Set;

/**
 * String {@link Map} backed by remote kv-store.
 * <p/>
 * It must be {@linkplain #start() started} before use.
 */
public interface DynamicConfig extends Map<String, String>, AutoCloseable {

    @FunctionalInterface
    interface Listener {
        /**
         * @param key
         * @param value null if parameter has been removed
         */
        void configChanged(String key, String value);
    }

    /**
     * Listen for changes to any of the entries.
     *
     * @param listener
     */
    void addListener(Listener listener);

    /**
     * Listen for changes to some or all of the keys, first producing
     * an event for each key with its initial value.
     *
     * @param listener
     * @param forKeys set of keys to recieve events for, or {@code null}
     *       to recieve events for all keys.
     */
    void addAndInvokeListener(Listener listener, Set<String> forKeys);

    /**
     * Start this DynamicConfig, blocking until complete.
     *
     * @throws Exception
     */
    default void start() throws Exception {}

    /**
     * Start this DynamicConfig asynchronously if possible.
     *
     * @throws Exception
     */
    default ListenableFuture<Boolean> startAsync() {
        try {
            start();
            return Futures.immediateFuture(Boolean.TRUE);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    /**
     * Convenience - same as {@link #get(Object)} but with default
     *
     * @param key
     * @param defaultValue
     * @return
     */
    @Override
    default String getOrDefault(Object key, String defaultValue) {
        String v = get(key);
        return v != null? v : defaultValue;
    }
}
