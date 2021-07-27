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

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing">Rendezvous hashing</a>
 * implementation for group of participants which implements {@link IntPredicate}.
 *
 */
public interface Distributor extends Closeable, IntPredicate {

    /**
     * Distributor for a particular key type {@link T}, which implements
     * {@link Predicate}{@code <T>}.
     *
     * @param <T>
     */
    interface KeyDistributor<T> extends Predicate<T> {
        /**
         * @param key
         * @return the id of the participant currently associated with the
         *      provided key
         */
        String participantFor(T key);
    }

    /**
     * @param hashFunction
     * @param <T>
     * @return a {@link KeyDistributor} based on this {@code Distributor} and
     *      the provided hash function
     */
    default <T> KeyDistributor<T> forKeys(ToIntFunction<T> hashFunction) {
        return new KeyDistributor<T>() {
            @Override
            public boolean test(T key) {
                return Distributor.this.test(hashFunction.applyAsInt(key));
            }

            @Override
            public String participantFor(T key) {
                return Distributor.this.participantFor(hashFunction.applyAsInt(key));
            }
        };
    }

    /**
     * @return a {@code String} {@link KeyDistributor} based on a default
     * hash function
     */
    KeyDistributor<String> forStringKeys();

    /**
     * @param key an arbitrary string
     * @return true if this participant is assigned the given key
     */
    @Override
    default boolean test(int key) {
        String ourId = getId();
        return ourId != null && ourId.equals(participantFor(key));
    }

    /**
     * @param key
     * @return the id of the participant currently associated with the
     *      provided key
     */
    String participantFor(int key);

    /**
     * @return our participant id or null if in observer mode
     */
    String getId();

    /**
     * @return current number of participants
     */
    int getParticipantCount();

    /**
     * Begin participation in the distribution.
     *
     * @throws Exception
     */
    ListenableFuture<Distributor> start() throws Exception;

    /**
     * Close this instance, give up participation.
     * This Distributor instance can't be reused.
     */
    @Override
    void close() throws IOException;

}
