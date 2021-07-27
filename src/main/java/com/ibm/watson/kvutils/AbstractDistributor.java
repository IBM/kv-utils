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

import com.google.common.base.Strings;

/**
 * Distributor pattern based on rendezvous hashing
 */
public abstract class AbstractDistributor implements Distributor {

    protected static final Participant[] EMPTY_ARR = new Participant[0];

    protected final String id;

    // nonzero iff we are participating
    private volatile int ourHash;

    // does not include us
    protected volatile Participant[] participants = EMPTY_ARR;

    public AbstractDistributor(String participantId) {
        if (participantId != null && participantId.contains("\\n")) {
            throw new IllegalArgumentException("id can't contain linebreak");
        }
        id = Strings.emptyToNull(participantId);
    }

    @Override
    public KeyDistributor<String> forStringKeys() {
        return forKeys(AbstractDistributor::hash);
    }

    // default string hash function (don't use hashCode() to ensure non-JRE-specific)
    public static int hash(String key) {
        if (key == null) {
            return 0;
        }
        int len = key.length(), hash = 0;
        for (int i = 0; i < len; i++) {
            hash = (hash << 5) - hash + key.charAt(i);
        }
        return hash;
    }

    protected static int hash(int h1, int h2) {
        return xorshift32(h1 ^ h2);
    }

    protected static int xorshift32(int x) {
        /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
        x ^= x << 13;
        x ^= x >>> 17;
        x ^= x << 5;
        return x;
    }

    @Override
    public boolean test(int value) {
        if (id == null) {
            return false;
        }
        int ourHashNow = ourHash;
        if (ourHashNow == 0) {
            return false;
        }
        Participant[] parr = participants;
        if (parr.length == 0) {
            return true;
        }
        int ourCombinedHash = hash(ourHashNow, value);
        for (Participant p : parr) {
            int diff = Integer.compare(hash(p.hash, value), ourCombinedHash);
            if (diff > 0 || diff == 0 && p.id.compareTo(id) > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String participantFor(int value) {
        int ourHashNow = id != null ? ourHash : 0;
        boolean includeUs = ourHashNow != 0;
        Participant[] parr = participants;
        int len = parr.length;
        if (len == 0) {
            return includeUs ? id : null;
        }
        if (!includeUs && len == 1) {
            return parr[0].id;
        }
        int maxCombinedHash, first;
        String maxId;
        if (includeUs) {
            maxCombinedHash = hash(ourHashNow, value);
            maxId = id;
            first = 0;
        } else {
            maxCombinedHash = hash(parr[0].hash, value);
            maxId = parr[0].id;
            first = 1;
        }
        for (int i = first; i < len; i++) {
            Participant p = parr[i];
            int pCombinedHash = hash(p.hash, value);
            int diff = Integer.compare(pCombinedHash, maxCombinedHash);
            if (diff > 0 || diff == 0 && p.id.compareTo(maxId) > 0) {
                maxCombinedHash = pCombinedHash;
                maxId = p.id;
            }
        }
        return maxId;
    }

    protected final void updateParticipants(Participant[] participants, boolean includeUs) {
        if (id != null) {
            ourHash = includeUs ? hash(id) : 0;
        }
        this.participants = participants;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getParticipantCount() {
        return participants.length + (ourHash == 0 ? 0 : 1);
    }

    protected abstract void doClose();

    @Override
    public final synchronized void close() {
        doClose();
        ourHash = 0;
    }

    // ------ participant class

    protected static class Participant {
        public final String id;
        public final int hash;

        public Participant(String id) {
            this.id = id;
            hash = hash(id);
        }

        @Override
        public String toString() {
            return "P[id=" + id + ", hash=" + hash + "]";
        }
    }

}
