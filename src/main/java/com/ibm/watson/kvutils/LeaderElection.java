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

import java.io.Closeable;
import java.io.IOException;

/**
 * A common leader election interface.
 */
public interface LeaderElection extends Closeable {

    interface ElectionListener {
        /**
         * Called if our leadership status changes
         *
         * @param isLeader true if we are the leader, false otherwise
         */
        void leadershipChange(boolean isLeader);
    }

    /**
     * @return our candidate id, or null if in observer-only mode
     */
    String getId();

    /**
     * @return the leader's id or null if unavailable
     */
    String getLeaderId() throws Exception;

    /**
     * @return true if we are the leader, false otherwise
     */
    boolean isLeader();

    //TODO executor TBD
    void addListener(ElectionListener listener);

    /**
     * Declare candidacy if we are a participant (id not null),
     * or else just start watching ({@link #getLeaderId()} might
     * not work prior to calling this method)
     *
     * @throws Exception
     */
    void start() throws Exception;

    /**
     * Close this instance, give up candidacy or leadership.
     * This LeaderElection instance can't be reused
     */
    @Override
    void close() throws IOException;

}
