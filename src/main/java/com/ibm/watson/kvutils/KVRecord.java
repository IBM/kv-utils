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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/*
 * TODO!! PROBABLY RENAME TO KVTableRecord
 *
 */

public abstract class KVRecord extends JsonExtensible {

    /**
     * -1 means does not (yet) exist in store
     */
    private transient long version = -1L;

    private transient long leaseId;

    @JsonIgnore
    @Deprecated
    private transient Object kvMetadata;

    /**
     * this indicates whether listeners should be notified
     * of the current update (or initial addition)
     */
    @JsonProperty("actionable")
    protected boolean actionableUpdate;

    /**
     * Exposes store-specific metadata; only to help port
     * legacy ZookeeperTable usage
     */
    @JsonIgnore
    @Deprecated
    public Object getKvMetadata() {
        return kvMetadata;
    }

    @JsonIgnore
    public long getVersion() {
        return version;
    }

    @JsonIgnore
    public long getLeaseId() {
        return leaseId;
    }

    @JsonIgnore
    public void setActionableUpdate() {
        actionableUpdate = true;
    }

    @JsonIgnore
    public boolean isActionableUpdate() {
        return actionableUpdate;
    }

    @JsonIgnore
    public boolean isNew() {
        return version == -1L;
    }

    // these for table-internal use only

    @JsonIgnore
    void setVersion(long version, long leaseId) {
        this.version = version;
        this.leaseId = leaseId;
    }

    @Deprecated
    @JsonIgnore
    void setKvMetadata(Object kvMetadata) {
        this.kvMetadata = kvMetadata;
    }

    void resetActionableUpdate() {
        actionableUpdate = false;
    }

}
