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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class encapsulates "out of schema" data in json-serialized
 * data types, allowing unrecognized properties to survive deserialization
 * / re-serialization untouched, rather than causing a failure or being
 * suppressed.
 * <p>
 * This is particularly useful/important to support integrity of
 * model changes during upgrades - old instances won't wipe out
 * new properties added by newer instances when they update records.
 */
public abstract class JsonExtensible {
    private Map<String, Object> unknownAttrs;

    @JsonAnyGetter
    public Map<String, Object> getUnknown() {
        Map<String, Object> unknown = unknownAttrs;
        return unknown != null? unknown : Collections.emptyMap();
    }

    @JsonAnySetter
    public void setUnknown(String name, Object value) {
        // TreeMap because expected to be small
        if (unknownAttrs == null) {
            unknownAttrs = new TreeMap<>();
        }
        unknownAttrs.put(name, value);
    }

}
