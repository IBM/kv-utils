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

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.utils.PersistentLeaseKey;
import com.ibm.etcd.client.utils.RangeCache;
import com.ibm.watson.kvutils.SessionNode;

/**
 *
 */
public class EtcdSessionNode extends PersistentLeaseKey implements SessionNode {

    public EtcdSessionNode(EtcdClient client, ByteString key,
            ByteString defaultValue, RangeCache rangeCache) {
        super(client, key, defaultValue, rangeCache);
    }

    @Override
    public void setDefaultValue(byte[] data) {
        setDefaultValue(ByteString.copyFrom(data));
    }
}
