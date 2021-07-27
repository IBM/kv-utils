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

import com.ibm.watson.kvutils.SessionNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is to allow us to set the internal data directly in the PersistentNode
 * instance without it attempting to update zookeeper (for when zookeeper is
 * updated separately)
 */
public class SettablePersistentNode extends PersistentNode implements SessionNode {

    private final AtomicReference<byte[]> pnData;

    @SuppressWarnings("unchecked")
    public SettablePersistentNode(CuratorFramework client, CreateMode mode,
            boolean useProtection, String basePath, byte[] initData) {
        super(client, mode, useProtection, basePath, initData);
        try {
            // get private superclass AtomicReference via reflection
            Field dataField = PersistentNode.class.getDeclaredField("data");
            dataField.setAccessible(true);
            pnData = (AtomicReference<byte[]>) dataField.get(this);
        } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Like {@link #setData(byte[])} but only updates the local copy, not zookeeper
     *
     * @param data
     */
    @Override
    public void setDefaultValue(byte[] data) {
        pnData.set(data);
    }
}
