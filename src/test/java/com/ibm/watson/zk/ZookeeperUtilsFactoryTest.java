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

import com.ibm.watson.kvutils.KVUtilsFactoryTest;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.BeforeClass;

public class ZookeeperUtilsFactoryTest extends KVUtilsFactoryTest {

    private static TestingServer ts;

    @BeforeClass
    public static void setup() throws Exception {
        ts = new TestingServer(true);
    }

    @Override
    public KVUtilsFactory getFactory() throws Exception {
        return new ZookeeperUtilsFactory(ts.getConnectString());
    }

    @Override
    public KVUtilsFactory getDisconnectedFactory() throws Exception {
        CuratorFramework cf2 = CuratorFrameworkFactory.newClient("localhost:1234",
                new BoundedExponentialBackoffRetry(80, 2000, 4));
        cf2.start();
        return new ZookeeperUtilsFactory(cf2);
    }
}
