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

import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTableTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

public class ZookeeperKVTableTest extends KVTableTest {

    @Test
    public void testZkKvTable() throws Exception {

        try (TestingServer ts = new TestingServer()) {
            ts.start();
            try (CuratorFramework cf = CuratorFrameworkFactory
                    .newClient(ts.getConnectString(),
                            new BoundedExponentialBackoffRetry(80, 2000, 4))) {
                cf.start();
                cf.blockUntilConnected();

                try (KVTable et = new ZookeeperKVTable(cf, "/testtc", 0)) {
                    testTable(et);

                    try (KVTable et2 = new ZookeeperKVTable(cf, "/testtc2/lower", 7)) {
                        testTableTxns(et, et2);
                    }

                }
            }
        }
    }


}
