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
import com.ibm.watson.kvutils.SharedCounter;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Ignore // etcd server not currently set up in CI
public class EtcdSharedCountTest {

    @Test
    public void test_etcd_shared_count() throws Exception {

        EtcdClient client1 = EtcdClient.forEndpoint("localhost", 2379).withPlainText().build();
        EtcdClient client2 = EtcdClient.forEndpoint("localhost", 2379).withPlainText().build();

        client1.getKvClient().delete(ByteString.copyFromUtf8("mycounter-a")).sync();
        client1.getKvClient().delete(ByteString.copyFromUtf8("mycounter-b")).sync();

        try (SharedCounter sc1 = new EtcdSharedCounter(client1, ByteString.copyFromUtf8("mycounter-a"), 5);
             SharedCounter sc2 = new EtcdSharedCounter(client2, ByteString.copyFromUtf8("mycounter-a"), 66)) {

            assertEquals(5, sc1.getCurrentCount());
            assertEquals(6, sc1.getNextCount());
            assertEquals(7, sc2.getNextCount());
            assertEquals(8, sc1.getNextCount());
            assertEquals(8, sc1.getCurrentCount());
            assertEquals(9, sc1.getNextCount());
            assertEquals(10, sc2.getNextCount());
        }
        try (SharedCounter scb = new EtcdSharedCounter(client1, ByteString.copyFromUtf8("mycounter-b"), 20)) {
            assertEquals(21, scb.getNextCount());
            assertEquals(21, scb.getCurrentCount());
        }
    }

}
