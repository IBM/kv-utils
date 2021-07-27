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
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.KVTableTest;
import org.junit.Ignore;
import org.junit.Test;

@Ignore // etcd server not yet set up in CI
public class EtcdKVTableTest extends KVTableTest {

    @Test
    public void testEtcdTable() throws Exception {

        EtcdClient client = EtcdClient.forEndpoint("localhost", 2379).withPlainText().build();
        try (KVTable et = new EtcdKVTable(client, "/mytable/")) {
            testTable(et);

            client.getKvClient().delete(ByteString.copyFromUtf8("/mytable/"))
                    .asPrefix().sync();
            client.getKvClient().delete(ByteString.copyFromUtf8("/myothertable/"))
                    .asPrefix().sync();

            // test transactions
            try (KVTable et2 = new EtcdKVTable(client, "/myothertable/")) {
                testTableTxns(et, et2);
            }
        }
    }
}
