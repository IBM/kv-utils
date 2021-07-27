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
import com.ibm.watson.kvutils.KVUtilsFactoryTest;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore // etcd server not currently set up in CI
public class EtcdUtilsFactoryTest extends KVUtilsFactoryTest {

    @BeforeClass
    public static void setup() {
        // cleanup
        try (EtcdClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {
            for (String p : PATHS_TO_TEST) {
                client.getKvClient().delete(ByteString.copyFromUtf8(p)).sync();
            }
        }
    }

    @Override
    public KVUtilsFactory getFactory() throws Exception {
        return new EtcdUtilsFactory("http://localhost:2379");
    }

    @Override
    public KVUtilsFactory getDisconnectedFactory() throws Exception {
        return new EtcdUtilsFactory("localhost:1234"); // nothing listening on this port
    }
}
