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

import com.ibm.etcd.client.EtcdClient;
import com.ibm.watson.kvutils.Distributor;
import com.ibm.watson.kvutils.DistributorTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static com.ibm.etcd.client.KeyUtils.bs;

@Ignore // etcd server not currently set up in CI
public class EtcdDistributorTest extends DistributorTest {

    private static EtcdClient client;

    @BeforeClass
    public static void setup() {
        client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
    }

    @AfterClass
    public static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @Override
    public void testDistributor() throws Exception {
        super.testDistributor();
    }

    @Override
    protected Distributor newDistributor(String path, String id) {
        return new EtcdDistributor(client, bs(path), id);
    }

}
