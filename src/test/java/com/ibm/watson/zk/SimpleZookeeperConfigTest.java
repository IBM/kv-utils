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

import com.ibm.watson.kvutils.DynamicConfig;
import com.ibm.watson.kvutils.DynamicConfig.Listener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class SimpleZookeeperConfigTest {

    @Test
    public void testSimpleZookeeperConfig() throws Exception {

        String configLoc = "/myconfig";

        try (TestingServer testzk = new TestingServer();
             CuratorFramework cf = newCurator(testzk.getConnectString());
             CuratorFramework ccf = newCurator(testzk.getConnectString()); // use different session for updates
             DynamicConfig config = new SimpleZookeeperConfig(ccf, configLoc)) {

            assertNull(cf.checkExists().forPath(configLoc));

            try {
                config.get("mykey");
                fail("should throw before starting");
            } catch (IllegalStateException ise) {
            }

            final List<String> events =
                    Collections.synchronizedList(new ArrayList<String>());

            config.addListener(new Listener() {
                @Override
                public void configChanged(String key, String value) {
                    events.add(key);
                    events.add(value);
                }
            });

            config.start();

            Thread.sleep(80);
            assertNull(config.get("mykey"));

            // ensure the root config node isn't created as a side effect
            assertNull(cf.checkExists().forPath(configLoc));

            cf.create().creatingParentsIfNeeded().forPath(configLoc + "/mykey", "myval".getBytes());

            Thread.sleep(80);
            assertEquals("myval", config.get("mykey"));

            cf.setData().forPath(configLoc + "/mykey", "mynewval".getBytes());
            Thread.sleep(80);

            assertEquals("mynewval", config.get("mykey"));

            assertNull(config.get("nonexist"));

            assertEquals("defaultval", config.getOrDefault("nonexist", "defaultval"));

            cf.create().forPath(configLoc + "/myotherkey", "myotherval".getBytes());
            cf.create().forPath(configLoc + "/myotherkey/invalid", "something".getBytes());

            cf.create().forPath("/someothernode", "something".getBytes());

            Thread.sleep(100);
            assertEquals(events, Arrays.asList("mykey", "myval", "mykey", "mynewval", "myotherkey", "myotherval"));
        }
    }

    public static CuratorFramework newCurator(String connStr) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(connStr).retryPolicy(retryPolicy).canBeReadOnly(true)
                .sessionTimeoutMs(5000).connectionTimeoutMs(2000).build();
        curator.start();
        return curator;
    }

}
