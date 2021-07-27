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
package com.ibm.watson.kvutils.factory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.watson.kvutils.Distributor;
import com.ibm.watson.kvutils.DynamicConfig;
import com.ibm.watson.kvutils.KVTable;
import com.ibm.watson.kvutils.LeaderElection;
import com.ibm.watson.kvutils.SessionNode;
import com.ibm.watson.kvutils.SharedCounter;
import com.ibm.watson.zk.ZookeeperClient;
import com.ibm.watson.zk.ZookeeperUtilsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Factory for KV utilities - allows for centralized configuration
 * and abstraction between key-value datastores.
 */
public abstract class KVUtilsFactory {

    private static final Logger logger = LoggerFactory.getLogger(KVUtilsFactory.class);

    public static final String KV_STORE_EV = "KV_STORE";
    public static final String ETCD_TYPE = "etcd", ZK_TYPE = "zookeeper";

    private static volatile KVUtilsFactory defaultFactory;

    /**
     * @return the default {@link KVUtilsFactory} based on the value of the {@code KV_STORE}
     *       environment variable
     * @throws Exception
     */
    public static KVUtilsFactory getDefaultFactory() throws Exception {
        if (defaultFactory != null) {
            return defaultFactory;
        }
        synchronized (KVUtilsFactory.class) {
            String storeVal = getParameter(KV_STORE_EV);
            if (storeVal != null) {
                logger.info("KV_STORE=" + storeVal);
                return defaultFactory = newFactory(storeVal);
            } else {
                String zkConnString = getParameter(ZookeeperClient.ZK_CONN_STRING_ENV_VAR);
                if (zkConnString != null) {
                    logger.info("KV_STORE env var not set, falling back to use "
                                + ZookeeperClient.ZK_CONN_STRING_ENV_VAR);
                    logger.info("creating new Zookeeper KV factory for conn string: " + zkConnString);
                    return defaultFactory = new ZookeeperUtilsFactory(zkConnString);
                }
            }
            throw new RuntimeException("No KV store configured, must set " + KV_STORE_EV);
        }
    }

    @VisibleForTesting
    public static void resetDefaultFactory() {
        defaultFactory = null;
    }

    /**
     * @param targetString
     * @return a new factory for the given targetString
     * @throws Exception
     */
    public static KVUtilsFactory newFactory(String targetString) throws Exception {
        //TODO maybe cache based on the target string
        int idx = targetString.indexOf(':');
        if (idx < 0) {
            throw new RuntimeException("Invalid kv store target string format");
        }
        String storeType = targetString.substring(0, idx);
        String configString = targetString.substring(idx + 1);
        if (ETCD_TYPE.equalsIgnoreCase(storeType)) {
//          return new EtcdUtilsFactory(configString);
            // for now avoid dependency requirement on etcd-java unless etcd is being used
            try {
                logger.info("creating new etcd KV factory with config file: " + configString);
                return Class.forName("com.ibm.watson.etcd.EtcdUtilsFactory")
                        .asSubclass(KVUtilsFactory.class)
                        .getConstructor(String.class).newInstance(configString);
            } catch (InvocationTargetException ite) {
                Throwables.throwIfInstanceOf(ite.getCause(), Exception.class);
                Throwables.throwIfUnchecked(ite.getCause());
                throw new RuntimeException(ite.getCause());
            }
        } else if (ZK_TYPE.equalsIgnoreCase(storeType)) {
            logger.info("creating new Zookeeper KV factory for conn string: " + configString);
            return new ZookeeperUtilsFactory(configString);
        }
        throw new RuntimeException("Unrecognized KV store type: " + storeType);
    }

    protected static String getParameter(String key) {
        String sysProp = System.getProperty(key);
        return sysProp != null ? sysProp : System.getenv(key);
    }

    public abstract String getKvStoreType();

    /**
     * @return future which complete as true if the connection is verified successfully
     * or complete exceptionally otherwise
     */
    public abstract ListenableFuture<Boolean> verifyKvStoreConnection();

    /**
     * @see SessionNode
     *
     * @param path
     * @param defaultValue
     */
    public abstract SessionNode newSessionNode(String path, byte[] defaultValue);

    /**
     * @return sessionId associated with this client, or 0 if the session has not
     * yet been established
     */
    public abstract long getSessionId();

    /**
     * @see KVTable
     *
     * @param path
     * @param defaultBuckets may or may not be used
     */
    public abstract KVTable newKVTable(String path, int defaultBuckets);

    /**
     * @see KVTable.Helper
     */
    public abstract KVTable.Helper KVTableHelper();

    /**
     * @see DynamicConfig
     *
     * @param path
     */
    public abstract DynamicConfig newDynamicConfig(String path);

    /**
     * @see LeaderElection
     *
     * @param path
     * @param id our own candidate id
     */
    public abstract LeaderElection newLeaderElection(String path, String id);

    /**
     * @see Distributor
     *
     * @param path
     * @param id our own participant id
     */
    public abstract Distributor newDistributor(String path, String id);

    /**
     * @see SharedCounter
     *
     * @param path
     * @param startFrom initial value of the counter to use if it does not already exist
     * @throws Exception
     */
    public abstract SharedCounter newSharedCounter(String path, long startFrom) throws Exception;


    // The below are "standalone" primitives implemented consistently across the supported datastores

    /**
     * Get a count stored in a specified key, or if not present atomically assign and return
     * a new unique count to that key based on a separate shared counter.
     * <p>
     * Will return -1 if the key is not present and {@code counterPath} is null.
     *
     * @param path        path to get/store the assigned value
     * @param counterPath path to shared counter used only if assigned value key doesn't exist,
     *                    or null to call in "read only" mode
     * @return the discovered or assigned count, or -1 if the count does not exist
     * and counterPath is passed as null
     * @throws Exception
     */
    public abstract long getOrAssignSharedCount(String path, String counterPath) throws Exception;

    /**
     * Atomically checks for existence of each of the provided string paths. Note that in the
     * case of etcd this is equivalent to the existence of one or more keys having the
     * corresponding utf8-encoded string <i>prefix</i> (a key corresponding to the path itself
     * might not exist).
     *
     * @return boolean array with each entry indicating the existence of the path
     * at the corresponding index of the {@code paths} array.
     * @throws Exception
     */
    public abstract boolean[] pathsExist(String[] paths) throws Exception;

    /**
     * @param path          path to compare and set
     * @param expectedValue {@code null} means "absent"
     * @param newValue      {@code null} means "delete"
     * @return object which is identically equal to {@code newValue} if successful,
     * otherwise the (unexpected) current value
     * @throws Exception
     */
    public abstract byte[] compareAndSetNodeContents(String path,
            byte[] expectedValue, byte[] newValue) throws Exception;
}
