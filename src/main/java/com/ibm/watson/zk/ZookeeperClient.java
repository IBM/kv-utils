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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ibm.watson.kvutils.OrderedShutdownHooks;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The original version of this class lived in litelinks
 */
public class ZookeeperClient {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

    public static final String ZK_CONN_STRING_ENV_VAR = "ZOOKEEPER";

    public static boolean SHORT_TIMEOUTS; // for stability unit testing

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static volatile boolean shutdown;

    private static final ScheduledExecutorService ses
            = Executors.newScheduledThreadPool(0, threadFactory("zkclient-retry-thread-%d"));

    private static final LoadingCache<String, CuratorFramework> clientCache
            = CacheBuilder.newBuilder().build(new CacheLoader<String, CuratorFramework>() {
        @Override
        public CuratorFramework load(String connString) {
            return newCurator(connString);
        }
    });

    static {
        // always disconnect zookeeper sessions
        OrderedShutdownHooks.addHook(1, new Runnable() {
            @Override
            public void run() {
                shutdown(true);
            }
        });
    }

    private static final Lock rLock = lock.readLock();

    /**
     * @param zookeeperConnString if null will fall back to env var
     * @return null if no zookeeper configured
     */
    public static CuratorFramework getCurator(String zookeeperConnString) {
        zookeeperConnString = resolveConnString(zookeeperConnString);
        if (zookeeperConnString == null) {
            return null;
        }

        rLock.lock();
        try {
            if (shutdown) {
                throw new IllegalStateException("ZookeeperClient registry has been shut down");
            }
            return clientCache.getUnchecked(zookeeperConnString);
        } finally {
            rLock.unlock();
        }
    }

    public static CuratorFramework getCurator(String zookeeperConnString, boolean ensureRoot) throws Exception {
        final CuratorFramework curator = getCurator(zookeeperConnString);
        // this is needed because curator doesn't work with zk namespace (chroot) otherwise
        if (curator != null && ensureRoot) {
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), "/", true, null, false);
        }
        return curator;
    }

    public static String resolveConnString(String zookeeperConnString) {
        if (zookeeperConnString == null) {
            zookeeperConnString = System.getenv(ZK_CONN_STRING_ENV_VAR);
        }
        return zookeeperConnString != null? zookeeperConnString.trim() : null;
    }

    private static CuratorFramework newCurator(String zookeeperConnString) {
        logger.info("Connecting to zookeeper with connect string: " + zookeeperConnString);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        CuratorFrameworkFactory.Builder cfBuilder = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConnString)
                .retryPolicy(retryPolicy)
                .canBeReadOnly(true);
        if (SHORT_TIMEOUTS) {
            cfBuilder.sessionTimeoutMs(5000).connectionTimeoutMs(2000);
        }
        CuratorFramework curator = cfBuilder.build();

        /* We add our own conn-loss background retry thread here in addition
         * to Curator's to catch name resolution problems which cause an
         * UnknownHostException. In this case Curator gives up retrying in the
         * background and so for example ephemeral nodes aren't re-established
         * once the problems are resolved (services don't re-register).
         */
        curator.getConnectionStateListenable().addListener(newConnRetryListener());

        curator.start();
        return curator;
    }

    private static ConnectionStateListener newConnRetryListener() {
        return new ConnectionStateListener() {
            volatile ScheduledFuture<?> job;

            @Override
            public void stateChanged(final CuratorFramework client, ConnectionState newState) {
                synchronized (client) {
                    if (job == null && !newState.isConnected()) {
                        logger.info("Scheduling background retry task - will test zookeeper conn every 20sec");
                        job = ses.scheduleWithFixedDelay(new Runnable() {
                            @Override
                            public void run() {
                                synchronized (client) {
                                    CuratorZookeeperClient czc = client.getZookeeperClient();
                                    if (czc.isConnected() ||
                                        !clientCache.asMap().containsKey(czc.getCurrentConnectionString())) {
                                        cancel();
                                    } else {
                                        try {
                                            long sessId = czc.getZooKeeper().getSessionId();
                                            logger.info("retry task obtained sessionId: 0x"
                                                        + Long.toHexString(sessId));
                                        } catch (Exception e) {
                                            logger.info("Exception retrying zookeeper conn: "
                                                        + e.getClass() + ": " + e.getMessage());
                                        }
                                    }
                                }
                            }
                        }, 20L, 20L, TimeUnit.SECONDS);
                    } else if (job != null && newState.isConnected()) {
                        cancel();
                    }
                }
            }

            private void cancel() {
                logger.info("Zookeeper background retry task stopped");
                job.cancel(false);
                job = null;
            }
        };
    }

    /**
     * Close all curator framework instances / connections to zookeeper. Class can no longer
     * be used post-shutdown
     *
     * @param disconnectOnly if true, does minimal work to disconnect from zookeeper (for shutdown hook)
     */
    public static void shutdown(boolean disconnectOnly) {
        shutdown(disconnectOnly, true);
    }

    public static synchronized void shutdown(boolean disconnectOnly, boolean isFinal) {
        final Lock wLock = lock.writeLock();
        wLock.lock();
        try {
            if (shutdown) {
                return;
            }
            long n = clientCache.size();
            if (n == 0) {
                logger.info("No zk conns to close");
                return;
            }
            logger.info("Shutting down " + n + " zookeeper connection(s)");
            for (Iterator<CuratorFramework> it = clientCache.asMap().values().iterator();
                 it.hasNext(); ) {
                CuratorFramework cf = it.next();
                it.remove();
                try {
                    if (!disconnectOnly) {
                        cf.close();
                    } else {
                        cf.getZookeeperClient().getZooKeeper().close();
                    }
                } catch (Exception e) {
                }
            }
            if (isFinal) {
                shutdown = true;
            }
        } finally {
            wLock.unlock();
        }
    }

    public static synchronized void disconnectCurator(String zookeeperConnString) {
        CuratorFramework cf = clientCache.getIfPresent(zookeeperConnString);
        if (cf != null) {
            logger.info("Shutting down zookeeper connection to " + zookeeperConnString);
            clientCache.invalidate(zookeeperConnString);
            cf.close();
        }
    }

    /**
     * Create a new thread factory which returns daemon threads
     * with the specified name format (using %d for counter substitution).
     * Created threads will have their (inherited) logging MDC cleared before running.
     */
    public static ThreadFactory threadFactory(final String nameFormat) {
        String.format(nameFormat, 0); // fail-fast for bad format
        final AtomicLong count = new AtomicLong();
        return r -> new Thread(r, String.format(nameFormat, count.incrementAndGet())) {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                // make sure log MDC doesn't get inherited
                MDC.clear();
                super.run();
            }
        };
    }
}
