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
package com.ibm.watson.kvutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderedShutdownHooks {

    private static final Logger logger = LoggerFactory.getLogger(OrderedShutdownHooks.class);

    private OrderedShutdownHooks() {} // static only

    private static class Hook implements Comparable<Hook> {
        final Runnable hook;
        final int priority;

        public Hook(Runnable hook, int priority) {
            this.hook = hook;
            this.priority = priority;
        }

        @Override
        public int compareTo(Hook o) {
            return Integer.compare(o.priority, priority); // note reverse order
        }
    }

    private static final List<Hook> hooks = new ArrayList<>(8);

    private static volatile boolean inShutdown;

    public static boolean isInShutdown() {
        return inShutdown;
    }

    public static void addHook(Runnable hook) {
        addHook(0, hook);
    }

    // higher integer priority values will be run before lower
    public static synchronized void addHook(int priority, Runnable hook) {
        if (hook == null) {
            throw new NullPointerException("null hook runnable");
        }
        hooks.add(new Hook(hook, priority));
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread("ordered-shutdown-hook-thread") {
            @Override
            public void run() {
                inShutdown = true;
                synchronized (OrderedShutdownHooks.class) {
                    // this will traverse in *descending* priority order
                    Collections.sort(hooks);
                    for (Hook hook : hooks) {
                        try {
                            hook.hook.run();
                        } catch (RuntimeException e) {
                            logger.warn("Exception executing shutdown hook: ", e);
                        }
                    }
                }
            }
        });
    }
}
