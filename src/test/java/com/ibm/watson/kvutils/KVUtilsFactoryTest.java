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

import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public abstract class KVUtilsFactoryTest {

    public abstract KVUtilsFactory getFactory() throws Exception;

    public abstract KVUtilsFactory getDisconnectedFactory() throws Exception;

    protected static final String[] PATHS_TO_TEST = { "/a", "/b/c", "/d" };

    @Test
    public void testConnectionVerification() throws Exception {
        assertTrue(getFactory().verifyKvStoreConnection().get(2, TimeUnit.SECONDS));
    }

    @Test(expected = Exception.class)
    public void testConnectionVerificationFailure() throws Exception {
        getDisconnectedFactory().verifyKvStoreConnection().get(2, TimeUnit.SECONDS);
    }

    @Test
    public void testPathOperations() throws Exception {
        KVUtilsFactory fac = getFactory();

        assertEquals(0, fac.pathsExist(new String[0]).length);
        assertArrayEquals(new boolean[3], fac.pathsExist(PATHS_TO_TEST));

        byte[] val1 = "val1".getBytes(), val2 = "val2".getBytes();

        assertEquals(val1, fac.compareAndSetNodeContents("/a", null, val1));
        assertArrayEquals(new boolean[] { true, false, false },
                fac.pathsExist(PATHS_TO_TEST));

        assertEquals(val1, fac.compareAndSetNodeContents("/b/c", null, val1));
        assertArrayEquals(new boolean[] { true, true, false },
                fac.pathsExist(PATHS_TO_TEST));

        // update should not succeed, return existing value
        assertArrayEquals(val1, fac.compareAndSetNodeContents("/a", null, val2));
        assertArrayEquals(new boolean[] { true, true, false },
                fac.pathsExist(PATHS_TO_TEST));

        assertEquals(val2, fac.compareAndSetNodeContents("/a", val1, val2));
        assertArrayEquals(new boolean[] { true, true, false },
                fac.pathsExist(PATHS_TO_TEST));

        assertEquals(null, fac.compareAndSetNodeContents("/d", val1, val2));
        assertArrayEquals(new boolean[] { true, true, false },
                fac.pathsExist(PATHS_TO_TEST));

        // update should not succeed, return existing value
        assertArrayEquals(val2, fac.compareAndSetNodeContents("/a", val1, null));
        assertArrayEquals(new boolean[] { true, true, false },
                fac.pathsExist(PATHS_TO_TEST));

        assertEquals(null, fac.compareAndSetNodeContents("/a", val2, null));
        assertArrayEquals(new boolean[] { false, true, false },
                fac.pathsExist(PATHS_TO_TEST));
    }
}
