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

import com.google.common.io.BaseEncoding;
import com.ibm.watson.kvutils.Distributor.KeyDistributor;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public abstract class DistributorTest {

    public void testDistributor() throws Exception {

        String path = "/distributor-test";

        Distributor[] d = new Distributor[4];
        d[1] = newDistributor(path, "d1");
        d[2] = newDistributor(path, "d2");
        d[3] = newDistributor(path, "d3");

        int abcd = AbstractDistributor.hash("abcd");

        d[1].start().get(1, TimeUnit.SECONDS);
        Thread.sleep(300L);
        assertTrue(d[1].test(abcd));
        assertEquals("d1", d[1].participantFor(abcd));
        d[2].start().get(1, TimeUnit.SECONDS);
        Thread.sleep(300L);
        assertNotEquals(d[1].test(abcd), d[2].test(abcd));
        boolean isd1 = d[1].test(abcd);
        assertEquals(isd1? "d1" : "d2", d[1].participantFor(abcd));
        assertEquals(isd1? "d1" : "d2", d[2].participantFor(abcd));
        d[3].start().get(1, TimeUnit.SECONDS);
        d[1].close();
        Thread.sleep(300L);
        assertNotEquals(d[3].test(abcd), d[2].test(abcd));
        assertFalse(d[1].test(abcd));

        d[1] = newDistributor(path, "d1").start().get(1, TimeUnit.SECONDS);
        d[0] = newDistributor(path, "d0").start().get(1, TimeUnit.SECONDS);
        Thread.sleep(300);

        int N = 5000;

        int[] counts = new int[d.length];
        List<KeyDistributor<String>> kds = Arrays.stream(d).map(Distributor::forStringKeys)
                .collect(Collectors.toList());
        for (int i = 0; i < N; i++) {
            String key = "myprefix-" + randomStr();
            int count = 0;
            String id = null;
            for (KeyDistributor<String> dr : kds) {
                if (dr.test(key)) {
                    count++;
                }
                String p = dr.participantFor(key);
                assertNotNull("null participant for key " + i + ": " + key, p);
                if (id == null) {
                    id = p;
                } else {
                    assertEquals(id, p);
                }
            }
            assertNotNull(id);
            assertEquals('d', id.charAt(0));
            counts[id.charAt(1) - '0']++;
            assertEquals(key + " had count " + count, 1, count);
        }

        int min = Integer.MAX_VALUE, max = 0;
        for (int c : counts) {
            min = Math.min(min, c);
            max = Math.max(max, c);
        }
        int range = max - min;

        System.out.println("counts are: " + Arrays.toString(counts) + ", range is " + range);

        assertTrue("range greater than or equal to " + N / 35, range < N / 35);

        for (Distributor toclose : d) {
            if (toclose != null) {
                toclose.close();
            }
        }
    }

    private final Random random = new Random();

    String randomStr() {
        final byte[] buffer = new byte[6];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }

    protected abstract Distributor newDistributor(String path, String id);

}
