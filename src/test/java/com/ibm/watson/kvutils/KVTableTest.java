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

import com.google.common.collect.Iterables;
import com.ibm.watson.kvutils.KVTable.Serializer;
import com.ibm.watson.kvutils.KVTable.TableView;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public abstract class KVTableTest {

    public class StringRecord extends KVRecord {
        public String str;

        public StringRecord(String str) {
            this.str = str;
        }

        @Override
        public int hashCode() {
            return 31 + (str == null? 0 : str.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof StringRecord
                   && Objects.equals(str, ((StringRecord) obj).str);
        }

        @Override
        public String toString() {
            return "SR[" + str + "]";
        }
    }

    public final Serializer<StringRecord> stringSer = new Serializer<StringRecord>() {
        @Override
        public byte[] serialize(StringRecord sr) {
            return ((sr.isActionableUpdate()? "a" : "n") + sr.str)
                    .getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public StringRecord deserialize(byte[] data) {
            String str = new String(data, StandardCharsets.UTF_8);
            StringRecord sr = new StringRecord(str.substring(1));
            if (str.charAt(0) == 'a') {
                sr.setActionableUpdate();
            }
            return sr;
        }
    };


    public void testTable(KVTable table) throws Exception {

        TableView<StringRecord> view = table.getView(stringSer, 1);

        view.addListener(true, (type, key, record) -> {
            System.out.println(
                    "KVT listener event type=" + type + " key=" + key + " thread=" + Thread.currentThread().getName());
        });

        table.start(30, TimeUnit.SECONDS);

        view.unconditionalDelete("key-a");
        view.unconditionalDelete("key-b");
        view.unconditionalDelete("key-c");
        view.unconditionalDelete("key-d");

        assertTrue(view.conditionalSet("key-a", new StringRecord("value1")));
        StringRecord srb = new StringRecord("value1");
        assertTrue(view.conditionalSetAndGet("key-b", srb) == srb);
        StringRecord src = new StringRecord("value1");
        src.setActionableUpdate();
        assertTrue(view.conditionalSet("key-c", src));
        assertTrue(view.conditionalSet("key-d", new StringRecord("value1")));

        Thread.sleep(2000L);

        assertTrue(Iterables.elementsEqual(view, view.strongIterable(true)));

        src.str = "value2";
        assertTrue(view.conditionalSetAndGet("key-c", src) == src);

        StringRecord src2 = new StringRecord("value3");

        StringRecord result = view.conditionalSetAndGet("key-c", src2);
        assertEquals(result, src); // this compares the strings
        assertFalse(result == src); // will have been re de-serialized

        view.unconditionalDelete("key-c");
//        view.unconditionalDelete("key-a");
        view.unconditionalDelete("key-d");
        view.unconditionalDelete("key-b");

        assertEquals("value1", view.get("key-a").str);

        Thread.sleep(2000L);

    }

    public void testTableTxns(KVTable table, KVTable table2) throws Exception {

        // test transactions

        TableView<StringRecord> view1 = table.getView(stringSer, 1);
        TableView<StringRecord> view2 = table2.getView(stringSer, 1);

        StringRecord v1 = new StringRecord("val1"), v2 = new StringRecord("val2");

        KVTable.Helper helper = table.getHelper();

        List<KVRecord> results = helper.txn()
                .set(view1, "key1", v1)
                .ensureAbsent(view1, "nokeyhere")
                .set(view2, "key2", v2)
                .executeAndGet();

        assertNull(results);

        assertEquals("val2", view2.get("key2").str);
        table2.start();

        v1.str = "newval1";

        results = helper.txn()
                .set(view1, "key1", v1)
                .ensureAbsent(view2, "nokeyhere")
                .delete(view2, "wrongkey", v2)
                .executeAndGet();

        assertNotNull(results);
        assertEquals(3, results.size());
        assertNull(results.get(1));
        assertNull(results.get(2));
        assertEquals("val1", ((StringRecord) results.get(0)).str);

        results = helper.txn()
                .set(view1, "key1", v1)
                .ensureAbsent(view2, "firstpart/nokeyhere")
                .delete(view2, "key2", v2)
                .executeAndGet();

        assertNull(results);
        assertNull(view2.get("key2"));
        assertNull(view2.getStrong("key2"));
        assertEquals("newval1", view1.get("key1").str);
    }
}
