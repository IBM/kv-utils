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

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.ibm.etcd.api.Compare;
import com.ibm.etcd.api.Compare.CompareResult;
import com.ibm.etcd.api.Compare.CompareTarget;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.watson.kvutils.SharedCounter;

import java.nio.ByteBuffer;

/**
 * Etcd {@link SharedCounter} implementation
 */
public class EtcdSharedCounter implements SharedCounter {

    protected static final int MAX_TRIES = 24;

    protected final KvClient client;
    protected final ByteString countKey;
    protected final long startFrom;

    protected ByteString curVal;

    //TODO optimization to support range batching

    /**
     * @param startFrom used only if count doesn't already exist
     */
    public EtcdSharedCounter(EtcdClient etcdClient, ByteString countKey, long startFrom) {
        client = etcdClient.getKvClient();
        this.countKey = countKey;
        this.startFrom = startFrom;
    }

    public EtcdSharedCounter(EtcdClient etcdClient, ByteString countKey) {
        this(etcdClient, countKey, 0L);
    }

    @Override
    public long getCurrentCount() {
        RangeResponse rr = client.get(countKey)
                .serializable().timeout(3000L).sync();
        if (rr.getKvsCount() == 0) {
            return startFrom;
        }
        return toLong(curVal = rr.getKvs(0).getValue());
    }

    @Override
    public long getNextCount() {
        // initialize builders which can be reused in loop. values
        // set here are invariant, other values are updated within the loop
        RequestOp.Builder opBld = RequestOp.newBuilder();
        TxnRequest.Builder treqBld = TxnRequest.newBuilder()
                // transaction always includes a get of the key as the failure op
                .addFailure(opBld.setRequestRange(RangeRequest.newBuilder()
                        .setKey(countKey)).build());
        Compare.Builder cmpBld = Compare.newBuilder().setKey(countKey)
                .setResult(CompareResult.EQUAL);
        PutRequest.Builder putBld = PutRequest.newBuilder().setKey(countKey);


        ByteString ourVal = curVal;
        for (int i = 0; i < MAX_TRIES; i++) {
            // determine incremented value which we want to set
            long nextLong = 1 + (ourVal == null? startFrom : toLong(ourVal));

            // encode as ByteString for etcd request
            ByteString nextVal = toByteString(nextLong);

            // if value doesn't exist, comparison rule needs to be version == 0
            if (ourVal == null) {
                cmpBld.setTarget(CompareTarget.VERSION).setVersion(0L);
            }
            // else rule is based on value we're incrementing from
            else {
                cmpBld.setTarget(CompareTarget.VALUE).setValue(ourVal);
            }

            // success case sets the incremented value, failure case gets the new "starting" value
            RequestOp succOp = opBld.setRequestPut(putBld.setValue(nextVal).build()).build();
            if (i == 0) {
                treqBld.addCompare(cmpBld.build());
                treqBld.addSuccess(succOp);
            } else {
                treqBld.setCompare(0, cmpBld.build());
                treqBld.setSuccess(0, succOp);
            }

            // attempt atomic increment transaction against etcd
            TxnResponse tresp = GrpcClient.waitFor(client.txn(treqBld.build()), 5000L);
            if (tresp.getSucceeded()) {
                // increment succeeded, return new value
                curVal = nextVal;
                return nextLong;
            }
            // else read off the new "current" value and continue loop
            RangeResponse rr = tresp.getResponses(0).getResponseRange();
            ourVal = rr.getKvsCount() > 0? rr.getKvs(0).getValue() : null;

//            ByteString prevNow = curVal; //TODO add this optimization
//            if(prevNow != null && ourVal != null
//                    && (prevNow > ourVal)) ourVal = prevNow;
        }

        throw new RuntimeException("Failed to get next unique id in " + MAX_TRIES + " attempts");
    }

    protected static ByteString toByteString(long val) {
        return UnsafeByteOperations.unsafeWrap(Longs.toByteArray(val));
    }

    protected static long toLong(ByteString bs) {
        ByteBuffer bb = bs.asReadOnlyByteBuffer();
        // also support reading 4-bytes in case data was copied from zookeeper
        return bs.size() == 4? bb.getInt() : bb.getLong();
    }
}
