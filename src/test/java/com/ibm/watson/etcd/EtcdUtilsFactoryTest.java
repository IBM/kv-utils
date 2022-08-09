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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.*;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.watson.kvutils.KVUtilsFactoryTest;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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

    @Test
    public void testFactoryWithAuth() throws Exception {
        // etcd-java client doesn't support auth APIs natively - use gRPC API directly
        final ManagedChannel channel = NettyChannelBuilder.forTarget("localhost:2379")
                .usePlaintext().build();
        final AuthGrpc.AuthBlockingStub authClient = AuthGrpc.newBlockingStub(channel);
        // root user must be created
        authClient.userAdd(AuthUserAddRequest.newBuilder().setName("root").setPassword("root").build());
        authClient.userGrantRole(AuthUserGrantRoleRequest.newBuilder().setUser("root").setRole("root").build());
        // create user
        authClient.userAdd(AuthUserAddRequest.newBuilder().setName("user1").setPassword("password1").build());
        // create role
        authClient.roleAdd(AuthRoleAddRequest.newBuilder().setName("role1").build());
        // grant access permissions for prefix "user1chroot/" to role
        ByteString chrootPrefix = ByteString.copyFromUtf8("user1chroot/");
        authClient.roleGrantPermission(AuthRoleGrantPermissionRequest.newBuilder().setName("role1").setPerm(
                Permission.newBuilder().setPermType(Permission.Type.READWRITE)
                        .setKey(chrootPrefix).setRangeEnd(KeyUtils.plusOne(chrootPrefix))
                        .build()).build());
        // assign role to user
        authClient.userGrantRole(AuthUserGrantRoleRequest.newBuilder().setUser("user1").setRole("role1").build());
        // enable auth
        authClient.authEnable(AuthEnableRequest.getDefaultInstance());
        try (EtcdClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withCredentials("user1", "password1")
                .withPlainText().build()) {

            // user1 attempt to access outside chroot should fail
            KVUtilsFactory rootFactory = new EtcdUtilsFactory(client, null);
            ListenableFuture<Boolean> connCheck = rootFactory.verifyKvStoreConnection();
            try {
                connCheck.get(2, TimeUnit.SECONDS);
                fail("unauthorized access succeeded");
            } catch (Exception e) {
                assertEquals(Status.Code.PERMISSION_DENIED, Status.fromThrowable(e).getCode());
            }

            // user1 attempt to access inside chroot should succeed
            KVUtilsFactory userFactory = new EtcdUtilsFactory(client, chrootPrefix);
            assertTrue(userFactory.verifyKvStoreConnection().get(2, TimeUnit.SECONDS));
        } finally {
            // always disable auth - but must be root
            final AuthenticateResponse ar = authClient.authenticate(AuthenticateRequest.newBuilder()
                    .setName("root").setPassword("root").build());
            AuthGrpc.AuthBlockingStub rootClient = authClient.withCallCredentials(new CallCredentials() {
                @Override
                public void applyRequestMetadata(RequestInfo requestInfo, Executor executor,
                                                 MetadataApplier metadataApplier) {
                    Metadata header = new Metadata();
                    header.put(Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER), ar.getToken());
                    metadataApplier.apply(header);
                }
                @Override
                public void thisUsesUnstableApi() {}
            });
            // clean up users/roles
            rootClient.userDelete(AuthUserDeleteRequest.newBuilder().setName("user1").build());
            rootClient.roleDelete(AuthRoleDeleteRequest.newBuilder().setRole("role1").build());
            rootClient.authDisable(AuthDisableRequest.getDefaultInstance());
            // finally delete root user
            authClient.userDelete(AuthUserDeleteRequest.newBuilder().setName("root").build());
        }
    }
}
