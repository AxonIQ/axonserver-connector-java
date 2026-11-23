/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.control.Capabilities;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.NodeInfo;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.grpc.ConnectivityState;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the channel follows a redirect to completion, and that the capabilities it ends up holding are
 * those of the node it is actually connected to - including when it stops following redirects and stays put.
 */
class AxonServerManagedChannelRedirectTest {

    private static final String REDIRECT_HOST_PREFIX = "redirect-target-";

    private Server server;
    private StubPlatformService platformService;
    private AxonServerManagedChannel testSubject;
    private final List<ServerAddress> connectAttempts = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        platformService = new StubPlatformService();
        int port = freePort();
        server = NettyServerBuilder.forPort(port).addService(platformService).build().start();

        testSubject = new AxonServerManagedChannel(
                singletonList(new ServerAddress("entry-node")),
                new ReconnectConfiguration(2000, 0, true, TimeUnit.MILLISECONDS),
                "default",
                ClientIdentification.newBuilder()
                                    .setClientId(UUID.randomUUID().toString())
                                    .setComponentName(getClass().getSimpleName())
                                    .build(),
                mock(ScheduledExecutorService.class),
                (address, context) -> {
                    connectAttempts.add(address);
                    // every address resolves to the one stub, which varies its answer per call instead
                    return NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();
                }
        );
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        testSubject.shutdownNow();
        server.shutdownNow();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void followsRedirectAndTakesCapabilitiesFromTheNodeItSettlesOn() {
        platformService.redirectsBeforeSettling(1);

        assertEquals(ConnectivityState.READY, testSubject.getState(true));

        assertEquals(2, connectAttempts.size());
        assertEquals("entry-node", connectAttempts.get(0).getHostName());
        assertEquals(REDIRECT_HOST_PREFIX + "0", connectAttempts.get(1).getHostName());
        assertTrue(testSubject.supports(capabilityOfNode(1)));
    }

    @Test
    void dropsTheCapabilitiesOfANodeItHasMovedAwayFrom() {
        platformService.redirectsBeforeSettling(1);

        assertEquals(ConnectivityState.READY, testSubject.getState(true));

        // the entry node reported its own, but we no longer talk to it
        assertFalse(testSubject.supports(capabilityOfNode(0)));
    }

    @Test
    void keepsTheCapabilitiesOfTheLastNodeWhenRedirectsNeverSettle() {
        platformService.redirectsBeforeSettling(Integer.MAX_VALUE);

        assertEquals(ConnectivityState.READY, testSubject.getState(true));

        // the entry node, plus one attempt per followed redirect, then it stays where it is
        assertEquals(4, connectAttempts.size());
        assertTrue(testSubject.supports(capabilityOfNode(3)));
        assertFalse(testSubject.supports(capabilityOfNode(2)));
    }

    private static String capabilityOfNode(int node) {
        return "test:node-" + node;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Answers with a redirect for the first {@code redirects} calls, and describes itself after that. Every answer
     * reports the capabilities of the node that produced it, redirect or not, each call standing in for a
     * different node.
     */
    private static class StubPlatformService extends PlatformServiceGrpc.PlatformServiceImplBase {

        private final AtomicInteger calls = new AtomicInteger();
        private volatile int redirects;

        void redirectsBeforeSettling(int redirects) {
            this.redirects = redirects;
        }

        @Override
        public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
            int call = calls.getAndIncrement();
            PlatformInfo.Builder response =
                    PlatformInfo.newBuilder()
                                .setCapabilities(Capabilities.newBuilder().addCapability(capabilityOfNode(call)));
            if (call < redirects) {
                /*
                 * Each redirect names a different host. Pointing at the address the caller already dialled would
                 * legitimately count as answering for itself, which is what the address check in the channel is
                 * for.
                 */
                response.setSameConnection(false)
                        .setPrimary(NodeInfo.newBuilder()
                                            .setNodeName("target")
                                            .setHostName(REDIRECT_HOST_PREFIX + call)
                                            .setGrpcPort(8124));
            } else {
                response.setSameConnection(true).setPrimary(NodeInfo.newBuilder().setNodeName("target"));
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }
    }
}
