/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class AxonServerManagedChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private final List<ServerAddress> connectAttempts = new CopyOnWriteArrayList<>();
    private AxonServerManagedChannel testSubject;
    private Runnable nextConnectTask;
    private ScheduledExecutorService mockExecutor;
    private Supplier<ManagedChannel> localConnection;

    @BeforeEach
    void setUp() throws Exception {
        axonServerProxy.enable();
        mockExecutor = mock(ScheduledExecutorService.class);
        doAnswer(inv -> {
            nextConnectTask = () -> {
                nextConnectTask = null;
                inv.getArgumentAt(0, Runnable.class).run();
            };
            return null;
        }).when(mockExecutor)
          .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        doAnswer(inv -> nextConnectTask = () -> {
            nextConnectTask = null;
            inv.getArgumentAt(0, Runnable.class).run();
        }).when(mockExecutor)
          .execute(any(Runnable.class));
        localConnection = () -> NettyChannelBuilder.forAddress(axonServerAddress.getHostName(), axonServerAddress.getGrpcPort())
                                                   .usePlaintext()
                                                   .build();
        testSubject = new AxonServerManagedChannel(asList(new ServerAddress("server1"),
                                                          new ServerAddress("server2"),
                                                          new ServerAddress("server3")),
                                                   new ReconnectConfiguration(2000, 0, true, TimeUnit.MILLISECONDS), "default",
                                                   ClientIdentification.newBuilder()
                                                                       .setClientId(UUID.randomUUID().toString())
                                                                       .setComponentName(getClass().getSimpleName())
                                                                       .build(),
                                                   mockExecutor,
                                                   (address, context) -> {
                                                       connectAttempts.add(address);
                                                       return localConnection.get();
                                                   });
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        testSubject.shutdown();
        testSubject.awaitTermination(1, TimeUnit.SECONDS);
        testSubject.shutdownNow();
    }

    @Test
    void connectionIdleOnCreation() {
        assertEquals(ConnectivityState.IDLE, testSubject.getState(false));
    }

    @Test
    void connectionReadyOnRequestConnection() {
        assertEquals(ConnectivityState.READY, testSubject.getState(true));
    }

    @Test
    void connectionIdleWhenConnectionInterrupted() throws Exception {
        // make sure we run previously all scheduled (re)connect tasks
        while (nextConnectTask != null) {
            nextConnectTask.run();
        }

        // abruptly close connection
        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            // we should see the existing connection go IDLE
            assertEquals(ConnectivityState.IDLE, testSubject.getState(false));
        });

        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(ConnectivityState.READY, testSubject.getState(true)));
    }

    /*
      This test asserts that the `beforeStart` is called before a connection attempt is being made,
      so that the AxonServerManagedChannel can safely assume any upstream StreamObservers here have
      been registered with the different communication channels (e.g. ControlChannel) before any
      errors are reported back.
     */
    @Test
    void testCallOnDisconnectedChannelFailImmediately() throws IOException {
        axonServerProxy.disable();

        EventStoreGrpc.EventStoreStub stub = EventStoreGrpc.newStub(testSubject);

        ClientResponseObserver<Event, Confirmation> observer = mock(ClientResponseObserver.class);
        StreamObserver<Event> upstream = stub.appendEvent(observer);

        InOrder inOrder = Mockito.inOrder(observer);
        inOrder.verify(observer).beforeStart(any());
        inOrder.verify(observer).onError(any());
        inOrder.verifyNoMoreInteractions();

        // these should not throw an exception
        upstream.onNext(Event.getDefaultInstance());
        upstream.onError(new StatusRuntimeException(Status.ABORTED));
    }

    @RepeatedTest(20)
    void connectionRecoveredOnDisconnection() throws Exception {
        // make sure we run previously all scheduled (re)connect tasks
        while (nextConnectTask != null) {
            nextConnectTask.run();
        }

        // abruptly close connection
        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            // by requesting a connection, we force a FAILURE - AxonServer is down
            assertEquals(ConnectivityState.TRANSIENT_FAILURE, testSubject.getState(true));
            // we also want to wait for the next reconnect task to be scheduled, to avoid race conditions later on
            assertNotNull(nextConnectTask);
        });

        connectAttempts.clear();

        nextConnectTask.run();

        assertEquals(asList(new ServerAddress("server1"),
                            new ServerAddress("server2"),
                            new ServerAddress("server3")),
                     connectAttempts);

        axonServerProxy.enable();

        nextConnectTask.run();

        assertEquals(ConnectivityState.READY, testSubject.getState(false));
    }

    @RepeatedTest(20)
    void sameConnectionRecoveredOnDisconnectionWithoutForcedPlatformReconnect() throws Exception {
        testSubject = new AxonServerManagedChannel(asList(new ServerAddress("server1"),
                                                          new ServerAddress("server2"),
                                                          new ServerAddress("server3")),
                                                   new ReconnectConfiguration(2000, 0, false, TimeUnit.MILLISECONDS), "default",
                                                   ClientIdentification.newBuilder()
                                                                       .setClientId(UUID.randomUUID().toString())
                                                                       .setComponentName(getClass().getSimpleName())
                                                                       .build(),
                                                   mockExecutor,
                                                   (address, context) -> {
                                                       connectAttempts.add(address);
                                                       return localConnection.get();
                                                   });
        // make sure we run previously all scheduled (re)connect tasks
        while (nextConnectTask != null) {
            nextConnectTask.run();
        }

        // abruptly close connection
        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            ConnectivityState state = testSubject.getState(true);
            assertEquals(ConnectivityState.TRANSIENT_FAILURE, state);
            // we also want to wait for the next reconnect task to be scheduled, to avoid race conditions later on
            assertNotNull(nextConnectTask);
        });

        axonServerProxy.enable();
        connectAttempts.clear();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            // because we want the test to re-attempt in quick succession, we must avoid underlying connect backoff
            testSubject.resetConnectBackoff();
            if (nextConnectTask != null) {
                // keep looping the connect cycle
                nextConnectTask.run();
            }
            // because we want the test to re-attempt in quick succession, we must avoid underlying connect backoff
            testSubject.resetConnectBackoff();
            assertEquals(ConnectivityState.READY, testSubject.getState(true));
        });
        assertEquals(Collections.emptyList(), connectAttempts);
    }
}