package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class AxonServerManagedChannelTest extends AbstractAxonServerIntegrationTest {

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
            // by requesting a connection, we force a FAILURE - AxonServer is down
            ConnectivityState state = testSubject.getState(true);
            assertNotEquals(ConnectivityState.READY, state);
            assertNotEquals(ConnectivityState.SHUTDOWN, state);
            // we also want to wait for the next reconnect task to be scheduled, to avoid race conditions later on
            assertNotNull(nextConnectTask);
        });

        connectAttempts.clear();
        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> {
            if (nextConnectTask != null) {
                // keep looping the connect cycle
                nextConnectTask.run();
            }
            assertEquals(ConnectivityState.READY, testSubject.getState(true));
        });
        assertEquals(Collections.emptyList(), connectAttempts);
    }
}