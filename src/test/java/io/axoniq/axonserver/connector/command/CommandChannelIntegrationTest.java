/*
 * Copyright (c) 2020-2021. AxonIQ
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

package io.axoniq.axonserver.connector.command;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.command.impl.CommandChannelImpl;
import io.axoniq.axonserver.connector.impl.ContextConnection;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;
    private static final Logger logger = LoggerFactory.getLogger(CommandChannelIntegrationTest.class);

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName(),
                                                                   "client1")
                                                        .routingServers(axonServerAddress)
                                                        .forceReconnectViaRoutingServers(false)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .build();
        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName(),
                                                                   "client2")
                                                        .routingServers(axonServerAddress)
                                                        .reconnectInterval(500, TimeUnit.MILLISECONDS)
                                                        .forceReconnectViaRoutingServers(false)
                                                        .build();

        connection2 = connectionFactory2.connect("default");
    }

    @AfterEach
    void tearDown() {
        silently(connectionFactory1, AxonServerConnectionFactory::shutdown);
        silently(connectionFactory2, AxonServerConnectionFactory::shutdown);
    }

    @Test
    void testSubscribeWhileDisconnected() throws IOException {
        CommandChannel commandChannel = connection1.commandChannel();
        assertWithin(2, TimeUnit.SECONDS, connection1::isReady);

        logger.info("Closing TCP connection to AxonServer");
        axonServerProxy.disable();

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isConnectionFailed()));

        Registration result = commandChannel
                .registerCommandHandler(r -> CompletableFuture.completedFuture(CommandResponse.getDefaultInstance()), 100, "test");
        assertNotNull(result);
    }

    @Test
    void testUnsubscribedHandlersDoesNotReceiveCommands() throws Exception {
        CommandChannel commandChannel = connection1.commandChannel();
        Registration registration = commandChannel.registerCommandHandler(this::mockHandler, 100, "testCommand");

        registration.cancel().get(1, TimeUnit.SECONDS);

        CompletableFuture<CommandResponse> result = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());

        assertTrue(result.get(1, TimeUnit.SECONDS).hasErrorMessage());

        logger.info("Closing TCP connection to AxonServer");
        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isConnectionFailed()));

        logger.info("Re-enabling TCP connection to AxonServer");
        axonServerProxy.enable();

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        CompletableFuture<CommandResponse> result2 = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());
        assertTrue(result2.get(1, TimeUnit.SECONDS).hasErrorMessage());
    }

    @Test
    void testSubscribedHandlersReconnectAfterConnectionFailure() throws Exception {
        CommandChannel commandChannel = connection1.commandChannel();
        commandChannel.registerCommandHandler(this::mockHandler, 100, "testCommand")
                      .awaitAck(1, TimeUnit.SECONDS);

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isConnectionFailed()));

        axonServerProxy.enable();

        assertWithin(3, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        // an ACK is not a guarantee that the registration has also been fully processed...
        Thread.sleep(100);

        CompletableFuture<CommandResponse> result = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());

        CommandResponse commandResponse = result.get(1, TimeUnit.SECONDS);
        assertEquals("", commandResponse.getErrorMessage().getMessage());
    }

    @RepeatedTest(10)
    void testCommandChannelConsideredReadyWhenNoHandlersSubscribed() throws IOException, TimeoutException, InterruptedException {
        CommandChannelImpl commandChannel = (CommandChannelImpl) connection1.commandChannel();
        // just to make sure that no attempt was made to connect, since there are no handlers
        assertTrue(commandChannel.isReady());

        // make sure no real connection can be set up
        axonServerProxy.disable();
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        assertTrue(commandChannel.isReady());
        assertFalse(connection1.isConnected());

        Registration registration = commandChannel.registerCommandHandler(c -> CompletableFuture.completedFuture(null),
                                                                          100, "TestCommand");
        AxonServerException exception = assertThrows(AxonServerException.class, () -> registration.awaitAck(1, TimeUnit.SECONDS));
        assertEquals(ErrorCategory.INSTRUCTION_ACK_ERROR, exception.getErrorCategory());

        // because of the attempt to set up a connection, it may need a few milliseconds to discover that's not possible
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(commandChannel.isReady()));

        axonServerProxy.enable();

        // verify connection is established
        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(commandChannel.isReady()));
    }

    @Test
    void testReconnectFinishesCommandsInTransit() throws InterruptedException {

        Queue<CompletableFuture<CommandResponse>> commandsInProgress = new ConcurrentLinkedQueue<>();
        CommandChannel commandChannel = connection1.commandChannel();
        commandChannel.registerCommandHandler(command -> {
            CompletableFuture<CommandResponse> result = new CompletableFuture<>();
            commandsInProgress.add(result);
            return result;
        }, 100, "testCommand");

        CommandChannel commandChannel2 = connection2.commandChannel();

        assertWithin(1, TimeUnit.SECONDS, () -> connection1.isReady());

        // an ACK is not a guarantee that the registration has also been fully processed...
        Thread.sleep(100);

        List<CompletableFuture<CommandResponse>> actual = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            actual.add(commandChannel2.sendCommand(Command.newBuilder().setName("testCommand").build()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, commandsInProgress.size()));

        ((ContextConnection) connection1).getManagedChannel().requestReconnect();
        ((CommandChannelImpl) connection1.commandChannel()).reconnect();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        while (!commandsInProgress.isEmpty()) {
            doIfNotNull(commandsInProgress.poll(), r -> r.complete(CommandResponse.getDefaultInstance()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> actual.forEach(r -> assertTrue(r.isDone())));

        long messagesInError = actual.stream().filter(r -> r.join().hasErrorMessage()).count();
        actual.forEach(r -> {
            assertFalse((r.isCompletedExceptionally()));
            assertFalse(r.join().hasErrorMessage(), messagesInError + " Commands finished with  in error. In one instance: " + r.join().getErrorMessage().getMessage());
        });

    }

    @Test
    void testDisconnectFinishesCommandsInTransit() throws InterruptedException {

        Queue<CompletableFuture<CommandResponse>> commandsInProgress = new ConcurrentLinkedQueue<>();
        CommandChannel commandChannel = connection1.commandChannel();
        commandChannel.registerCommandHandler(command -> {
            CompletableFuture<CommandResponse> result = new CompletableFuture<>();
            commandsInProgress.add(result);
            return result;
        }, 100, "testCommand");

        CommandChannel commandChannel2 = connection2.commandChannel();

        assertWithin(1, TimeUnit.SECONDS, () -> connection1.isReady());

        // an ACK is not a guarantee that the registration has also been fully processed...
        Thread.sleep(100);

        List<CompletableFuture<CommandResponse>> actual = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            actual.add(commandChannel2.sendCommand(Command.newBuilder().setName("testCommand").build()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(10, commandsInProgress.size()));

        // a call to disconnect waits for disconnection to be completed. That isn't compatible with our test here.
        CompletableFuture.runAsync(() -> connection1.disconnect());

        while (!commandsInProgress.isEmpty()) {
            doIfNotNull(commandsInProgress.poll(), r -> r.complete(CommandResponse.getDefaultInstance()));
        }

        assertWithin(1, TimeUnit.SECONDS, () -> actual.forEach(r -> assertTrue(r.isDone())));

        long messagesInError = actual.stream().filter(r -> r.join().hasErrorMessage()).count();
        actual.forEach(r -> {
            assertFalse((r.isCompletedExceptionally()));
            assertFalse(r.join().hasErrorMessage(), messagesInError + " Commands finished with  in error. In one instance: " + r.join().getErrorMessage().getMessage());
        });

    }

    @Test
    void testDispatchCommandOnDisconnectReturnsError() throws Exception {
        CommandChannel commandChannel = connection1.commandChannel();
        commandChannel.registerCommandHandler(this::mockHandler, 100, "testCommand");

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));
        CompletableFuture<CommandResponse> result = commandChannel.sendCommand(Command.newBuilder().setName("testCommand").build());

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(result.isCompletedExceptionally()));
    }

    @Test
    void unsubscribingHandlerReturnsUnknownHandlerForCommand() throws TimeoutException, InterruptedException, ExecutionException {
        CommandChannel commandChannel = connection1.commandChannel();
        Registration registration = commandChannel.registerCommandHandler(this::mockHandler, 100, "testCommand")
                                                  .awaitAck(1, TimeUnit.SECONDS);

        // an ACK is not a guarantee that the registration has also been fully processed...
        Thread.sleep(100);

        CompletableFuture<CommandResponse> actual1 = commandChannel.sendCommand(Command.newBuilder().setName("testCommand").build());

        assertEquals("", actual1.get(1, TimeUnit.SECONDS).getErrorMessage().getMessage());
        assertEquals("", actual1.get(1, TimeUnit.SECONDS).getErrorCode());

        registration.cancel().get(2, TimeUnit.SECONDS);

        CompletableFuture<CommandResponse> actual2 = commandChannel.sendCommand(Command.newBuilder().setName("testCommand").build());

        assertEquals(ErrorCategory.NO_HANDLER_FOR_COMMAND.errorCode(), actual2.get(1, TimeUnit.SECONDS).getErrorCode());
        assertEquals("No Handler for command: testCommand", actual2.get(1, TimeUnit.SECONDS).getErrorMessage().getMessage());

    }

    private CompletableFuture<CommandResponse> mockHandler(Command command) {
        return CompletableFuture.completedFuture(CommandResponse.getDefaultInstance());
    }
}
