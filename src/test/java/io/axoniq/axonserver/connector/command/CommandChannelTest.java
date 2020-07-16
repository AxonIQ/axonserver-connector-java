/*
 * Copyright (c) 2010-2020. Axon Framework
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
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.silently;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;
    private static final Logger logger = LoggerFactory.getLogger(CommandChannelTest.class);

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
        commandChannel.registerCommandHandler(this::mockHandler, 100, "testCommand");

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        axonServerProxy.enable();

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        Thread.sleep(100);

        CompletableFuture<CommandResponse> result = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());

        CommandResponse commandResponse = result.get(1, TimeUnit.SECONDS);
        assertFalse(commandResponse.hasErrorMessage(),
                    () -> "Unexpected message: " + commandResponse.getErrorMessage().getMessage());
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

    private CompletableFuture<CommandResponse> mockHandler(Command command) {
        return CompletableFuture.completedFuture(CommandResponse.getDefaultInstance());
    }
}