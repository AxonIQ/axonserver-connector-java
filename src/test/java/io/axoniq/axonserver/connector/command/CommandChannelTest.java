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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommandChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory connectionFactory1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory connectionFactory2;
    private AxonServerConnection connection2;

    @BeforeEach
    void setUp() {
        connectionFactory1 = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                                        .routingServers(axonServerAddress);
        connection1 = connectionFactory1.connect("default");

        connectionFactory2 = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                                        .routingServers(axonServerAddress);
        connection2 = connectionFactory2.connect("default");
    }

    @AfterEach
    void tearDown() {
        connectionFactory1.shutdown();
        connectionFactory2.shutdown();
    }

    @Test
    void testUnsubscribedHandlersDoesNotReceiveCommands() throws Exception {
        CommandChannel commandChannel = connection1.commandChannel();
        Registration registration = commandChannel.registerCommandHandler(this::mockHandler, "testCommand");

        registration.cancel();

        CompletableFuture<CommandResponse> result = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());

        assertTrue(result.get(1, TimeUnit.SECONDS).hasErrorMessage());
        axonServerProxy.disable();
        assertWithin(100, TimeUnit.MILLISECONDS, () -> assertFalse(connection1.isReady()));
        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        CompletableFuture<CommandResponse> result2 = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());
        assertTrue(result.get(1, TimeUnit.SECONDS).hasErrorMessage());
    }

    @Test
    void testSubscribedHandlersReconnectAfterConnectionFailure() throws Exception {
        CommandChannel commandChannel = connection1.commandChannel();
        commandChannel.registerCommandHandler(this::mockHandler, "testCommand");

        axonServerProxy.disable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        axonServerProxy.enable();

        assertWithin(1, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        CompletableFuture<CommandResponse> result = connection2.commandChannel().sendCommand(Command.newBuilder().setName("testCommand").build());

        CommandResponse commandResponse = result.get(1, TimeUnit.SECONDS);
        assertFalse(commandResponse.hasErrorMessage(),
                    () -> "Unexpected message: " + commandResponse.getErrorMessage().getMessage());
    }

    private CompletableFuture<CommandResponse> mockHandler(Command command) {
        return CompletableFuture.completedFuture(CommandResponse.getDefaultInstance());
    }
}