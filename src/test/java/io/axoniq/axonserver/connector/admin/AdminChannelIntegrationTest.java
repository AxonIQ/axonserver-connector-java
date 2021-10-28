package io.axoniq.axonserver.connector.admin;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration tests for {@link io.axoniq.axonserver.connector.admin.impl.AdminChannelImpl}
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
class AdminChannelIntegrationTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory client;
    private AxonServerConnection connection;

    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient("admin-client")
                                            .routingServers(axonServerAddress)
                                            .reconnectInterval(500, MILLISECONDS)
                                            .build();
        connection = client.connect("default");
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Test
    void testPauseEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.pauseEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    void testStartEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.startEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    void testSplitEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.splitEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }

    @Test
    void testMergeEventProcessor() throws Exception {
        AdminChannel adminChannel = connection.adminChannel();
        CompletableFuture<Void> accepted = adminChannel.splitEventProcessor("processor", "tokenStore");
        accepted.get(1, SECONDS);
        Assertions.assertTrue(accepted.isDone());
    }
}
