package io.axoniq.axonserver.connector.impl;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.doIfNotNull;
import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InstructionChannelTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory client;

    @AfterEach
    void tearDown() {
        doIfNotNull(client, AxonServerConnectionFactory::shutdown);
    }

    @Test
    void connectionRecoveredByHeartbeat() throws Exception {
        client = AxonServerConnectionFactory.forClient(getClass().getSimpleName())
                                            .routingServers(axonServerAddress)
                                            .build();
        AxonServerConnection connection1 = client.connect("default");
        connection1.instructionChannel().enableHeartbeat(500, 500, TimeUnit.MILLISECONDS);

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

        // we make sure the heartbeat doesn't complain at first
        long endCheck = System.currentTimeMillis() + 800;
        while (endCheck > System.currentTimeMillis()) {
            assertTrue(connection1.isConnected());
            Thread.sleep(100);
        }

        Timeout connectionIssue = axonServerProxy.toxics().timeout("bad_connection", ToxicDirection.DOWNSTREAM, Long.MAX_VALUE);

        assertWithin(2, TimeUnit.SECONDS, () -> assertFalse(connection1.isConnected()));

        connectionIssue.remove();

        assertWithin(2, TimeUnit.SECONDS, () -> assertTrue(connection1.isReady()));

    }
}