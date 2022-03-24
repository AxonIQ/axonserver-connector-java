package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import org.junit.jupiter.api.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Stefan Dragisic
 */
public class EventTransformationChannelIntegrationTest {  //extends AbstractAxonServerIntegrationTest


    private AxonServerConnectionFactory client;
    private AxonServerConnection connection;


    @BeforeEach
    void setUp() {
        client = AxonServerConnectionFactory.forClient("event-transformer")
                                            .routingServers(new ServerAddress("localhost", 8124))
                                            .reconnectInterval(500, MILLISECONDS)
                                            .build();
        connection = client.connect("default");
    }

    @AfterEach
    void tearDown() {
        client.shutdown();
    }

    @Test
    void startAndCancelTransformation() throws Exception {
        EventTransformationChannel eventTransformationChannel = connection.eventTransformationChannel();

        eventTransformationChannel.newTransformation("my transformation")
                .thenCompose(transformation -> transformation.deleteEvent(24))
                .thenCompose(transformation -> transformation.deleteEvent(25))
                .thenCompose(NewEventTransformation.ActiveEventTransformation::apply)
                .thenCompose(NewEventTransformation.ActiveEventTransformation::rollback);
    }
}
