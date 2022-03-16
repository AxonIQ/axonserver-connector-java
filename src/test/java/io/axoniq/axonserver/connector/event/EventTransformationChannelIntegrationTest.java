package io.axoniq.axonserver.connector.event;

import com.google.protobuf.Descriptors;
import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.connector.testutils.MessageFactory.createEvent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

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
        eventTransformationChannel.startTransformation()
                .thenAccept(eventTransformationChannel::cancelTransformation).join();
    }
}
