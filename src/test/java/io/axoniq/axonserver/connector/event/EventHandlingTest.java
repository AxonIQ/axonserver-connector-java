package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.connector.testutils.MessageFactory;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventHandlingTest extends AbstractAxonServerIntegrationTest {

    private AxonServerConnectionFactory client1;
    private AxonServerConnection connection1;
    private AxonServerConnectionFactory client2;
    private AxonServerConnection connection2;

    @BeforeEach
    void setUp() {
        client1 = AxonServerConnectionFactory.forClient("event-handler")
                                             .routingServers(axonServerAddress)
                                             .reconnectInterval(500, MILLISECONDS)
                                             .build();
        connection1 = client1.connect("default");

        client2 = AxonServerConnectionFactory.forClient("event-sender")
                                             .routingServers(axonServerAddress)
                                             .reconnectInterval(500, MILLISECONDS)
                                             .build();
        connection2 = client2.connect("default");
    }

    @AfterEach
    void tearDown() {
        client1.shutdown();
        client2.shutdown();
    }

    @Test
    void testCallbackNotifiedOfAvailableEvent() throws Exception {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        try (ResultStream<EventWithToken> stream = eventChannel.openStream(-1, 1000)) {
            Assertions.assertNull(stream.nextIfAvailable(100, TimeUnit.MILLISECONDS));

            CountDownLatch dataAvailable = new CountDownLatch(1);

            CompletableFuture<Confirmation> result = publishingEventChannel
                    .startAppendEventsTransaction()
                    .appendEvent(MessageFactory.createEvent("Hello world"))
                    .commit();
            Confirmation confirmation = result.get(1, SECONDS);

            assertTrue(confirmation.getSuccess());

            stream.onAvailable(dataAvailable::countDown);
            assertTrue(dataAvailable.await(1, SECONDS));
        }
    }

    @Test
    void testReadAggregateEvents() throws Exception {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.startAppendEventsTransaction()
                              .appendEvent(MessageFactory.createEvent("event0"))
                              .appendEvent(MessageFactory.createEvent("event1").toBuilder()
                                                         .setAggregateIdentifier("aggregate1")
                                                         .setAggregateSequenceNumber(0)
                                                         .setAggregateType("Aggregate")
                                                         .build())
                              .appendEvent(MessageFactory.createEvent("event2"))
                              .commit()
                              .join();

        AggregateEventStream stream = eventChannel.openAggregateStream("aggregate1");
        assertTrue(stream.hasNext());
        Assertions.assertEquals("event1", stream.next().getPayload().getData().toStringUtf8());
        Assertions.assertFalse(stream.hasNext());
    }

    @Test
    void testCancelAggregateStream() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        for (int i = 0; i < 200; i++) {
            publishingEventChannel.startAppendEventsTransaction()
                                  .appendEvent(MessageFactory.createEvent("event0"))
                                  .appendEvent(MessageFactory.createEvent("event1").toBuilder()
                                                             .setAggregateIdentifier("aggregate1")
                                                             .setAggregateSequenceNumber(i)
                                                             .setAggregateType("Aggregate")
                                                             .build())
                                  .appendEvent(MessageFactory.createEvent("event2"))
                                  .commit()
                                  .join();
        }

        AggregateEventStream stream = eventChannel.openAggregateStream("aggregate1");
        stream.cancel();
        while (stream.hasNext()) {
            Assertions.assertNotEquals(199, stream.next().getAggregateSequenceNumber());
        }
    }

    @Test
    void testReadAggregateEventsAsStream() {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.startAppendEventsTransaction()
                              .appendEvent(MessageFactory.createEvent("event0"))
                              .appendEvent(MessageFactory.createEvent("event1").toBuilder()
                                                         .setAggregateIdentifier("aggregate1")
                                                         .setAggregateSequenceNumber(0)
                                                         .setAggregateType("Aggregate")
                                                         .build())
                              .appendEvent(MessageFactory.createEvent("event2"))
                              .commit()
                              .join();

        AggregateEventStream aggregateStream = eventChannel.openAggregateStream("aggregate1");
        Stream<Event> stream = aggregateStream.asStream();
        List<Event> asList = stream.collect(Collectors.toList());
        Assertions.assertEquals(1, asList.size());
        Assertions.assertEquals("event1", asList.get(0).getPayload().getData().toStringUtf8());

    }

    @Test
    void testEventChannelReconnectsAutomatically() throws Exception {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        assertTrue(publishingEventChannel.startAppendEventsTransaction()
                                         .appendEvent(MessageFactory.createEvent("event1"))
                                         .commit()
                                         .get()
                                         .getSuccess());

        // force disconnection between client and server

        try (ResultStream<EventWithToken> stream = eventChannel.openStream(-1, 64)) {
            Assertions.assertNotNull(stream.nextIfAvailable(5, SECONDS));
            axonServerProxy.disable();
            assertWithin(5, SECONDS, () -> assertTrue(stream.isClosed()));
        }

        try (ResultStream<EventWithToken> stream = eventChannel.openStream(-1, 64)) {
            Assertions.assertNull(stream.nextIfAvailable(1, SECONDS));
            assertTrue(stream.isClosed());
            assertThrows(StreamClosedException.class, stream::next);
        }

        axonServerProxy.enable();

        assertWithin(2, SECONDS, () -> assertTrue(connection1.isReady()));

        try (ResultStream<EventWithToken> stream = eventChannel.openStream(-1, 64)) {
            Assertions.assertNotNull(stream.nextIfAvailable(5, SECONDS));
            Assertions.assertFalse(stream.isClosed());
        }
    }
}
