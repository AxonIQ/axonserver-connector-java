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

package io.axoniq.axonserver.connector.event;

import io.axoniq.axonserver.connector.AbstractAxonServerIntegrationTest;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.connector.testutils.AssertUtils.assertWithin;
import static io.axoniq.axonserver.connector.testutils.MessageFactory.createDomainEvent;
import static io.axoniq.axonserver.connector.testutils.MessageFactory.createEvent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class EventHandlingIntegrationTest extends AbstractAxonServerIntegrationTest {

    private static final String QUERY_EXPRESSION_ALL = "";
    private static final boolean LIVE = true;
    private static final boolean NOT_LIVE = false;

    private static final String EVENT_PAYLOAD_ONE = "event1";
    private static final String EVENT_PAYLOAD_TWO = "event2";
    private static final String SNAPSHOT_PAYLOAD = "snapshot1";
    private static final String AGGREGATE_IDENTIFIER = "test";

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
                    .appendEvent(createEvent("Hello world"))
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
                              .appendEvent(createEvent("event0"))
                              .appendEvent(createEvent(EVENT_PAYLOAD_ONE).toBuilder()
                                                                         .setAggregateIdentifier("aggregate1")
                                                                         .setAggregateSequenceNumber(0)
                                                                         .setAggregateType("Aggregate")
                                                                         .build())
                              .appendEvent(createEvent(EVENT_PAYLOAD_TWO))
                              .commit()
                              .join();

        AggregateEventStream stream = eventChannel.openAggregateStream("aggregate1");
        assertTrue(stream.hasNext());
        Assertions.assertEquals(EVENT_PAYLOAD_ONE, stream.next().getPayload().getData().toStringUtf8());
        assertFalse(stream.hasNext());
    }

    @Test
    void testCancelAggregateStream() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        for (int i = 0; i < 200; i++) {
            publishingEventChannel.startAppendEventsTransaction()
                                  .appendEvent(createEvent("event0"))
                                  .appendEvent(createEvent(EVENT_PAYLOAD_ONE).toBuilder()
                                                                             .setAggregateIdentifier("aggregate1")
                                                                             .setAggregateSequenceNumber(i)
                                                                             .setAggregateType("Aggregate")
                                                                             .build())
                                  .appendEvent(createEvent(EVENT_PAYLOAD_TWO))
                                  .commit()
                                  .join();
        }

        AggregateEventStream stream = eventChannel.openAggregateStream("aggregate1");
        stream.cancel();
        try {
            while (stream.hasNext()) {
                Assertions.assertNotEquals(199, stream.next().getAggregateSequenceNumber());
            }
        } catch (StreamClosedException e) {
            // that's ok, because we're reading from a stream we have closed ourselves.
        }
    }

    @Test
    void testReadAggregateEventsAsStream() {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.startAppendEventsTransaction()
                              .appendEvent(createEvent("event0"))
                              .appendEvent(createEvent(EVENT_PAYLOAD_ONE).toBuilder()
                                                                         .setAggregateIdentifier("aggregate1")
                                                                         .setAggregateSequenceNumber(0)
                                                                         .setAggregateType("Aggregate")
                                                                         .build())
                              .appendEvent(createEvent(EVENT_PAYLOAD_TWO))
                              .commit()
                              .join();

        AggregateEventStream aggregateStream = eventChannel.openAggregateStream("aggregate1");
        Stream<Event> stream = aggregateStream.asStream();
        List<Event> asList = stream.collect(Collectors.toList());
        Assertions.assertEquals(1, asList.size());
        Assertions.assertEquals(EVENT_PAYLOAD_ONE, asList.get(0).getPayload().getData().toStringUtf8());
    }

    @Test
    void testEventChannelReconnectsAutomatically() throws Exception {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        assertTrue(publishingEventChannel.startAppendEventsTransaction()
                                         .appendEvent(createEvent(EVENT_PAYLOAD_ONE))
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
            assertThrows(StreamClosedException.class, stream::next);
            Assertions.assertNull(stream.nextIfAvailable(1, SECONDS));
            assertTrue(stream.isClosed());
        }

        axonServerProxy.enable();

        assertWithin(2, SECONDS, () -> assertTrue(connection1.isReady()));

        try (ResultStream<EventWithToken> stream = eventChannel.openStream(-1, 64)) {
            Assertions.assertNotNull(stream.nextIfAvailable(5, SECONDS));
            assertFalse(stream.isClosed());
        }
    }

    @Test
    void testScheduleAndCancel() throws Exception {
        assumeTrue(
                axonServerVersion.matches("20[0-9]{2}.*") || axonServerVersion.matches("4\\.[4-9].*"),
                "Version " + axonServerVersion + " does not support scheduled events"
        );

        EventChannel eventChannel = connection1.eventChannel();

        CompletableFuture<String> result =
                eventChannel.scheduleEvent(Duration.ofDays(1), createEvent("payload"));
        String token = result.get(1, SECONDS);
        assertNotNull(token);

        InstructionAck cancelResult = eventChannel.cancelSchedule(token).get(1, SECONDS);
        assertTrue(cancelResult.getSuccess());
    }

    @Test
    void testCancelUnknownToken() throws Exception {
        assumeTrue(
                axonServerVersion.matches("20[0-9]{2}.*") || axonServerVersion.matches("4\\.[4-9].*"),
                "Version " + axonServerVersion + " does not support scheduled events"
        );

        EventChannel eventChannel = connection1.eventChannel();

        CompletableFuture<String> result =
                eventChannel.scheduleEvent(Duration.ofDays(1), createEvent("payload"));
        String token = result.get(1, SECONDS);
        assertNotNull(token);

        InstructionAck cancelResult = eventChannel.cancelSchedule(token).get(1, SECONDS);
        assertTrue(cancelResult.getSuccess());
    }

    @Test
    void testSubscribeUsingInitialToken() throws InterruptedException, ExecutionException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createEvent(EVENT_PAYLOAD_ONE),
                                            createEvent(EVENT_PAYLOAD_TWO));

        Long firstToken = eventChannel.getFirstToken().get();
        EventStream actual = eventChannel.openStream(firstToken, 10);
        assertEquals(EVENT_PAYLOAD_ONE, actual.next().getEvent().getPayload().getData().toStringUtf8());
        assertEquals(EVENT_PAYLOAD_TWO, actual.next().getEvent().getPayload().getData().toStringUtf8());
        assertNull(actual.nextIfAvailable());
        actual.close();
    }

    @Test
    void testResubscribeUsingReceivedTokenContinuesOnStream() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createEvent(EVENT_PAYLOAD_ONE),
                                            createEvent(EVENT_PAYLOAD_TWO));

        EventStream firstStream = eventChannel.openStream(-1, 10);
        EventWithToken firstEvent = firstStream.next();
        assertEquals(EVENT_PAYLOAD_ONE, firstEvent.getEvent().getPayload().getData().toStringUtf8());
        firstStream.close();

        EventStream secondStream = eventChannel.openStream(firstEvent.getToken(), 10);
        EventWithToken secondEvent = secondStream.nextIfAvailable(1, SECONDS);
        assertEquals(EVENT_PAYLOAD_TWO, secondEvent.getEvent().getPayload().getData().toStringUtf8());
        secondStream.close();
    }

    @Test
    void testQueryEvents_WithLive() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createDomainEvent(EVENT_PAYLOAD_ONE, AGGREGATE_IDENTIFIER, 0),
                                            createDomainEvent(EVENT_PAYLOAD_TWO, AGGREGATE_IDENTIFIER, 1))
                              .join();
        publishingEventChannel.appendSnapshot(createDomainEvent("snapshot1", AGGREGATE_IDENTIFIER, 0))
                              .join();

        ResultStream<EventQueryResultEntry> queryResults = eventChannel.queryEvents(QUERY_EXPRESSION_ALL, LIVE);
        List<EventQueryResultEntry> actualValues = new ArrayList<>();
        EventQueryResultEntry row;
        while ((row = queryResults.nextIfAvailable(500, MILLISECONDS)) != null) {
            actualValues.add(row);
        }
        assertEquals(new HashSet<>(Arrays.asList(EVENT_PAYLOAD_ONE, EVENT_PAYLOAD_TWO)),
                     actualValues.stream().map(i -> i.getValueAsString("payloadData")).collect(Collectors.toSet()));
        assertNull(queryResults.nextIfAvailable(100, MILLISECONDS));

        publishingEventChannel.appendEvents(createEvent("event3"));
        assertNotNull(queryResults.nextIfAvailable(500, MILLISECONDS));

        assertFalse(queryResults.isClosed());

        queryResults.close();
        assertWithin(1, SECONDS, () -> assertTrue(queryResults.isClosed()));
    }

    @Test
    void testQueryEvents_WithoutLive() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createDomainEvent(EVENT_PAYLOAD_ONE, AGGREGATE_IDENTIFIER, 0),
                                            createDomainEvent(EVENT_PAYLOAD_TWO, AGGREGATE_IDENTIFIER, 1))
                              .join();
        publishingEventChannel.appendSnapshot(createDomainEvent(SNAPSHOT_PAYLOAD, AGGREGATE_IDENTIFIER, 0))
                              .join();

        ResultStream<EventQueryResultEntry> queryResults = eventChannel.queryEvents(QUERY_EXPRESSION_ALL, NOT_LIVE);
        List<EventQueryResultEntry> actualValues = new ArrayList<>();
        EventQueryResultEntry row;
        while ((row = queryResults.nextIfAvailable(500, MILLISECONDS)) != null) {
            actualValues.add(row);
        }
        assertEquals(new HashSet<>(Arrays.asList(EVENT_PAYLOAD_ONE, EVENT_PAYLOAD_TWO)),
                     actualValues.stream().map(i -> i.getValueAsString("payloadData")).collect(Collectors.toSet()));
        assertNull(queryResults.nextIfAvailable(100, MILLISECONDS));

        // we're not reading live events. Server should indicate end of stream.
        assertWithin(1, SECONDS, () -> assertTrue(queryResults.isClosed()));
    }

    @Test
    void testQuerySnapshotEvents_WithLive() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createDomainEvent(EVENT_PAYLOAD_ONE, AGGREGATE_IDENTIFIER, 0),
                                            createDomainEvent(EVENT_PAYLOAD_TWO, AGGREGATE_IDENTIFIER, 1))
                              .join();
        publishingEventChannel.appendSnapshot(createDomainEvent(SNAPSHOT_PAYLOAD, AGGREGATE_IDENTIFIER, 0))
                              .join();

        ResultStream<EventQueryResultEntry> queryResults = eventChannel.querySnapshotEvents(QUERY_EXPRESSION_ALL, LIVE);
        List<EventQueryResultEntry> actualValues = new ArrayList<>();
        EventQueryResultEntry row;
        while ((row = queryResults.nextIfAvailable(500, MILLISECONDS)) != null) {
            actualValues.add(row);
        }
        assertEquals(Collections.singleton(SNAPSHOT_PAYLOAD),
                     actualValues.stream().map(i -> i.getValueAsString("payloadData")).collect(Collectors.toSet()));
        assertNull(queryResults.nextIfAvailable(100, MILLISECONDS));

        publishingEventChannel.appendSnapshot(createDomainEvent("snapshot2", AGGREGATE_IDENTIFIER, 1));
        assertNotNull(queryResults.nextIfAvailable(500, MILLISECONDS));

        assertFalse(queryResults.isClosed());

        queryResults.close();
        assertWithin(1, SECONDS, () -> assertTrue(queryResults.isClosed()));
    }

    @Test
    void testQuerySnapshotEvents_WithoutLive() throws InterruptedException {
        EventChannel eventChannel = connection1.eventChannel();
        EventChannel publishingEventChannel = connection2.eventChannel();

        publishingEventChannel.appendEvents(createDomainEvent(EVENT_PAYLOAD_ONE, AGGREGATE_IDENTIFIER, 0),
                                            createDomainEvent(EVENT_PAYLOAD_TWO, AGGREGATE_IDENTIFIER, 1))
                              .join();
        publishingEventChannel.appendSnapshot(createDomainEvent(SNAPSHOT_PAYLOAD, AGGREGATE_IDENTIFIER, 0))
                              .join();

        ResultStream<EventQueryResultEntry> queryResults =
                eventChannel.querySnapshotEvents(QUERY_EXPRESSION_ALL, NOT_LIVE);
        List<EventQueryResultEntry> actualValues = new ArrayList<>();
        EventQueryResultEntry row;
        while ((row = queryResults.nextIfAvailable(500, MILLISECONDS)) != null) {
            actualValues.add(row);
        }
        assertEquals(Collections.singleton(SNAPSHOT_PAYLOAD),
                     actualValues.stream().map(i -> i.getValueAsString("payloadData")).collect(Collectors.toSet()));
        assertNull(queryResults.nextIfAvailable(100, MILLISECONDS));

        // we're not reading live events. Server should indicate end of stream.
        assertWithin(1, SECONDS, () -> assertTrue(queryResults.isClosed()));
    }

    @Test
    void testCreateTokenAt() throws ExecutionException, InterruptedException {
        long now = System.currentTimeMillis();
        connection1.eventChannel().appendEvents(
                createEvent(EVENT_PAYLOAD_ONE, now - 1000),
                createEvent(EVENT_PAYLOAD_TWO, now - 100),
                createEvent("event3", now - 100),
                createEvent("event4", now - 100),
                createEvent("event5", now - 50),
                createEvent("event6", now - 10)
        ).get();

        long token = connection2.eventChannel().getTokenAt(now - 100).get();
        EventStream buffer = connection2.eventChannel().openStream(token, 5);

        EventWithToken eventWithToken = buffer.nextIfAvailable(1, SECONDS);
        assertNotNull(eventWithToken);
        assertEquals(EVENT_PAYLOAD_TWO, eventWithToken.getEvent().getPayload().getData().toStringUtf8());
        assertEquals("event3", buffer.nextIfAvailable(1, SECONDS).getEvent().getPayload().getData().toStringUtf8());
        assertEquals("event4", buffer.nextIfAvailable(1, SECONDS).getEvent().getPayload().getData().toStringUtf8());
        assertEquals("event5", buffer.nextIfAvailable(1, SECONDS).getEvent().getPayload().getData().toStringUtf8());
        assertEquals("event6", buffer.nextIfAvailable(1, SECONDS).getEvent().getPayload().getData().toStringUtf8());
    }
}
