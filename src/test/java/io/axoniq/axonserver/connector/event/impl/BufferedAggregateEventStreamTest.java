package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validationg the {@link BufferedAggregateEventStream}.
 *
 * @author Allard Buijze
 */
class BufferedAggregateEventStreamTest {

    private BufferedAggregateEventStream testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new BufferedAggregateEventStream(10);
    }

    @Test
    void testEventStreamPropagatesErrorOnHasNext() {
        testSubject.onError(new RuntimeException("Mock"));

        assertThrows(StreamClosedException.class, () -> testSubject.hasNext());
    }

    @Test
    void testEventStreamPropagatesErrorOnHasNextAfterReadingAvailableEvents() throws InterruptedException {
        testSubject.onNext(Event.getDefaultInstance());
        testSubject.onNext(Event.newBuilder().setAggregateSequenceNumber(1).build());
        testSubject.onError(new RuntimeException("Mock"));

        assertTrue(testSubject.hasNext());
        assertEquals(Event.getDefaultInstance(), testSubject.next());
        assertTrue(testSubject.hasNext());
        assertEquals(Event.newBuilder().setAggregateSequenceNumber(1).build(), testSubject.next());
        assertThrows(StreamClosedException.class, () -> testSubject.hasNext());
    }

    @Test
    void testEventStreamErrorOnInvalidAggregateSequenceNumber() throws InterruptedException {
        ClientCallStreamObserver clientCallStreamObserverMock = mock(ClientCallStreamObserver.class);
        testSubject.beforeStart(clientCallStreamObserverMock);
        testSubject.onNext(Event.getDefaultInstance());
        testSubject.onNext(Event.getDefaultInstance());

        assertTrue(testSubject.hasNext());
        assertEquals(Event.getDefaultInstance(), testSubject.next());
        verify(clientCallStreamObserverMock).onError(any(RuntimeException.class));
        assertThrows(StreamClosedException.class, () -> testSubject.hasNext());
    }
}