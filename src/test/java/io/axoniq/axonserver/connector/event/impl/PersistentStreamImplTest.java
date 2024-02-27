package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.InitializationProperties;
import io.axoniq.axonserver.grpc.streams.SequencingPolicy;
import io.axoniq.axonserver.grpc.streams.StreamRequest;
import io.axoniq.axonserver.grpc.streams.StreamSignal;
import io.grpc.stub.ClientCallStreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStreamImplTest {

    private final ClientIdentification clientIdentification = ClientIdentification.getDefaultInstance();
    private final String streamId = "test-stream";
    private final PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId);
    private final TestClientCallStreamObserver clientCallStreamObserver = new TestClientCallStreamObserver();

    @BeforeEach
    void setup() {
        testSubject.beforeStart(clientCallStreamObserver);
    }

    @Test
    void openConnection() {
        testSubject.openConnection();
        assertEquals(1, clientCallStreamObserver.requests.size());
        StreamRequest request = clientCallStreamObserver.requests.get(0);
        assertTrue(request.hasOpen());
        assertFalse(request.getOpen().hasInitializationProperties());
    }

    @Test
    void testOpenConnection() {
        testSubject.openConnection(InitializationProperties.newBuilder()
                                                           .setSegments(1)
                                           .setSequencingPolicy(SequencingPolicy.newBuilder()
                                                                        .setPolicyName("SP")
                                                                                .build())
                                                           .build());
        assertEquals(1, clientCallStreamObserver.requests.size());
        StreamRequest request = clientCallStreamObserver.requests.get(0);
        assertTrue(request.hasOpen());
        assertTrue(request.getOpen().hasInitializationProperties());
    }

    @Test
    void close() {
        testSubject.openConnection();
        testSubject.close();
        assertTrue(clientCallStreamObserver.closed.get());
    }


    @Test
    void onNext() throws InterruptedException {
        testSubject.openConnection();
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        testSubject.onSegmentOpened(s -> segments.put(s.segment(), s));
        testSubject.onNext(eventSignal(0, 100));

        PersistentStreamSegment segment = segments.get(0);
        assertNotNull(segment);
        assertNotNull(segment.peek());
        EventWithToken next = segment.next();
        assertEquals(100, next.getToken());
        
        segment.acknowledge(100);
        assertEquals(2, clientCallStreamObserver.requests.size());
        StreamRequest streamRequest = clientCallStreamObserver.requests.get(1);
        assertEquals(100, streamRequest.getAcknowledgeProgress().getPosition());
    }
    @Test
    void onNextCallsOnAvailable() {
        testSubject.openConnection();
        Map<Integer, AtomicInteger> availableCounter = new HashMap<>();
        testSubject.onAvailable(segment -> availableCounter.computeIfAbsent(segment, s -> new AtomicInteger()).incrementAndGet());
        testSubject.onNext(eventSignal(0, 100));
        assertEquals(1, availableCounter.get(0).get());
    }

    @Test
    void onNext_ifAvailable() throws InterruptedException {
        testSubject.openConnection();
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        testSubject.onSegmentOpened(s -> segments.put(s.segment(), s));
        testSubject.onNext(eventSignal(0, 100));

        PersistentStreamSegment segment = segments.get(0);
        assertNotNull(segment);
        EventWithToken next = segment.next();
        assertEquals(100, next.getToken());
        assertNull(segment.nextIfAvailable());
        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            testSubject.onNext(eventSignal(0, 101));
        }, 50, TimeUnit.MILLISECONDS);

        next = segment.nextIfAvailable(100, TimeUnit.MILLISECONDS);
        assertNotNull(next);
        assertEquals(101, next.getToken());
    }

    @Test
    void onNext_segmentClosed() throws InterruptedException {
        testSubject.openConnection();
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        testSubject.onSegmentOpened(s -> segments.put(s.segment(), s));
        testSubject.onNext(eventSignal(0, 100));

        PersistentStreamSegment segment = segments.get(0);
        segment.next();
        testSubject.onNext(segmentClosed(0));

        assertTrue(segment.isClosed());
        try {
            segment.next();
            fail("Reading from closed segment");
        } catch (StreamClosedException streamClosedException) {
            // expected
        }
    }

    @Test
    void onError() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean closed = new AtomicBoolean();
        testSubject.onClosed(t -> {
            error.set(t);
            closed.set(true);
        });
        testSubject.openConnection();
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        testSubject.onSegmentOpened(s -> segments.put(s.segment(), s));
        testSubject.onNext(eventSignal(0, 100));

        testSubject.onError(new RuntimeException("Error on Axon Server connection"));
        PersistentStreamSegment segment = segments.get(0);
        segment.next();
        try {
            segment.next();
            fail("Reading from closed stream");
        } catch (StreamClosedException streamClosedException) {
            assertTrue(segment.getError().isPresent());
        }

        assertNotNull(error.get());
        assertTrue(closed.get());
    }

    @Test
    void onCompleted() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean closed = new AtomicBoolean();
        testSubject.onClosed(t -> {
            error.set(t);
            closed.set(true);
        });
        testSubject.openConnection();
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        testSubject.onSegmentOpened(s -> segments.put(s.segment(), s));
        testSubject.onNext(eventSignal(0, 100));

        testSubject.onCompleted();
        PersistentStreamSegment segment = segments.get(0);
        segment.next();
        try {
            segment.next();
            fail("Reading from closed stream");
        } catch (StreamClosedException streamClosedException) {
            assertFalse(segment.getError().isPresent());
        }

        assertNull(error.get());
        assertTrue(closed.get());
    }

    @NotNull
    private static StreamSignal eventSignal(int segment, long token) {
        return StreamSignal.newBuilder()
                           .setSegment(segment)
                           .setEvent(EventWithToken.newBuilder()
                                                   .setToken(token)
                                                   .setEvent(Event.getDefaultInstance())
                                                   .build())
                           .build();
    }
    @NotNull
    private static StreamSignal segmentClosed(int segment) {
        return StreamSignal.newBuilder()
                           .setSegment(segment)
                           .setClosed(true)
                           .build();
    }

    private static class TestClientCallStreamObserver extends ClientCallStreamObserver<StreamRequest> {

        private final List<StreamRequest> requests = new LinkedList<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void cancel(@Nullable String s, @Nullable Throwable throwable) {

        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {

        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public void request(int i) {

        }

        @Override
        public void setMessageCompression(boolean b) {

        }

        @Override
        public void onNext(StreamRequest streamRequest) {
            requests.add(streamRequest);
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
            closed.set(true);
        }

        @Override
        public void onCompleted() {
            closed.set(true);
        }
    }
}