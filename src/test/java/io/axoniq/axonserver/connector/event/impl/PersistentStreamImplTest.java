/*
 * Copyright (c) 2020-2024. AxonIQ
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

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.event.PersistentStreamCallbacks;
import io.axoniq.axonserver.connector.event.PersistentStreamSegment;
import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.InitializationProperties;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
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
    private final PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(null,
                                                                                      null,
                                                                                      null,
                                                                                      null);
    private final PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                              callbacks);
    private final TestClientCallStreamObserver clientCallStreamObserver = new TestClientCallStreamObserver();

    @BeforeEach
    void setup() {
        testSubject.beforeStart(clientCallStreamObserver);
    }

    @Test
    void openConnection() {
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
        assertEquals(1, clientCallStreamObserver.requests.size());
        StreamRequest request = clientCallStreamObserver.requests.get(0);
        assertTrue(request.hasOpen());
        assertFalse(request.getOpen().hasInitializationProperties());
    }

    @Test
    void testOpenConnection() {
        testSubject.beforeStart(clientCallStreamObserver);
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
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(s -> segments.put(s.segment(), s),
                                                                            null,
                                                                            null,
                                                                            null);
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();

        testSubject.onNext(eventSignal(0, 100));

        PersistentStreamSegment segment = segments.get(0);
        assertNotNull(segment);
        assertNotNull(segment.peek());
        PersistentStreamEvent next = segment.next();
        assertEquals(100, next.getEvent().getToken());

        segment.acknowledge(100);
        assertEquals(3, clientCallStreamObserver.requests.size());
        StreamRequest streamRequest = clientCallStreamObserver.requests.get(2);
        assertEquals(100, streamRequest.getAcknowledgeProgress().getPosition());
    }

    @Test
    void onNextCallsOnAvailable() {
        Map<Integer, AtomicInteger> availableCounter = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(null,
                                                                            null,
                                                                            segment -> availableCounter.computeIfAbsent(
                                                                                                               segment.segment(),
                                                                                                               s -> new AtomicInteger())
                                                                                                       .incrementAndGet(),
                                                                            null);
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
        testSubject.onNext(eventSignal(0, 100));
        assertEquals(1, availableCounter.get(0).get());
    }

    @Test
    void onNext_ifAvailable() throws InterruptedException {
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(s -> segments.put(s.segment(), s),
                                                                            null,
                                                                            null,
                                                                            null);
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
        testSubject.onNext(eventSignal(0, 100));

        PersistentStreamSegment segment = segments.get(0);
        assertNotNull(segment);
        PersistentStreamEvent next = segment.next();
        assertEquals(100, next.getEvent().getToken());
        assertNull(segment.nextIfAvailable());
        Executors.newSingleThreadScheduledExecutor()
                 .schedule(() -> testSubject.onNext(eventSignal(0, 101)), 50, TimeUnit.MILLISECONDS);

        next = segment.nextIfAvailable(100, TimeUnit.MILLISECONDS);
        assertNotNull(next);
        assertEquals(101, next.getEvent().getToken());
    }

    @Test
    void onNext_segmentClosed() throws InterruptedException {
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(s -> segments.put(s.segment(), s),
                                                                            null,
                                                                            null,
                                                                            null);
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
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
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(s -> segments.put(s.segment(), s),
                                                                            null,
                                                                            null,
                                                                            t -> {
                                                                                error.set(t);
                                                                                closed.set(true);
                                                                            });
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
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
        Map<Integer, PersistentStreamSegment> segments = new HashMap<>();
        PersistentStreamCallbacks callbacks = new PersistentStreamCallbacks(s -> segments.put(s.segment(), s),
                                                                            null,
                                                                            null,
                                                                            t -> {
                                                                                error.set(t);
                                                                                closed.set(true);
                                                                            });
        PersistentStreamImpl testSubject = new PersistentStreamImpl(clientIdentification, streamId, 100, 50,
                                                                    callbacks);
        testSubject.beforeStart(clientCallStreamObserver);
        testSubject.openConnection();
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
                           .setEvent(PersistentStreamEvent.newBuilder()
                                                          .setEvent(EventWithToken.newBuilder()
                                                                                  .setToken(token)
                                                                                  .setEvent(Event.getDefaultInstance())
                                                                                  .build()))
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