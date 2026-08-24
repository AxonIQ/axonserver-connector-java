/*
 * Copyright (c) 2020-2026. AxonIQ
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

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.streams.PersistentStreamEvent;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link BufferedPersistentStreamSegment}.
 *
 * @author Steven van Beelen
 */
class BufferedPersistentStreamSegmentTest {

    private final AtomicLong lastAcknowledged = new AtomicLong(-1);
    private final AtomicInteger errorReports = new AtomicInteger();

    private BufferedPersistentStreamSegment testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new BufferedPersistentStreamSegment("stream-id", 0, 100, 0,
                                                          lastAcknowledged::set,
                                                          error -> errorReports.incrementAndGet());
    }

    @Test
    void isClosedStaysFalseWhileServerClosedSegmentStillHasBufferedEvents() {
        testSubject.onNext(eventWithToken(0));
        testSubject.onNext(eventWithToken(1));

        // when — Axon Server signals the segment is done (e.g. reassigned), while 2 events are still buffered
        testSubject.onCompleted();

        // then — isClosed() must not lie while a real, already-received event is still available for reading
        assertFalse(testSubject.isClosed());
        assertNotNull(testSubject.nextIfAvailable());
        assertFalse(testSubject.isClosed());
        assertNotNull(testSubject.nextIfAvailable());

        // then — only once genuinely drained does isClosed() report true
        assertTrue(testSubject.isClosed());
        assertNull(testSubject.nextIfAvailable());
    }

    @Test
    void closeKeepsBufferedEventsAvailableUntilDrained() {
        testSubject.onNext(eventWithToken(0));

        // when — a local/client-initiated close is requested while an event is still buffered
        testSubject.close();

        // then
        assertFalse(testSubject.isClosed());
        assertNotNull(testSubject.nextIfAvailable());
        assertTrue(testSubject.isClosed());
    }

    @Test
    void closeNotifiesSegmentClosedListenersExactlyOnce() {
        AtomicInteger notifications = new AtomicInteger();
        testSubject.onSegmentClosed(notifications::incrementAndGet);

        testSubject.close();
        testSubject.close(); // idempotent — must not double-fire

        assertEquals(1, notifications.get());
    }

    @Test
    void onCompletedNotifiesSegmentClosedListenersExactlyOnce() {
        AtomicInteger notifications = new AtomicInteger();
        testSubject.onSegmentClosed(notifications::incrementAndGet);

        testSubject.onCompleted();
        testSubject.onCompleted(); // idempotent — must not double-fire

        assertEquals(1, notifications.get());
    }

    @Test
    void acknowledgeAlwaysForwardsToProgressCallbackEvenAfterClose() {
        testSubject.close();

        testSubject.acknowledge(42L);

        assertEquals(42L, lastAcknowledged.get());
    }

    private static PersistentStreamEvent eventWithToken(long token) {
        return PersistentStreamEvent.newBuilder()
                                    .setEvent(EventWithToken.newBuilder().setToken(token))
                                    .build();
    }
}
