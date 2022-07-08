/*
 * Copyright (c) 2021. AxonIQ
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

import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validationg the {@link BufferedAggregateEventStream}.
 *
 * @author Allard Buijze
 */
class BufferedAggregateEventStreamTest {

    private BufferedAggregateEventStream testSubject;
    private ClientCallStreamObserver<GetAggregateEventsRequest> clientCallStreamObserver;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        testSubject = new BufferedAggregateEventStream(10, 1);
        clientCallStreamObserver = mock(ClientCallStreamObserver.class);
        testSubject.beforeStart(clientCallStreamObserver);
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
    void throwsExceptionOnTimeoutWhileRetrievingEvents() throws Exception {
        reduceTimeout();

        // Push messages
        for (int i = 0; i < 20; i++) {
            testSubject.onNext(Event.newBuilder().setAggregateSequenceNumber(i).build());
            testSubject.hasNext();
            testSubject.next();
        }

        // Now, wait while there is no message
        RuntimeException exception = assertThrows(RuntimeException.class,
                                                  () -> testSubject.hasNext());
        assertEquals(
                "Was unable to load aggregate due to timeout while waiting for events. Last sequence number received: 19",
                exception.getMessage());
    }

    /**
     * Modify timeout to lower value. Otherwise, test will hang for 10 seconds waiting for the timeout. It's private and
     * final, so we have to work around the modifiers as well.
     */
    private void reduceTimeout() throws NoSuchFieldException, IllegalAccessException {
        Field timeoutField = BufferedAggregateEventStream.class.getDeclaredField("TAKE_TIMEOUT_MILLIS");
        timeoutField.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(timeoutField, timeoutField.getModifiers() & ~Modifier.FINAL);

        timeoutField.setInt(testSubject, 100);
    }
}
