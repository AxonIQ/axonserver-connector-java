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

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.FlowControl;
import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Timeout(5)
class FlowControlledBufferTest {

    private FlowControlledBuffer<String, String> testSubject;
    private ClientCallStreamObserver<String> outboundStream;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        testSubject = new FlowControlledBuffer<String, String>("test", 1024, 8) {

            @Override
            protected String buildFlowControlMessage(FlowControl flowControl) {
                return null;
            }

            @Override
            protected String terminalMessage() {
                return "TERMINAL";
            }
        };
        outboundStream = mock(ClientCallStreamObserver.class);
        testSubject.beforeStart(outboundStream);
        verify(outboundStream).disableAutoRequestWithInitial(1024);
    }

    @Test
    void testPermitsRefilledWhenMessagesConsumed() throws InterruptedException {
        for (int i = 0; i < 16; i++) {
            testSubject.onNext("Message" + i);
        }
        for (int i = 0; i < 7; i++) {
            assertEquals("Message" + i, testSubject.tryTake(1, TimeUnit.SECONDS));
        }
        verify(outboundStream, never()).request(anyInt());
        assertEquals("Message7", testSubject.tryTakeNow());
        verify(outboundStream).request(8);
        for (int i = 0; i < 8; i++) {
            assertNotNull(testSubject.tryTakeNow());
        }
        verify(outboundStream, times(2)).request(8);

        // the queue is empty. Attempting to consume messages should not increase the counter
        for (int i = 0; i < 8; i++) {
            assertNull(testSubject.tryTakeNow());
        }
        assertNull(testSubject.tryTake(1, TimeUnit.MILLISECONDS));
        verify(outboundStream, times(2)).request(8);

        testSubject.close();
    }

    @Test
    void bufferReturnsAllMessagesWhenCompleted() {
        for (int i = 0; i < 16; i++) {
            testSubject.onNext("Message" + i);
        }
        testSubject.onCompleted();

        for (int i = 0; i < 16; i++) {
            assertFalse(testSubject.isClosed());
            assertEquals("Message" + i, testSubject.tryTakeNow());
        }
        assertTrue(testSubject.isClosed());
        assertNull(testSubject.tryTakeNow());
    }

    @Test
    void bufferReturnsAllMessagesWhenClosedWithError() throws InterruptedException {
        for (int i = 0; i < 16; i++) {
            testSubject.onNext("Message" + i);
        }
        testSubject.onError(new RuntimeException("Mock Exception"));

        for (int i = 0; i < 16; i++) {
            assertFalse(testSubject.isClosed());
            assertEquals("Message" + i, testSubject.tryTake());
        }
        assertTrue(testSubject.isClosed());
        assertNotNull(testSubject.getErrorResult());
        assertNull(testSubject.tryTakeNow());

        StreamClosedException e = assertThrows(StreamClosedException.class, () -> testSubject.take());
        assertNotNull(e.getCause());
        assertEquals(RuntimeException.class, e.getCause().getClass());
    }

    @Test
    void testPeekAndTryTakeReturnNullWhenNoMessagesAvailable() throws InterruptedException {
        testSubject.onNext("Message");
        assertEquals("Message", testSubject.peek());
        assertEquals("Message", testSubject.take());

        assertNull(testSubject.tryTakeNow());
        assertNull(testSubject.tryTake(1, TimeUnit.MILLISECONDS));
    }

    @Test
    void testAllTakeMethodsReturnImmediatelyWhenStreamIsClosed() throws InterruptedException {
        testSubject.onCompleted();
        assertTrue(testSubject.isClosed());

        assertThrows(StreamClosedException.class, () -> testSubject.take());

        assertNull(testSubject.tryTakeNow());
        assertNull(testSubject.tryTake());
        assertTimeout(Duration.ofMillis(500), () ->
                assertNull(testSubject.tryTake(5, TimeUnit.SECONDS)));
        assertTrue(testSubject.isClosed());
    }
}