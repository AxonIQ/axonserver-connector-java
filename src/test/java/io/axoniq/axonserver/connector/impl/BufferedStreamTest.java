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

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class BufferedStreamTest {

    private AbstractBufferedStream<String, String> testSubject;
    private ClientCallStreamObserver<String> mockUpstream;

    @BeforeEach
    void setUp() {
        mockUpstream = mock(ClientCallStreamObserver.class);
        this.testSubject = new AbstractBufferedStream<String, String>("test", 3, 2) {

            @Override
            protected String buildFlowControlMessage(FlowControl flowControl) {
                return null;
            }

            @Override
            protected String terminalMessage() {
                return "__terminal__";
            }
        };
        testSubject.beforeStart(mockUpstream);
    }

    @Test
    void testBufferAndCloseStream() {
        verify(mockUpstream).disableAutoRequestWithInitial(3);
        assertNull(testSubject.nextIfAvailable());

        testSubject.onNext("First");
        testSubject.onNext("Second");
        testSubject.onCompleted();

        assertEquals("First", testSubject.nextIfAvailable());
        assertFalse(testSubject.isClosed());

        verify(mockUpstream, never()).request(anyInt());

        assertEquals("Second", testSubject.nextIfAvailable());

        verify(mockUpstream).request(2);

        assertTrue(testSubject.isClosed());
    }

    @Test
    void testBufferAndCloseStreamWithError() {
        assertNull(testSubject.nextIfAvailable());

        testSubject.onNext("First");
        testSubject.onNext("Second");
        testSubject.onError(new RuntimeException("Faking an error"));

        assertEquals("First", testSubject.nextIfAvailable());
        assertFalse(testSubject.isClosed());
        assertTrue(testSubject.getError().isPresent());
        assertEquals("Second", testSubject.nextIfAvailable());
        assertTrue(testSubject.isClosed());
        assertTrue(testSubject.getError().isPresent());
    }

    @Test
    void testDataAvailableHandlerTriggeredImmediatelyOnErroredBuffer() {
        assertNull(testSubject.nextIfAvailable());

        testSubject.onError(new RuntimeException("Faking an error"));

        assertTrue(testSubject.isClosed());

        AtomicBoolean triggered = new AtomicBoolean(false);
        testSubject.onAvailable(() -> triggered.set(true));

        assertTrue(triggered.get());
    }

    @Test
    void testDataAvailableHandlerTriggeredImmediatelyOnClosedBuffer() {
        assertNull(testSubject.nextIfAvailable());

        testSubject.onCompleted();

        assertTrue(testSubject.isClosed());

        AtomicBoolean triggered = new AtomicBoolean(false);
        testSubject.onAvailable(() -> triggered.set(true));

        assertTrue(triggered.get());
    }
}