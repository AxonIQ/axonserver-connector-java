package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.grpc.FlowControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferedStreamTest {

    private AbstractBufferedStream<String, String> testSubject;

    @BeforeEach
    void setUp() {
        this.testSubject = new AbstractBufferedStream<String, String>("test", 100, 0) {

            @Override
            protected String buildFlowControlMessage(FlowControl flowControl) {
                return null;
            }

            @Override
            protected String terminalMessage() {
                return "__terminal__";
            }
        };
    }

    @Test
    void testBufferAndCloseStream() {
        assertNull(testSubject.nextIfAvailable());

        testSubject.onNext("First");
        testSubject.onNext("Second");
        testSubject.onCompleted();

        assertEquals("First", testSubject.nextIfAvailable());
        assertFalse(testSubject.isClosed());
        assertEquals("Second", testSubject.nextIfAvailable());
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