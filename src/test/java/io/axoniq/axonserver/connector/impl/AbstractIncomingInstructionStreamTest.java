/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.InstructionHandler;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {{@link AbstractIncomingInstructionStream}}.
 *
 * @author Steven van Beelen
 */
class AbstractIncomingInstructionStreamTest {

    private static final String CLIENT_ID = "clientId";
    private static final int PERMITS = 100;
    private static final int PERMITS_BATCH = 100;

    @Test
    void testOnCompleted() {
        AtomicReference<Throwable> disconnectHandlerThrowable = new AtomicReference<>();
        AbstractIncomingInstructionStream<Object, Object> testSubject = new TestAbstractIncomingInstructionStreamImpl(
                CLIENT_ID, PERMITS, PERMITS_BATCH, disconnectHandlerThrowable::set, r -> {
        }, true
        );
        //noinspection unchecked
        ClientCallStreamObserver<Object> mockedClientCallStreamObserver = mock(ClientCallStreamObserver.class);

        // Given
        testSubject.beforeStart(mockedClientCallStreamObserver);
        // When
        testSubject.onCompleted();
        // Then
        assertTrue(disconnectHandlerThrowable.get() instanceof StreamUnexpectedlyCompletedException);
        verify(mockedClientCallStreamObserver).onCompleted();
    }

    private static class TestAbstractIncomingInstructionStreamImpl
            extends AbstractIncomingInstructionStream<Object, Object> {

        private final boolean unregisterOutboundStreamResponse;

        public TestAbstractIncomingInstructionStreamImpl(String clientId,
                                                         int permits,
                                                         int permitsBatch,
                                                         Consumer<Throwable> disconnectHandler,
                                                         Consumer<CallStreamObserver<Object>> beforeStartHandler,
                                                         boolean unregisterOutboundStreamResponse) {
            super(clientId, permits, permitsBatch, disconnectHandler, beforeStartHandler);
            this.unregisterOutboundStreamResponse = unregisterOutboundStreamResponse;
        }

        @Override
        protected Object buildAckMessage(InstructionAck ack) {
            return null;
        }

        @Override
        protected String getInstructionId(Object instruction) {
            return null;
        }

        @Override
        protected InstructionHandler<Object, Object> getHandler(Object msgIn) {
            return null;
        }

        @Override
        protected boolean unregisterOutboundStream(CallStreamObserver<Object> expected) {
            return this.unregisterOutboundStreamResponse;
        }

        @Override
        protected Object buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }
}