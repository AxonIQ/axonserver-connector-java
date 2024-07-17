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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class FlowControlledStreamTest {

    private ClientCallStreamObserver<String> outboundStream;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        outboundStream = mock(ClientCallStreamObserver.class);
    }

    @Test
    void testParameterValuesAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> new TestFlowControlledStream("test", -1, 10));
        assertThrows(IllegalArgumentException.class, () -> new TestFlowControlledStream("test", 9, 10));
        assertThrows(IllegalArgumentException.class, () -> new TestFlowControlledStream(null, 1024, 10));
        assertDoesNotThrow(() -> new TestFlowControlledStream("test", 10, 10));
        assertDoesNotThrow(() -> new TestFlowControlledStream("test", 11, 10));
    }

    @Test
    void permitsRefilledAfterConsumingMessages() {
        TestFlowControlledStream testSubject = new TestFlowControlledStream("test", 1024, 8);
        testSubject.beforeStart(outboundStream);

        verify(outboundStream).disableAutoRequestWithInitial(1024);
        for (int i = 0; i < 7; i++) {
            testSubject.markConsumed();
        }
        verify(outboundStream, never()).request(anyInt());

        testSubject.markConsumed();
        verify(outboundStream).request(8);
    }

    @Test
    void permitsRefilledAfterConsumingMessagesTogetherWithAppFlowControlMessage() {
        TestFlowControlledStream testSubject = new TestFlowControlledStream("test", 1024, 8) {
            @Override
            protected String buildFlowControlMessage(FlowControl flowControl) {
                return "UpdateFlowControlMessage";
            }

            @Override
            protected String buildInitialFlowControlMessage(FlowControl flowControl) {
                return "InitialFlowControlMessage";
            }
        };
        testSubject.beforeStart(outboundStream);

        verify(outboundStream).disableAutoRequestWithInitial(1024);

        testSubject.enableFlowControl();
        verify(outboundStream).onNext("InitialFlowControlMessage");

        for (int i = 0; i < 7; i++) {
            testSubject.markConsumed();
        }

        verify(outboundStream, never()).request(anyInt());

        testSubject.markConsumed();
        verify(outboundStream).request(8);
        verify(outboundStream).onNext("UpdateFlowControlMessage");
    }

    private static class TestFlowControlledStream extends FlowControlledStream<String, String> {
        public TestFlowControlledStream(String clientId, int permits, int refillBatch) {
            super(clientId, permits, refillBatch);
        }

        @Override
        public void onNext(String value) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }

        @Override
        protected String buildFlowControlMessage(FlowControl flowControl) {
            return null;
        }
    }
}