/*
 * Copyright (c) 2020. AxonIQ
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

import io.grpc.stub.ClientCallStreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;

class SynchronizedRequestStreamTest {

    private ExecutorService executorService;
    private ClientCallStreamObserver<String> mockObserver;
    private SynchronizedRequestStream<String> testSubject;

    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(3);
        mockObserver = mock(ClientCallStreamObserver.class);
        testSubject = new SynchronizedRequestStream<>(mockObserver);
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    @RepeatedTest(10)
    void testConcurrentWriteAndCompleteSuppressesExceptions() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            String message = "Message " + i;
            executorService.submit(() -> testSubject.onNext(message));
            if (i == 5) {
                executorService.submit(() -> testSubject.onCompleted());
            }
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        InOrder inOrder = Mockito.inOrder(mockObserver);
        inOrder.verify(mockObserver, atLeastOnce()).onNext(anyString());
        inOrder.verify(mockObserver).onCompleted();
        inOrder.verifyNoMoreInteractions();
    }

    @RepeatedTest(10)
    void testConcurrentWriteAndCompleteWithErrorSuppressesExceptions() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            String message = "Message " + i;
            executorService.submit(() -> testSubject.onNext(message));
            if (i == 5) {
                executorService.submit(() -> testSubject.onError(new RuntimeException("Faking an exception")));
            }
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        InOrder inOrder = Mockito.inOrder(mockObserver);
        inOrder.verify(mockObserver, atLeastOnce()).onNext(anyString());
        inOrder.verify(mockObserver).onError(isA(RuntimeException.class));
        inOrder.verifyNoMoreInteractions();
    }
}