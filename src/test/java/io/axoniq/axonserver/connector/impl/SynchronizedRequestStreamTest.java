/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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

import static org.mockito.ArgumentMatchers.*;
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