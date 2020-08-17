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

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncRegistrationTest {


    private CompletableFuture<Void> future;
    private CompletableFuture<Void> cancelFuture;
    private AsyncRegistration testSubject;

    @BeforeEach
    void setUp() {
        future = new CompletableFuture<>();
        cancelFuture = new CompletableFuture<>();

        testSubject = new AsyncRegistration(future, () -> cancelFuture);
    }

    @Test
    void testAwaitDoesNotWaitOnCompletedFuture() throws TimeoutException, InterruptedException {
        future.complete(null);

        assertSame(testSubject, testSubject.awaitAck(1, TimeUnit.SECONDS));
    }

    @Test
    void testAwaitTimeoutThrowsException() {
        assertThrows(TimeoutException.class, () -> testSubject.awaitAck(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void testAwaitRethrowsAxonServerException() {
        AxonServerException thrown = new AxonServerException(ErrorCategory.OTHER, "Testing", "");
        future.completeExceptionally(thrown);

        try {
            testSubject.awaitAck(1, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            assertEquals(AxonServerException.class, e.getClass());
            assertSame(thrown, e);
        }
    }

    @Test
    void testAwaitWrapsOtherExceptionInAxonServerException() {
        Exception thrown = new Exception("Testing");
        future.completeExceptionally(thrown);

        try {
            testSubject.awaitAck(1, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            assertEquals(AxonServerException.class, e.getClass());
            assertEquals(ErrorCategory.INSTRUCTION_ACK_ERROR, ((AxonServerException) e).getErrorCategory());
            assertEquals(ExecutionException.class, e.getCause().getClass());
            assertSame(thrown, e.getCause().getCause());
        }
    }

    @Test
    void testOnAckExecutesAtCompletion() {
        AtomicBoolean result = new AtomicBoolean();
        testSubject.onAck(() -> result.set(true));

        assertFalse(result.get());

        future.complete(null);

        assertTrue(result.get());
    }

    @Test
    void cancelReturnsFutureIndependentOfRequestAck() {
        assertSame(cancelFuture, testSubject.cancel());
        future.complete(null);
        assertSame(cancelFuture, testSubject.cancel());
    }

}