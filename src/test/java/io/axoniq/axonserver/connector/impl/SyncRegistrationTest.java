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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncRegistrationTest {

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private SyncRegistration testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new SyncRegistration(() -> cancelled.set(true));
    }

    @Timeout(1)
    @Test
    void testAwaitDoesNotWait() throws TimeoutException, InterruptedException {
        assertSame(testSubject, testSubject.awaitAck(60, TimeUnit.SECONDS));
    }

    @Test
    void testOnAckExecutesImmediately() {
        AtomicBoolean result = new AtomicBoolean();
        testSubject.onAck(() -> result.set(true));

        assertTrue(result.get());
    }

    @Test
    void cancelReturnsCompletedFuture() {
        testSubject.cancel();
        assertTrue(cancelled.get());

        assertSame(testSubject.cancel(), testSubject.cancel());
        assertTrue(testSubject.cancel().isDone());
    }

}