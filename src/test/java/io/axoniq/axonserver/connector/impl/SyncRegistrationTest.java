/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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