/*
 * Copyright (c) 2020-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.Registration;

import java.util.concurrent.CompletableFuture;

/**
 * Synchronous implementation of the {@link Registration}.
 */
public class SyncRegistration implements Registration {

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);
    private final Runnable cancelAction;

    /**
     * Construct a {@link SyncRegistration}, using the given {@code cancelAction} when {@link #cancel()} is invoked.
     *
     * @param cancelAction the operation to perform when {@link #cancel()} is invoked
     */
    public SyncRegistration(Runnable cancelAction) {
        this.cancelAction = cancelAction;
    }

    @Override
    public CompletableFuture<Void> cancel() {
        cancelAction.run();
        return COMPLETED;
    }
}
