/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.axonserver.connector.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.Registration;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Asynchronous implementation of the {@link Registration}.
 */
public class AsyncRegistration implements Registration {

    private final CompletableFuture<Void> requestAck;
    private final Supplier<CompletableFuture<Void>> cancelAction;

    /**
     * Construct a {@link AsyncRegistration}, using the given {@code requestAck} on {@link #awaitAck(long, TimeUnit)}
     * and {@link #onAck(Runnable)}, and the given {@code cancelAction} on {@link #cancel()}.
     *
     * @param requestAck   the {@link CompletableFuture} to wait for on #awaitAck(long, TimeUnit)
     * @param cancelAction the {@link Supplier} of the {@link CompletableFuture} to retrieve on {@link #cancel()}
     */
    public AsyncRegistration(CompletableFuture<Void> requestAck, Supplier<CompletableFuture<Void>> cancelAction) {
        this.requestAck = requestAck;
        this.cancelAction = cancelAction;
    }

    @Override
    public CompletableFuture<Void> cancel() {
        return cancelAction.get();
    }

    @Override
    public Registration awaitAck(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
        try {
            requestAck.get(timeout, unit);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AxonServerException) {
                throw (AxonServerException) e.getCause();
            } else {
                throw new AxonServerException(
                        ErrorCategory.INSTRUCTION_ACK_ERROR, "An instruction returned a failed acknowledgement", "", e
                );
            }
        }
        return this;
    }

    @Override
    public Registration onAck(Runnable runnable) {
        requestAck.exceptionally(e -> null).thenRun(runnable);
        return this;
    }

    @Override
    public Registration onAck(Consumer<Optional<Throwable>> action) {
        requestAck.thenApply(ignored -> Optional.<Throwable>empty())
                  .exceptionally(Optional::of)
                  .thenAccept(action);
        return this;
    }
}
