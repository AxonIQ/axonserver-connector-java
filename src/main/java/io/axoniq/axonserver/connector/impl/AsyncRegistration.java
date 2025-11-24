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
